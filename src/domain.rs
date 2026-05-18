use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};

pub const DEFAULT_ACTION_LABEL: &str = "action";
const ACTION_BOUNDARY_EPS: f64 = 0.000_001;
const MIN_ACTION_RATE_DURATION_SECS: f64 = 1.0;
const VALUE_ANOMALY_RARE_FREQ: f64 = 0.25;
const VALUE_ANOMALY_CURVE: f64 = 0.6;

#[derive(Debug, Clone)]
pub struct EventRecord {
    pub ts: f64,
    pub type_id: String,
    pub obj: Value,
    pub keys: Vec<String>,
    pub size_bytes: u32,
    pub action_period_id: Option<u64>,
    pub in_action_period: bool,
    pub live_rate_score: f64,
    pub live_uniq_score: f64,
    /// When the event went through a type alias (merged group), this records the
    /// original structural type_id so it can be restored on unmerge.
    pub original_type_id: Option<String>,
}

/// A logical grouping of structural types into a single "merged" identity.
/// All downstream rendering, filtering, and scoring treat the group as one type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeGroup {
    /// "g:" + 10 hex (deterministic over sorted members).
    pub group_id: String,
    /// User-chosen label for the merged type.
    pub label: String,
    /// Sorted original structural hashes.
    pub members: Vec<String>,
    /// Each member's canonical name at the moment of merging, so we can preserve
    /// user-facing names through unmerge. None means the member had no rename
    /// (its display name was the default `type-<hex>` label).
    pub members_prior_name: Vec<Option<String>>,
}

#[derive(Debug, Clone)]
pub struct PreparedEvent {
    pub obj: Value,
    pub type_id: String,
    pub keys: Vec<String>,
    pub(crate) scalar_paths: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActionPeriod {
    pub id: u64,
    pub label: String,
    pub start: f64,
    pub end: Option<f64>,
}

#[derive(Debug, Clone, Default)]
pub struct PathStats {
    pub total: u64,
    pub values: HashMap<String, u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PathOverride {
    ForcedOn,
    ForcedOff,
}

#[derive(Debug, Clone)]
pub struct TypeProfile {
    pub type_id: String,
    pub name: Option<String>,
    pub count: u64,
    pub example: Value,
    pub considered_paths: IndexMap<String, bool>,
    pub path_overrides: IndexMap<String, PathOverride>,
    pub path_stats: IndexMap<String, PathStats>,
    pub baseline_path_stats: IndexMap<String, PathStats>,
    pub known_unrelated: bool,
    pub latest_rate: f64,
    pub latest_uniq: f64,
}

#[derive(Debug, Clone, Default)]
struct PeriodRateState {
    elapsed_secs: f64,
    counts: HashMap<String, u64>,
    first_event_ts: Option<f64>,
    type_first_ts: HashMap<String, f64>,
    type_last_ts: HashMap<String, f64>,
}

#[derive(Debug, Clone, Copy)]
pub struct RateDebugInfo {
    pub actual_rate: f64,
    pub expected_rate: f64,
    pub anomaly_score: f64,
}

#[derive(Debug, Clone)]
struct FilterTerm {
    negated: bool,
    value: String,
}

#[derive(Debug, Clone, Default)]
struct FilterExpr {
    groups: Vec<Vec<FilterTerm>>,
}

impl FilterExpr {
    fn parse(raw: &str) -> Self {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Self::default();
        }
        let mut groups: Vec<Vec<FilterTerm>> = vec![Vec::new()];
        let mut i = 0usize;
        while i < trimmed.len() {
            while i < trimmed.len() {
                let ch = trimmed[i..].chars().next().unwrap_or('\0');
                if !ch.is_whitespace() {
                    break;
                }
                i += ch.len_utf8();
            }
            if i >= trimmed.len() {
                break;
            }

            let start = i;
            let mut in_quote: Option<char> = None;
            let mut escaped = false;
            while i < trimmed.len() {
                let ch = trimmed[i..].chars().next().unwrap_or('\0');
                if let Some(q) = in_quote {
                    if escaped {
                        escaped = false;
                    } else if ch == '\\' {
                        escaped = true;
                    } else if ch == q {
                        in_quote = None;
                    }
                    i += ch.len_utf8();
                    continue;
                }
                if ch == '"' || ch == '\'' {
                    in_quote = Some(ch);
                    i += ch.len_utf8();
                    continue;
                }
                if let Some(_) = read_expr_operator(trimmed, i) {
                    break;
                }
                i += ch.len_utf8();
            }

            let raw_term = trimmed[start..i].trim();
            if let Some(term) = parse_filter_term(raw_term) {
                if let Some(group) = groups.last_mut() {
                    group.push(term);
                }
            }

            if let Some((is_or, consumed)) = read_expr_operator(trimmed, i) {
                i += consumed;
                if is_or {
                    groups.push(Vec::new());
                }
            }
        }
        groups.retain(|g| !g.is_empty());
        Self { groups }
    }

    fn matches(&self, mut predicate: impl FnMut(&str) -> bool) -> bool {
        if self.groups.is_empty() {
            return true;
        }
        self.groups.iter().any(|group| {
            group.iter().all(|term| {
                let hit = predicate(term.value.as_str());
                if term.negated {
                    !hit
                } else {
                    hit
                }
            })
        })
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataFilters {
    pub key_filter: String,
    pub type_filter: String,
    pub fuzzy_filter: String,
    pub exact_filter: String,
    #[serde(default)]
    pub substring_filter: String,
}

impl DataFilters {
    pub fn active_count(&self) -> usize {
        [
            &self.key_filter,
            &self.type_filter,
            &self.fuzzy_filter,
            &self.exact_filter,
            &self.substring_filter,
        ]
        .iter()
        .filter(|f| !f.is_empty())
        .count()
    }

    pub fn has_active(&self) -> bool {
        self.active_count() > 0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterField {
    Key,
    Type,
    Fuzzy,
    Exact,
    Substring,
}

impl FilterField {
    pub fn title(self) -> &'static str {
        match self {
            Self::Key => "keys",
            Self::Type => "type",
            Self::Fuzzy => "fuzzy",
            Self::Exact => "exact key=value",
            Self::Substring => "substring",
        }
    }
}

#[derive(Debug, Clone)]
pub struct AnalyzerModel {
    pub types: IndexMap<String, TypeProfile>,
    pub events: Vec<EventRecord>,
    pub periods: Vec<ActionPeriod>,
    pub current_label: String,
    /// group_id -> MergeGroup; iteration order is preserved for deterministic
    /// serialization.
    pub merge_groups: IndexMap<String, MergeGroup>,
    /// original_type_id -> group_id. Hot-path lookup: every `ingest_prepared` /
    /// `ingest_baseline_prepared` consults this once. Empty when nothing is
    /// merged, so the hook is a no-op for users who never use the feature.
    pub type_aliases: HashMap<String, String>,

    baseline_elapsed_secs: f64,
    baseline_counts: HashMap<String, u64>,
    baseline_last_ts: Option<f64>,
    baseline_last_seen_by_type: HashMap<String, f64>,
    last_seen_by_type: HashMap<String, f64>,
    period_rate_states: HashMap<u64, PeriodRateState>,
    last_rate_ts: Option<f64>,
    last_event_ts: Option<f64>,
    next_period_id: u64,
}

impl AnalyzerModel {
    pub fn new() -> Self {
        Self {
            types: IndexMap::new(),
            events: Vec::new(),
            periods: Vec::new(),
            current_label: DEFAULT_ACTION_LABEL.to_string(),
            merge_groups: IndexMap::new(),
            type_aliases: HashMap::new(),
            baseline_elapsed_secs: 0.0,
            baseline_counts: HashMap::new(),
            baseline_last_ts: None,
            baseline_last_seen_by_type: HashMap::new(),
            last_seen_by_type: HashMap::new(),
            period_rate_states: HashMap::new(),
            last_rate_ts: None,
            last_event_ts: None,
            next_period_id: 1,
        }
    }

    pub fn total_objects(&self) -> usize {
        self.events.len()
    }

    pub fn active_period(&self) -> Option<&ActionPeriod> {
        self.periods.last().filter(|p| p.end.is_none())
    }

    /// Closes any open period, using the last event timestamp or `close_ts` as a fallback.
    pub fn close_open_period(&mut self, close_ts: f64) {
        if let Some(last) = self.periods.last_mut() {
            if last.end.is_none() {
                last.end = Some(self.last_event_ts.unwrap_or(close_ts));
            }
        }
    }

    pub fn toggle_period(&mut self) -> bool {
        let Some(boundary_ts) = self.last_event_ts else {
            return false;
        };
        self.account_rate_elapsed(boundary_ts);
        if let Some(last) = self.periods.last_mut() {
            if last.end.is_none() {
                last.end = Some(boundary_ts);
                return true;
            }
        }
        let period_id = self.next_period_id;
        self.periods.push(ActionPeriod {
            id: period_id,
            label: self.current_label.clone(),
            start: boundary_ts + ACTION_BOUNDARY_EPS,
            end: None,
        });
        self.period_rate_states.entry(period_id).or_default();
        self.next_period_id += 1;
        true
    }

    fn get_or_create_type_profile(&mut self, type_id: &str, obj: &Value) -> &mut TypeProfile {
        self.types
            .entry(type_id.to_string())
            .or_insert_with(|| TypeProfile {
                type_id: type_id.to_string(),
                name: None,
                count: 0,
                example: obj.clone(),
                considered_paths: IndexMap::new(),
                path_overrides: IndexMap::new(),
                path_stats: IndexMap::new(),
                baseline_path_stats: IndexMap::new(),
                known_unrelated: false,
                latest_rate: 0.0,
                latest_uniq: 0.0,
            })
    }

    pub fn ingest(&mut self, obj: Value, ts: f64) {
        let prepared = prepare_event(obj);
        self.ingest_prepared(prepared, ts);
    }

    pub fn ingest_prepared(&mut self, prepared: PreparedEvent, ts: f64) {
        self.account_rate_elapsed(ts);
        self.last_event_ts = Some(ts);
        let PreparedEvent {
            obj,
            type_id,
            keys,
            scalar_paths,
        } = prepared;
        // Type-alias redirect: when this structural type is part of a merge
        // group, every downstream map/counter receives the group id instead.
        let (effective_type_id, original_type_id) =
            if let Some(group_id) = self.type_aliases.get(&type_id) {
                let group_id = group_id.clone();
                (group_id, Some(type_id))
            } else {
                (type_id, None)
            };
        let period_id = self.period_id_for_ts(ts);
        let in_action_period = period_id.is_some();

        let uniq = {
            let entry = self.get_or_create_type_profile(&effective_type_id, &obj);
            entry.count += 1;
            if entry.count == 1 {
                entry.example = obj.clone();
            }
            update_uniqueness(entry, &scalar_paths, in_action_period)
        };
        let prev_seen_ts = self
            .last_seen_by_type
            .get(effective_type_id.as_str())
            .copied();
        let rate = self.update_rate_scores(&effective_type_id, period_id, prev_seen_ts, ts);
        if let Some(entry) = self.types.get_mut(&effective_type_id) {
            entry.latest_rate = rate;
            entry.latest_uniq = uniq;
        }

        let size_bytes = obj.to_string().len() as u32;
        self.events.push(EventRecord {
            ts,
            type_id: effective_type_id.clone(),
            obj,
            keys,
            size_bytes,
            action_period_id: period_id,
            in_action_period,
            live_rate_score: rate,
            live_uniq_score: uniq,
            original_type_id,
        });
        self.last_seen_by_type.insert(effective_type_id, ts);
    }

    pub fn ingest_baseline(&mut self, obj: Value, ts: f64) {
        let prepared = prepare_event(obj);
        self.ingest_baseline_prepared(&prepared, ts);
    }

    pub fn ingest_baseline_prepared(&mut self, prepared: &PreparedEvent, ts: f64) {
        if let Some(last_ts) = self.baseline_last_ts {
            self.baseline_elapsed_secs += (ts - last_ts).max(0.0);
        }
        self.baseline_last_ts = Some(ts);

        let obj = &prepared.obj;
        let raw_type_id = &prepared.type_id;
        let effective_type_id = self
            .type_aliases
            .get(raw_type_id)
            .cloned()
            .unwrap_or_else(|| raw_type_id.clone());
        *self
            .baseline_counts
            .entry(effective_type_id.clone())
            .or_insert(0) += 1;
        self.baseline_last_seen_by_type
            .insert(effective_type_id.clone(), ts);

        let uniq = {
            let entry = self.get_or_create_type_profile(&effective_type_id, obj);
            entry.count += 1;
            if entry.count == 1 {
                entry.example = obj.clone();
            }
            update_uniqueness(entry, &prepared.scalar_paths, false)
        };

        if let Some(entry) = self.types.get_mut(&effective_type_id) {
            entry.latest_uniq = uniq;
        }
        self.last_seen_by_type.insert(effective_type_id, ts);
    }

    pub fn refresh_live_anomaly_scores(&mut self) {
        let mut updates = Vec::new();
        let mut prev_seen_by_type: HashMap<String, f64> = self.baseline_last_seen_by_type.clone();
        for (idx, e) in self.events.iter().enumerate() {
            let prev_seen = prev_seen_by_type.get(&e.type_id).copied();
            prev_seen_by_type.insert(e.type_id.clone(), e.ts);
            let Some(period_id) = e.action_period_id else {
                continue;
            };
            let rate = self.recomputed_rate_for_event(&e.type_id, period_id, e.ts, prev_seen);
            let uniq = self.recomputed_uniq_for(&e.type_id, &e.obj);
            updates.push((idx, rate, uniq));
        }
        for (idx, rate, uniq) in updates {
            if let Some(e) = self.events.get_mut(idx) {
                e.live_rate_score = rate;
                e.live_uniq_score = uniq;
            }
        }
    }

    fn recomputed_rate_for_event(
        &self,
        type_id: &str,
        period_id: u64,
        event_ts: f64,
        prev_seen_ts: Option<f64>,
    ) -> f64 {
        self.rate_debug_info(type_id, period_id, prev_seen_ts, event_ts)
            .map(|info| info.anomaly_score)
            .unwrap_or(0.0)
    }

    fn update_rate_scores(
        &mut self,
        type_id: &str,
        active_period_id: Option<u64>,
        prev_seen_ts: Option<f64>,
        now_ts: f64,
    ) -> f64 {
        if let Some(pid) = active_period_id {
            let ts = self.last_event_ts.unwrap_or_default();
            let pr = self.period_rate_states.entry(pid).or_default();
            *pr.counts.entry(type_id.to_string()).or_insert(0) += 1;
            pr.type_first_ts.entry(type_id.to_string()).or_insert(ts);
            pr.type_last_ts.insert(type_id.to_string(), ts);
            if pr.first_event_ts.is_none() {
                pr.first_event_ts = Some(ts);
            }
            self.rate_debug_info(type_id, pid, prev_seen_ts, now_ts)
                .map(|info| info.anomaly_score)
                .unwrap_or(0.0)
        } else {
            *self.baseline_counts.entry(type_id.to_string()).or_insert(0) += 1;
            0.0
        }
    }

    fn account_rate_elapsed(&mut self, now_ts: f64) {
        let Some(last_ts) = self.last_rate_ts else {
            self.last_rate_ts = Some(now_ts);
            return;
        };
        if now_ts <= last_ts {
            return;
        }
        self.distribute_elapsed(last_ts, now_ts);
        self.last_rate_ts = Some(now_ts);
    }

    fn period_id_for_ts(&self, ts: f64) -> Option<u64> {
        self.periods
            .iter()
            .find(|p| ts >= p.start && p.end.map(|end| ts <= end).unwrap_or(true))
            .map(|p| p.id)
    }

    fn distribute_elapsed(&mut self, from_ts: f64, to_ts: f64) {
        let mut cursor = from_ts;
        while cursor < to_ts {
            let next_boundary = self
                .periods
                .iter()
                .flat_map(|p| [Some(p.start), p.end])
                .flatten()
                .filter(|boundary| *boundary > cursor)
                .min_by(|a, b| a.total_cmp(b))
                .unwrap_or(to_ts);
            let seg_end = next_boundary.min(to_ts);
            if seg_end <= cursor {
                break;
            }
            let mid = (cursor + seg_end) * 0.5;
            let seg_elapsed = seg_end - cursor;
            if let Some(pid) = self.period_id_for_ts(mid) {
                self.period_rate_states.entry(pid).or_default().elapsed_secs += seg_elapsed;
            } else {
                self.baseline_elapsed_secs += seg_elapsed;
            }
            cursor = seg_end;
        }
    }

    pub fn rate_debug_info_for_event_index(&self, event_idx: usize) -> Option<RateDebugInfo> {
        let event = self.events.get(event_idx)?;
        let period_id = event.action_period_id?;
        let prev_seen = self.prev_seen_ts_for_event(event_idx, &event.type_id);
        self.rate_debug_info(&event.type_id, period_id, prev_seen, event.ts)
    }

    fn prev_seen_ts_for_event(&self, event_idx: usize, type_id: &str) -> Option<f64> {
        if event_idx > 0 {
            for i in (0..event_idx).rev() {
                let e = self.events.get(i)?;
                if e.type_id == type_id {
                    return Some(e.ts);
                }
            }
        }
        self.baseline_last_seen_by_type.get(type_id).copied()
    }

    fn rate_debug_info(
        &self,
        type_id: &str,
        period_id: u64,
        prev_seen_ts: Option<f64>,
        event_ts: f64,
    ) -> Option<RateDebugInfo> {
        let _ = self.period_rate_states.get(&period_id)?;
        let expected_rate = self.baseline_rate_for_type(type_id);
        let observed_rate = self
            .period_action_rate(type_id, period_id)
            .or_else(|| self.interarrival_action_rate(prev_seen_ts, event_ts));
        let actual_rate = observed_rate.unwrap_or(0.0);

        let anomaly_score = if self.baseline_counts.get(type_id).copied().unwrap_or(0) == 0 {
            1.0
        } else if observed_rate.is_none() {
            0.0
        } else {
            normalized_rate_anomaly(actual_rate, expected_rate)
        };

        Some(RateDebugInfo {
            actual_rate,
            expected_rate,
            anomaly_score,
        })
    }

    fn baseline_rate_for_type(&self, type_id: &str) -> f64 {
        let baseline_count = self.baseline_counts.get(type_id).copied().unwrap_or(0) as f64;
        baseline_count / self.baseline_elapsed_secs.max(0.001)
    }

    fn period_action_rate(&self, type_id: &str, period_id: u64) -> Option<f64> {
        let pr = self.period_rate_states.get(&period_id)?;
        if pr.elapsed_secs < MIN_ACTION_RATE_DURATION_SECS {
            return None;
        }
        let action_count = pr.counts.get(type_id).copied().unwrap_or(0) as f64;
        if action_count < 2.0 {
            return None;
        }
        let type_elapsed = pr.type_last_ts.get(type_id).copied().unwrap_or(0.0)
            - pr.type_first_ts.get(type_id).copied().unwrap_or(0.0);
        (type_elapsed > 0.0).then_some((action_count - 1.0) / type_elapsed)
    }

    fn interarrival_action_rate(&self, prev_seen_ts: Option<f64>, event_ts: f64) -> Option<f64> {
        let prev_ts = prev_seen_ts?;
        let delta = event_ts - prev_ts;
        (delta > 0.0).then_some(1.0 / delta)
    }

    fn recomputed_uniq_for(&self, type_id: &str, obj: &Value) -> f64 {
        let Some(tp) = self.types.get(type_id) else {
            return 0.0;
        };
        let mut scalar_paths = Vec::new();
        collect_scalar_paths(obj, "", &mut scalar_paths);

        let mut max_score = 0.0;
        for (path, token) in scalar_paths {
            let Some(stats) = tp.path_stats.get(path.as_str()) else {
                continue;
            };
            let auto = auto_consider_path(path.as_str(), stats);
            let considered = match tp.path_overrides.get(path.as_str()).copied() {
                Some(PathOverride::ForcedOn) => true,
                Some(PathOverride::ForcedOff) => false,
                None => tp
                    .considered_paths
                    .get(path.as_str())
                    .copied()
                    .unwrap_or(auto),
            };
            if !considered || stats.total <= 6 {
                continue;
            }
            let baseline_stats = tp.baseline_path_stats.get(path.as_str());
            let s = if let Some(bstats) = baseline_stats {
                let prev = bstats.values.get(token.as_str()).copied().unwrap_or(0);
                value_frequency_anomaly(prev, bstats.total)
            } else {
                1.0
            };
            if s > max_score {
                max_score = s;
            }
        }
        max_score
    }

    pub fn toggle_type_path(&mut self, type_id: &str, path: &str) {
        if let Some(tp) = self.types.get_mut(type_id) {
            let current = tp.considered_paths.get(path).copied().unwrap_or(false);
            let next = match tp.path_overrides.get(path).copied() {
                None => {
                    if current {
                        Some(PathOverride::ForcedOff)
                    } else {
                        Some(PathOverride::ForcedOn)
                    }
                }
                Some(PathOverride::ForcedOff) => Some(PathOverride::ForcedOn),
                Some(PathOverride::ForcedOn) => None,
            };
            match next {
                Some(mode) => {
                    tp.path_overrides.insert(path.to_string(), mode);
                }
                None => {
                    tp.path_overrides.shift_remove(path);
                }
            }
            sync_type_considered_paths(tp);
        }
    }

    pub fn normalized_field_overrides(&self) -> Vec<(String, String, PathOverride)> {
        let mut out = Vec::new();
        for (type_id, tp) in &self.types {
            for (path, mode) in &tp.path_overrides {
                out.push((type_id.clone(), path.clone(), *mode));
            }
        }
        out
    }

    pub fn apply_normalized_field_overrides(
        &mut self,
        overrides: &[(String, String, PathOverride)],
    ) {
        for tp in self.types.values_mut() {
            tp.path_overrides.clear();
        }
        for (type_id, path, mode) in overrides {
            if let Some(tp) = self.types.get_mut(type_id) {
                tp.path_overrides.insert(path.clone(), *mode);
            }
        }
        for tp in self.types.values_mut() {
            sync_type_considered_paths(tp);
        }
    }

    pub fn toggle_known_unrelated_type(&mut self, type_id: &str) {
        if let Some(tp) = self.types.get_mut(type_id) {
            tp.known_unrelated = !tp.known_unrelated;
        }
    }

    pub fn rename_type(&mut self, type_id: &str, name: String) {
        if let Some(tp) = self.types.get_mut(type_id) {
            let cleaned = name.trim();
            if cleaned.is_empty() {
                tp.name = None;
            } else {
                tp.name = Some(cleaned.to_string());
            }
        }
    }

    pub fn renamed_types(&self) -> Vec<(String, String)> {
        self.types
            .iter()
            .filter_map(|(type_id, tp)| tp.name.clone().map(|name| (type_id.clone(), name)))
            .collect()
    }

    pub fn apply_renames(&mut self, renames: &[(String, String)]) {
        for (type_id, name) in renames {
            self.rename_type(type_id, name.clone());
        }
    }

    /// Compute the deterministic group_id for a sorted member list.
    /// Sorts internally so callers don't have to.
    pub fn compute_group_id(members: &[String]) -> String {
        let mut sorted: Vec<String> = members.to_vec();
        sorted.sort();
        let payload = serde_json::to_vec(&sorted).unwrap_or_default();
        let mut hasher = Sha256::new();
        hasher.update(&payload);
        let digest = hasher.finalize();
        let full = format!("{:x}", digest);
        format!("g:{}", &full[..10])
    }

    /// Merge `members` into a single logical type with `label`. Returns the
    /// new group id, or None if fewer than 2 valid (non-grouped, existing)
    /// members remain after filtering. Caller is responsible for filter-string
    /// rewrites and calling `refresh_live_anomaly_scores`.
    pub fn merge_types(&mut self, members: &[String], label: String) -> Option<String> {
        let filtered: Vec<String> = members
            .iter()
            .filter(|m| self.types.contains_key(m.as_str()))
            .filter(|m| !self.merge_groups.contains_key(m.as_str()))
            .cloned()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        if filtered.len() < 2 {
            return None;
        }
        let mut sorted = filtered;
        sorted.sort();
        let group_id = Self::compute_group_id(&sorted);

        // Capture each member's display name at merge time.
        let members_prior_name: Vec<Option<String>> = sorted
            .iter()
            .map(|m| self.types.get(m).and_then(|tp| tp.name.clone()))
            .collect();

        // Build the merged TypeProfile by combining each member's stats.
        let cleaned_label = label.trim();
        let label_value = if cleaned_label.is_empty() {
            None
        } else {
            Some(cleaned_label.to_string())
        };
        let mut merged = TypeProfile {
            type_id: group_id.clone(),
            name: label_value,
            count: 0,
            example: Value::Null,
            considered_paths: IndexMap::new(),
            path_overrides: IndexMap::new(),
            path_stats: IndexMap::new(),
            baseline_path_stats: IndexMap::new(),
            known_unrelated: false,
            latest_rate: 0.0,
            latest_uniq: 0.0,
        };
        let mut example_set = false;
        for m in &sorted {
            let Some(tp) = self.types.get(m) else {
                continue;
            };
            if !example_set {
                merged.example = tp.example.clone();
                example_set = true;
            }
            merged.count += tp.count;
            merged.known_unrelated |= tp.known_unrelated;
            for (path, stats) in &tp.path_stats {
                let entry = merged.path_stats.entry(path.clone()).or_default();
                entry.total += stats.total;
                for (tok, cnt) in &stats.values {
                    *entry.values.entry(tok.clone()).or_insert(0) += cnt;
                }
            }
            for (path, stats) in &tp.baseline_path_stats {
                let entry = merged.baseline_path_stats.entry(path.clone()).or_default();
                entry.total += stats.total;
                for (tok, cnt) in &stats.values {
                    *entry.values.entry(tok.clone()).or_insert(0) += cnt;
                }
            }
            for (path, mode) in &tp.path_overrides {
                // ForcedOff wins on conflict.
                match merged.path_overrides.get(path).copied() {
                    Some(PathOverride::ForcedOff) => {}
                    Some(PathOverride::ForcedOn) => {
                        merged.path_overrides.insert(path.clone(), *mode);
                    }
                    None => {
                        merged.path_overrides.insert(path.clone(), *mode);
                    }
                }
            }
        }
        sync_type_considered_paths(&mut merged);

        // Roll up model-level type-keyed maps onto the group id.
        let mut sum_baseline = 0u64;
        for m in &sorted {
            if let Some(v) = self.baseline_counts.remove(m) {
                sum_baseline += v;
            }
        }
        if sum_baseline > 0 {
            *self
                .baseline_counts
                .entry(group_id.clone())
                .or_insert(0) += sum_baseline;
        }

        let mut max_baseline_ts: Option<f64> = None;
        for m in &sorted {
            if let Some(v) = self.baseline_last_seen_by_type.remove(m) {
                max_baseline_ts = Some(match max_baseline_ts {
                    Some(cur) => cur.max(v),
                    None => v,
                });
            }
        }
        if let Some(v) = max_baseline_ts {
            self.baseline_last_seen_by_type.insert(group_id.clone(), v);
        }

        let mut max_last_seen: Option<f64> = None;
        for m in &sorted {
            if let Some(v) = self.last_seen_by_type.remove(m) {
                max_last_seen = Some(match max_last_seen {
                    Some(cur) => cur.max(v),
                    None => v,
                });
            }
        }
        if let Some(v) = max_last_seen {
            self.last_seen_by_type.insert(group_id.clone(), v);
        }

        // Sum per-period state.
        let pids: Vec<u64> = self.period_rate_states.keys().copied().collect();
        for pid in pids {
            let pr = self.period_rate_states.entry(pid).or_default();
            let mut sum_count = 0u64;
            let mut min_first: Option<f64> = None;
            let mut max_last: Option<f64> = None;
            for m in &sorted {
                if let Some(v) = pr.counts.remove(m) {
                    sum_count += v;
                }
                if let Some(v) = pr.type_first_ts.remove(m) {
                    min_first = Some(match min_first {
                        Some(cur) => cur.min(v),
                        None => v,
                    });
                }
                if let Some(v) = pr.type_last_ts.remove(m) {
                    max_last = Some(match max_last {
                        Some(cur) => cur.max(v),
                        None => v,
                    });
                }
            }
            if sum_count > 0 {
                *pr.counts.entry(group_id.clone()).or_insert(0) += sum_count;
            }
            if let Some(v) = min_first {
                pr.type_first_ts.insert(group_id.clone(), v);
            }
            if let Some(v) = max_last {
                pr.type_last_ts.insert(group_id.clone(), v);
            }
        }

        // Rewrite existing events.
        let member_set: HashSet<String> = sorted.iter().cloned().collect();
        for ev in self.events.iter_mut() {
            if member_set.contains(&ev.type_id) {
                let prev = std::mem::replace(&mut ev.type_id, group_id.clone());
                if ev.original_type_id.is_none() {
                    ev.original_type_id = Some(prev);
                }
            }
        }

        // Replace member entries in `types` with the merged profile, then add
        // aliases and register the group.
        for m in &sorted {
            self.types.shift_remove(m);
        }
        self.types.insert(group_id.clone(), merged);

        for m in &sorted {
            self.type_aliases.insert(m.clone(), group_id.clone());
        }
        let group = MergeGroup {
            group_id: group_id.clone(),
            label: cleaned_label.to_string(),
            members: sorted,
            members_prior_name,
        };
        self.merge_groups.insert(group_id.clone(), group);

        Some(group_id)
    }

    /// Unmerge a previously-merged group. Returns the removed `MergeGroup` so
    /// the caller can fan out filter terms. Caller is responsible for filter
    /// expansion and calling `refresh_live_anomaly_scores`.
    pub fn unmerge_group(&mut self, group_id: &str) -> Option<MergeGroup> {
        let group = self.merge_groups.shift_remove(group_id)?;

        // Restore original type ids on events that flowed through this group.
        for ev in self.events.iter_mut() {
            if ev.type_id == group_id {
                let restored = ev
                    .original_type_id
                    .take()
                    .unwrap_or_else(|| group_id.to_string());
                ev.type_id = restored;
            }
        }

        // Remove the group from all type-keyed maps; member entries will be
        // rebuilt by replaying events below.
        self.types.shift_remove(group_id);
        self.baseline_counts.remove(group_id);
        self.baseline_last_seen_by_type.remove(group_id);
        self.last_seen_by_type.remove(group_id);
        for pr in self.period_rate_states.values_mut() {
            pr.counts.remove(group_id);
            pr.type_first_ts.remove(group_id);
            pr.type_last_ts.remove(group_id);
        }

        for m in &group.members {
            self.type_aliases.remove(m);
        }

        // Rebuild per-member state from the live event log. Baseline events are
        // owned by the App layer; the caller is responsible for invoking
        // `recompute_member_state_from_baseline` with those events.
        self.recompute_type_state_for_members(&group.members);

        // Restore each member's prior name (if any) after the rebuild.
        for (m, prior) in group.members.iter().zip(group.members_prior_name.iter()) {
            if let Some(name) = prior.clone() {
                if let Some(tp) = self.types.get_mut(m) {
                    tp.name = Some(name);
                }
            }
        }

        Some(group)
    }

    /// Rebuilds live-event-driven state for the given members by replaying the
    /// current `self.events`. Resets each member's `TypeProfile` and rebuilds
    /// path_stats / baseline_path_stats (the latter from events with no action
    /// period), period_rate_states, last_seen_by_type, and TypeProfile.count.
    /// Does NOT touch baseline_counts / baseline_last_seen_by_type — those are
    /// driven by `App.baseline_events`, not `self.events`.
    pub fn recompute_type_state_for_members(&mut self, members: &[String]) {
        let member_set: HashSet<String> = members.iter().cloned().collect();

        // Reset per-member TypeProfile placeholders and period maps for these members.
        for m in members {
            self.types.shift_remove(m);
        }
        for pr in self.period_rate_states.values_mut() {
            for m in members {
                pr.counts.remove(m);
                pr.type_first_ts.remove(m);
                pr.type_last_ts.remove(m);
            }
        }
        for m in members {
            self.last_seen_by_type.remove(m);
        }

        // Collect a temporary scalar-path cache by re-deriving from each event.
        // We iterate events in order, rebuilding state in lockstep with how
        // ingest would have populated it.
        let events_snapshot = self.events.clone();
        for ev in &events_snapshot {
            let raw = ev.original_type_id.as_ref().unwrap_or(&ev.type_id).clone();
            if !member_set.contains(&raw) {
                continue;
            }
            let mut scalar_paths: Vec<(String, String)> = Vec::new();
            collect_scalar_paths(&ev.obj, "", &mut scalar_paths);
            let in_action_period = ev.in_action_period;

            // Update TypeProfile.
            let tp = self.get_or_create_type_profile(&raw, &ev.obj);
            tp.count += 1;
            if tp.count == 1 {
                tp.example = ev.obj.clone();
            }
            let _ = update_uniqueness(tp, &scalar_paths, in_action_period);

            // Update period_rate_states.
            if let Some(pid) = ev.action_period_id {
                let pr = self.period_rate_states.entry(pid).or_default();
                *pr.counts.entry(raw.clone()).or_insert(0) += 1;
                pr.type_first_ts.entry(raw.clone()).or_insert(ev.ts);
                pr.type_last_ts.insert(raw.clone(), ev.ts);
            }

            self.last_seen_by_type.insert(raw, ev.ts);
        }
    }

    /// Apply a list of saved MergeGroups to a model that has not yet ingested
    /// events. After this call the type_aliases hook will redirect every
    /// matching incoming event into the appropriate group during ingest.
    /// The merged TypeProfile is initialized empty; it will accumulate from
    /// the event stream just like any other type.
    pub fn apply_merge_groups(&mut self, groups: &[MergeGroup]) {
        for g in groups {
            for m in &g.members {
                self.type_aliases.insert(m.clone(), g.group_id.clone());
            }
            self.merge_groups.insert(g.group_id.clone(), g.clone());
            // Seed an empty TypeProfile so renames/filters/etc. work before any
            // events flow in.
            let cleaned = g.label.trim();
            let name = if cleaned.is_empty() {
                None
            } else {
                Some(cleaned.to_string())
            };
            self.types
                .entry(g.group_id.clone())
                .or_insert_with(|| TypeProfile {
                    type_id: g.group_id.clone(),
                    name,
                    count: 0,
                    example: Value::Null,
                    considered_paths: IndexMap::new(),
                    path_overrides: IndexMap::new(),
                    path_stats: IndexMap::new(),
                    baseline_path_stats: IndexMap::new(),
                    known_unrelated: false,
                    latest_rate: 0.0,
                    latest_uniq: 0.0,
                });
        }
    }

    pub fn set_periods(&mut self, periods: Vec<ActionPeriod>) {
        let mut sorted = periods;
        sorted.sort_by(|a, b| a.start.total_cmp(&b.start).then(a.id.cmp(&b.id)));
        self.periods = sorted;
        self.next_period_id = self.periods.iter().map(|p| p.id).max().unwrap_or(0) + 1;
        self.period_rate_states.clear();
        for p in &self.periods {
            self.period_rate_states.entry(p.id).or_default();
        }
        for event in self.events.iter_mut() {
            let pid = self
                .periods
                .iter()
                .find(|p| event.ts >= p.start && p.end.map(|end| event.ts <= end).unwrap_or(true))
                .map(|p| p.id);
            event.action_period_id = pid;
            event.in_action_period = pid.is_some();
        }
    }

    pub fn type_display_name(&self, type_id: &str) -> String {
        if let Some(tp) = self.types.get(type_id) {
            let default = default_type_label(&tp.type_id);
            if let Some(name) = &tp.name {
                format!("{} ({})", name, default)
            } else {
                default
            }
        } else {
            default_type_label(type_id)
        }
    }

    pub fn canonical_type_name(&self, type_id: &str) -> String {
        if let Some(tp) = self.types.get(type_id) {
            if let Some(name) = &tp.name {
                return name.clone();
            }
        }
        default_type_label(type_id)
    }

    pub fn display_type_filter_value(&self, filter: &str) -> String {
        if self.types.contains_key(filter) {
            self.canonical_type_name(filter)
        } else {
            filter.to_string()
        }
    }

    pub fn find_type_index(&self, type_id: &str) -> Option<usize> {
        self.types.get_index_of(type_id)
    }

    pub fn filtered_events<'a>(&'a self, filters: &'a DataFilters) -> Vec<&'a EventRecord> {
        self.filtered_events_in_range(filters, None)
    }

    pub fn filtered_events_in_range<'a>(
        &'a self,
        filters: &'a DataFilters,
        range: Option<(f64, f64)>,
    ) -> Vec<&'a EventRecord> {
        let indices = self.filtered_event_indices(filters, range);
        indices
            .into_iter()
            .rev()
            .filter_map(|idx| self.events.get(idx))
            .collect()
    }

    pub fn filtered_event_indices(
        &self,
        filters: &DataFilters,
        range: Option<(f64, f64)>,
    ) -> Vec<usize> {
        if !filters.has_active() {
            return match range {
                None => (0..self.events.len()).collect(),
                Some((start, end)) => self
                    .events
                    .iter()
                    .enumerate()
                    .filter_map(|(i, e)| (e.ts >= start && e.ts <= end).then_some(i))
                    .collect(),
            };
        }
        let mut out = Vec::new();
        let type_expr = FilterExpr::parse(&filters.type_filter);
        let key_expr = FilterExpr::parse(&filters.key_filter);
        let substring_expr = FilterExpr::parse(&filters.substring_filter);
        let fuzzy_expr = FilterExpr::parse(&filters.fuzzy_filter);
        let exact_expr = FilterExpr::parse(&filters.exact_filter);

        for (idx, e) in self.events.iter().enumerate() {
            if let Some((start, end)) = range {
                if e.ts < start || e.ts > end {
                    continue;
                }
            }
            if !self.matches_filters(
                e,
                &type_expr,
                &key_expr,
                &substring_expr,
                &fuzzy_expr,
                &exact_expr,
            ) {
                continue;
            }
            out.push(idx);
        }
        out
    }

    pub fn filtered_event_slice<'a>(
        &'a self,
        events: &'a [EventRecord],
        filters: &'a DataFilters,
    ) -> Vec<&'a EventRecord> {
        self.filter_events_from_iter(events.iter().rev(), filters, None)
    }

    pub fn filtered_event_indices_in_slice(
        &self,
        events: &[EventRecord],
        filters: &DataFilters,
    ) -> Vec<usize> {
        if !filters.has_active() {
            return (0..events.len()).rev().collect();
        }
        let mut out = Vec::new();
        let type_expr = FilterExpr::parse(&filters.type_filter);
        let key_expr = FilterExpr::parse(&filters.key_filter);
        let substring_expr = FilterExpr::parse(&filters.substring_filter);
        let fuzzy_expr = FilterExpr::parse(&filters.fuzzy_filter);
        let exact_expr = FilterExpr::parse(&filters.exact_filter);
        for idx in (0..events.len()).rev() {
            let e = &events[idx];
            if !self.matches_filters(
                e,
                &type_expr,
                &key_expr,
                &substring_expr,
                &fuzzy_expr,
                &exact_expr,
            ) {
                continue;
            }
            out.push(idx);
        }
        out
    }

    fn filter_events_from_iter<'a>(
        &'a self,
        iter: impl Iterator<Item = &'a EventRecord>,
        filters: &DataFilters,
        range: Option<(f64, f64)>,
    ) -> Vec<&'a EventRecord> {
        let mut out = Vec::new();
        let type_expr = FilterExpr::parse(&filters.type_filter);
        let key_expr = FilterExpr::parse(&filters.key_filter);
        let substring_expr = FilterExpr::parse(&filters.substring_filter);
        let fuzzy_expr = FilterExpr::parse(&filters.fuzzy_filter);
        let exact_expr = FilterExpr::parse(&filters.exact_filter);
        for e in iter {
            if let Some((start, end)) = range {
                if e.ts < start || e.ts > end {
                    continue;
                }
            }
            if !self.matches_filters(
                e,
                &type_expr,
                &key_expr,
                &substring_expr,
                &fuzzy_expr,
                &exact_expr,
            ) {
                continue;
            }
            out.push(e);
        }
        out
    }

    fn matches_filters(
        &self,
        e: &EventRecord,
        type_expr: &FilterExpr,
        key_expr: &FilterExpr,
        substring_expr: &FilterExpr,
        fuzzy_expr: &FilterExpr,
        exact_expr: &FilterExpr,
    ) -> bool {
        let canonical = self.canonical_type_name(&e.type_id).to_lowercase();
        if !type_expr.matches(|term| canonical.contains(&term.to_lowercase())) {
            return false;
        }

        if !key_expr.matches(|term| {
            let term_lc = term.to_lowercase();
            e.keys
                .iter()
                .any(|event_key| event_key.to_lowercase() == term_lc)
        }) {
            return false;
        }

        let mut object_cache: Option<String> = None;
        if !substring_expr.matches(|term| {
            let obj_text = object_cache.get_or_insert_with(|| {
                serde_json::to_string(&e.obj)
                    .unwrap_or_default()
                    .to_lowercase()
            });
            obj_text.contains(&term.to_lowercase())
        }) {
            return false;
        }
        if !fuzzy_expr.matches(|term| {
            let obj_text = object_cache.get_or_insert_with(|| {
                serde_json::to_string(&e.obj)
                    .unwrap_or_default()
                    .to_lowercase()
            });
            fuzzy_match(obj_text.as_str(), &term.to_lowercase())
        }) {
            return false;
        }
        if !exact_expr.matches(|term| {
            parse_exact_filter(term)
                .map(|(k, v)| {
                    values_at_path(&e.obj, k)
                        .into_iter()
                        .any(|actual| exact_value_matches(actual, &v))
                })
                .unwrap_or(false)
        }) {
            return false;
        }

        true
    }

    pub fn closed_periods(&self) -> Vec<&ActionPeriod> {
        self.periods.iter().filter(|p| p.end.is_some()).collect()
    }
}

pub fn default_type_label(type_id: &str) -> String {
    if let Some(rest) = type_id.strip_prefix(TYPE_OVERRIDE_PREFIX) {
        return rest.to_string();
    }
    format!("type-{}", &type_id[..type_id.len().min(8)])
}

fn value_frequency_anomaly(prev: u64, total: u64) -> f64 {
    if prev == 0 {
        return 1.0;
    }
    if total == 0 {
        return 0.0;
    }
    let freq = prev as f64 / total as f64;
    let scaled = (freq / VALUE_ANOMALY_RARE_FREQ).max(0.0);
    (1.0 - scaled.powf(VALUE_ANOMALY_CURVE)).clamp(0.0, 1.0)
}

fn update_uniqueness(
    tp: &mut TypeProfile,
    scalar_paths: &[(String, String)],
    in_action_period: bool,
) -> f64 {
    let mut max_score = 0.0;
    for (path, token) in scalar_paths {
        let stats = tp.path_stats.entry(path.clone()).or_default();
        let prev = stats.values.get(token).copied().unwrap_or(0);
        let total = stats.total as f64;
        let distinct = stats.values.len() as f64;

        let auto = auto_consider_path(path.as_str(), stats);
        let considered = tp.considered_paths.entry(path.clone()).or_insert(auto);
        *considered = match tp.path_overrides.get(path.as_str()).copied() {
            Some(PathOverride::ForcedOn) => true,
            Some(PathOverride::ForcedOff) => false,
            None => auto,
        };

        if *considered && total > 6.0 {
            let s = if in_action_period {
                let baseline_stats = tp.baseline_path_stats.get(path.as_str());
                if let Some(bstats) = baseline_stats {
                    let baseline_prev = bstats.values.get(token.as_str()).copied().unwrap_or(0);
                    value_frequency_anomaly(baseline_prev, bstats.total)
                } else {
                    1.0
                }
            } else {
                value_frequency_anomaly(prev, stats.total)
            };
            if s > max_score {
                max_score = s;
            }
        }

        stats.total += 1;
        *stats.values.entry(token.clone()).or_insert(0) += 1;
        if !in_action_period {
            let baseline_stats = tp.baseline_path_stats.entry(path.clone()).or_default();
            baseline_stats.total += 1;
            *baseline_stats.values.entry(token.clone()).or_insert(0) += 1;
        }
        // Fast path for very high uniqueness once enough support exists.
        if !tp.path_overrides.contains_key(path.as_str()) && total >= 12.0 {
            let unique_ratio = (distinct + 1.0) / (total + 1.0);
            if unique_ratio >= 0.80 {
                if let Some(v) = tp.considered_paths.get_mut(path.as_str()) {
                    *v = false;
                }
            }
        }
    }

    max_score
}

fn sync_type_considered_paths(tp: &mut TypeProfile) {
    for (path, stats) in tp.path_stats.iter() {
        let auto = auto_consider_path(path.as_str(), stats);
        let considered = match tp.path_overrides.get(path.as_str()).copied() {
            Some(PathOverride::ForcedOn) => true,
            Some(PathOverride::ForcedOff) => false,
            None => auto,
        };
        tp.considered_paths.insert(path.clone(), considered);
    }
}

fn is_volatile_path(path: &str) -> bool {
    let last = path
        .trim_end_matches("[]")
        .rsplit('.')
        .next()
        .unwrap_or(path);
    [
        "ts",
        "timestamp",
        "time",
        "event_time",
        "created_at",
        "updated_at",
        "nonce",
        "request_id",
        "trace_id",
        "span_id",
    ]
    .iter()
    .any(|&v| last.eq_ignore_ascii_case(v))
}

fn auto_consider_path(path: &str, stats: &PathStats) -> bool {
    if is_volatile_path(path) {
        return false;
    }

    let total = stats.total as f64;
    if total < 10.0 {
        return true;
    }
    let distinct = stats.values.len() as f64;
    let unique_ratio = distinct / total;
    if unique_ratio >= 0.80 {
        return false;
    }

    // Numeric flat-ish heuristic: many distinct numeric values with no strong mode.
    let mut numeric_distinct = 0usize;
    let mut numeric_total = 0u64;
    let mut numeric_top = 0u64;
    for (k, c) in &stats.values {
        if k.starts_with("n:") {
            numeric_distinct += 1;
            numeric_total += *c;
            if *c > numeric_top {
                numeric_top = *c;
            }
        }
    }
    if numeric_total >= 12 && numeric_distinct >= 10 {
        let top_share = numeric_top as f64 / numeric_total as f64;
        if top_share <= 0.12 {
            return false;
        }
    }
    true
}

fn child_path(parent: &str, key: &str) -> String {
    if parent.is_empty() {
        key.to_string()
    } else {
        format!("{}.{}", parent, key)
    }
}

fn collect_scalar_paths(v: &Value, path: &str, out: &mut Vec<(String, String)>) {
    match v {
        Value::Object(map) => {
            for (k, child) in map {
                let p = child_path(path, k);
                collect_scalar_paths(child, &p, out);
            }
        }
        Value::Array(arr) => {
            for child in arr {
                let p = format!("{}[]", path);
                collect_scalar_paths(child, &p, out);
            }
        }
        _ => {
            out.push((path.to_string(), value_token(v)));
        }
    }
}

pub fn classify_event(obj: &Value) -> (String, Vec<String>) {
    let type_id = type_id_for(obj);
    let keys = collect_all_paths(obj);
    (type_id, keys)
}

pub fn prepare_event(obj: Value) -> PreparedEvent {
    let type_id = type_id_for(&obj);
    let keys = collect_all_paths(&obj);
    let mut scalar_paths = Vec::new();
    collect_scalar_paths(&obj, "", &mut scalar_paths);
    PreparedEvent {
        obj,
        type_id,
        keys,
        scalar_paths,
    }
}

const TYPE_OVERRIDE_PREFIX: &str = "t:";

fn type_id_for(obj: &Value) -> String {
    if let Some(name) = type_field_value(obj) {
        return format!("{}{}", TYPE_OVERRIDE_PREFIX, name);
    }
    let shape = extract_shape(obj);
    structural_hash(&shape)
}

/// Returns the `_type` override value when it is a non-empty trimmed string.
/// Non-string / empty values fall through to structural classification; the
/// strict validation that rejects those lives in the ingest pipeline.
pub fn type_field_value(obj: &Value) -> Option<String> {
    let raw = obj.get("_type")?.as_str()?;
    let trimmed = raw.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

pub fn value_token(v: &Value) -> String {
    match v {
        Value::Null => "null".to_string(),
        Value::Bool(b) => format!("b:{}", b),
        Value::Number(n) => format!("n:{}", n),
        Value::String(s) => format!("s:{}", s),
        Value::Array(_) => "array".to_string(),
        Value::Object(_) => "object".to_string(),
    }
}

pub fn type_is_negated_in_filter(filter: &str, canonical_name: &str) -> bool {
    let expr = FilterExpr::parse(filter);
    let canonical = canonical_name.to_lowercase();
    !expr.groups.is_empty()
        && expr.groups.iter().all(|group| {
            group
                .iter()
                .any(|term| term.negated && term.value.to_lowercase() == canonical)
        })
}

pub fn toggle_negated_type_in_filter(filter: &str, canonical_name: &str) -> String {
    let canonical = canonical_name.trim();
    if canonical.is_empty() {
        return filter.trim().to_string();
    }
    let mut expr = FilterExpr::parse(filter);
    let needle = canonical.to_lowercase();
    let present = !expr.groups.is_empty()
        && expr.groups.iter().all(|group| {
            group
                .iter()
                .any(|term| term.negated && term.value.to_lowercase() == needle)
        });
    if present {
        for group in &mut expr.groups {
            group.retain(|term| !(term.negated && term.value.to_lowercase() == needle));
        }
        expr.groups.retain(|group| !group.is_empty());
    } else {
        for group in &mut expr.groups {
            group.retain(|term| !(!term.negated && term.value.to_lowercase() == needle));
        }
        expr.groups.retain(|group| !group.is_empty());
        if expr.groups.is_empty() {
            expr.groups.push(vec![FilterTerm {
                negated: true,
                value: canonical.to_string(),
            }]);
        } else {
            for group in &mut expr.groups {
                group.push(FilterTerm {
                    negated: true,
                    value: canonical.to_string(),
                });
            }
        }
    }
    format_filter_expr(&expr)
}

pub fn replace_positive_type_filters(filter: &str, canonical_name: &str) -> String {
    let canonical = canonical_name.trim();
    let needle = canonical.to_lowercase();
    let mut terms = common_negated_type_terms(filter);
    if !canonical.is_empty() {
        terms.retain(|term| term.value.to_lowercase() != needle);
        terms.push(FilterTerm {
            negated: false,
            value: canonical.to_string(),
        });
    }
    format_filter_expr(&FilterExpr {
        groups: if terms.is_empty() {
            Vec::new()
        } else {
            vec![terms]
        },
    })
}

pub fn clear_positive_type_filters(filter: &str) -> String {
    let terms = common_negated_type_terms(filter);
    format_filter_expr(&FilterExpr {
        groups: if terms.is_empty() {
            Vec::new()
        } else {
            vec![terms]
        },
    })
}

pub fn rename_type_terms_in_filter(filter: &str, old_name: &str, new_name: &str) -> String {
    let old_name = old_name.trim();
    let new_name = new_name.trim();
    if old_name.is_empty() || old_name == new_name {
        return filter.trim().to_string();
    }
    let mut expr = FilterExpr::parse(filter);
    for group in &mut expr.groups {
        for term in group {
            if term.value == old_name {
                term.value = new_name.to_string();
            }
        }
    }
    format_filter_expr(&expr)
}

/// Applies a batch of `(old_name, new_name)` substitutions to a filter string.
/// Used when a remote operator renames a type and we need to rewrite the local
/// filter terms in-place so they keep matching.
pub fn apply_rename_batch_to_filter(filter: &str, renames: &[(String, String)]) -> String {
    let mut out = filter.to_string();
    for (old, new) in renames {
        out = rename_type_terms_in_filter(&out, old, new);
    }
    out
}

/// Drop duplicate terms within each AND group and drop duplicate groups.
pub fn dedupe_filter_terms(filter: &str) -> String {
    let mut expr = FilterExpr::parse(filter);
    for group in &mut expr.groups {
        let mut seen: HashSet<(bool, String)> = HashSet::new();
        group.retain(|term| seen.insert((term.negated, term.value.to_lowercase())));
    }
    expr.groups.retain(|g| !g.is_empty());
    // Deduplicate identical groups.
    let mut group_seen: HashSet<String> = HashSet::new();
    expr.groups.retain(|g| {
        let key = g
            .iter()
            .map(|t| format!("{}{}", if t.negated { "!" } else { "" }, t.value.to_lowercase()))
            .collect::<Vec<_>>()
            .join("\u{1f}");
        group_seen.insert(key)
    });
    format_filter_expr(&expr)
}

/// Expand a merged group's label into its member names. Positive references
/// to the label fan a group out into one copy per member; negative references
/// to the label stack !member terms within the group.
pub fn expand_merged_label_in_filter(
    filter: &str,
    label: &str,
    member_names: &[String],
) -> String {
    let label = label.trim();
    if label.is_empty() || member_names.is_empty() {
        return filter.trim().to_string();
    }
    let label_lc = label.to_lowercase();
    let expr = FilterExpr::parse(filter);
    if expr.groups.is_empty() {
        return filter.trim().to_string();
    }

    let mut out_groups: Vec<Vec<FilterTerm>> = Vec::new();
    for group in expr.groups {
        // Determine if any positive label term is present.
        let positive_indices: Vec<usize> = group
            .iter()
            .enumerate()
            .filter(|(_, t)| !t.negated && t.value.to_lowercase() == label_lc)
            .map(|(i, _)| i)
            .collect();

        // Stage 1: positive fan-out.
        let stage_groups: Vec<Vec<FilterTerm>> = if positive_indices.is_empty() {
            vec![group.clone()]
        } else {
            let mut variants: Vec<Vec<FilterTerm>> = Vec::new();
            for member in member_names {
                let mut g = group.clone();
                for &idx in &positive_indices {
                    g[idx] = FilterTerm {
                        negated: false,
                        value: member.clone(),
                    };
                }
                variants.push(g);
            }
            variants
        };

        // Stage 2: for each stage group, expand negative label terms by
        // replacing each one with a sequence of !member terms (stacked).
        for g in stage_groups {
            let mut expanded: Vec<FilterTerm> = Vec::new();
            for term in g {
                if term.negated && term.value.to_lowercase() == label_lc {
                    for member in member_names {
                        expanded.push(FilterTerm {
                            negated: true,
                            value: member.clone(),
                        });
                    }
                } else {
                    expanded.push(term);
                }
            }
            out_groups.push(expanded);
        }
    }
    let expanded_expr = FilterExpr { groups: out_groups };
    let formatted = format_filter_expr(&expanded_expr);
    dedupe_filter_terms(&formatted)
}

fn parse_exact_filter(filter: &str) -> Option<(&str, String)> {
    let (k, v) = filter.split_once('=')?;
    Some((k.trim(), v.trim().to_string()))
}

fn parse_filter_term(raw: &str) -> Option<FilterTerm> {
    let mut s = raw.trim();
    if s.is_empty() {
        return None;
    }
    let mut negated = false;
    while let Some(rest) = s.strip_prefix('!') {
        negated = !negated;
        s = rest.trim_start();
    }
    let value = unquote_filter_literal(s);
    if value.is_empty() {
        return None;
    }
    Some(FilterTerm { negated, value })
}

fn unquote_filter_literal(s: &str) -> String {
    let t = s.trim();
    if t.len() < 2 {
        return t.to_string();
    }
    let first = t.chars().next().unwrap_or('\0');
    let last = t.chars().last().unwrap_or('\0');
    if !((first == '"' && last == '"') || (first == '\'' && last == '\'')) {
        return t.to_string();
    }
    let inner = &t[first.len_utf8()..t.len() - last.len_utf8()];
    let mut out = String::new();
    let mut escaped = false;
    for ch in inner.chars() {
        if escaped {
            out.push(ch);
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else {
            out.push(ch);
        }
    }
    out
}

fn read_expr_operator(raw: &str, idx: usize) -> Option<(bool, usize)> {
    let rest = &raw[idx..];
    if rest.starts_with("&&") {
        return Some((false, 2));
    }
    if rest.starts_with("||") {
        return Some((true, 2));
    }
    if rest.starts_with(',') {
        return Some((false, 1));
    }
    if rest.starts_with('|') {
        return Some((true, 1));
    }
    if starts_word_op(raw, idx, "and") {
        return Some((false, 3));
    }
    if starts_word_op(raw, idx, "or") {
        return Some((true, 2));
    }
    None
}

fn starts_word_op(raw: &str, idx: usize, op: &str) -> bool {
    let rest = &raw[idx..];
    if !rest.starts_with(op) {
        return false;
    }
    let before_ok = if idx == 0 {
        true
    } else {
        raw[..idx]
            .chars()
            .last()
            .map(is_expr_boundary)
            .unwrap_or(true)
    };
    let end = idx + op.len();
    let after_ok = if end >= raw.len() {
        true
    } else {
        raw[end..]
            .chars()
            .next()
            .map(is_expr_boundary)
            .unwrap_or(true)
    };
    before_ok && after_ok
}

fn format_filter_expr(expr: &FilterExpr) -> String {
    expr.groups
        .iter()
        .map(|group| {
            group
                .iter()
                .map(format_filter_term)
                .collect::<Vec<_>>()
                .join(" && ")
        })
        .collect::<Vec<_>>()
        .join(" || ")
}

fn common_negated_type_terms(filter: &str) -> Vec<FilterTerm> {
    let expr = FilterExpr::parse(filter);
    let Some(first_group) = expr.groups.first() else {
        return Vec::new();
    };
    first_group
        .iter()
        .filter(|term| term.negated)
        .filter(|term| {
            expr.groups.iter().all(|group| {
                group.iter().any(|candidate| {
                    candidate.negated && candidate.value.eq_ignore_ascii_case(term.value.as_str())
                })
            })
        })
        .cloned()
        .collect()
}

fn format_filter_term(term: &FilterTerm) -> String {
    let value = quote_filter_literal(&term.value);
    if term.negated {
        format!("!{}", value)
    } else {
        value
    }
}

fn quote_filter_literal(raw: &str) -> String {
    let needs_quotes = raw.is_empty()
        || raw.chars().any(|ch| {
            ch.is_whitespace()
                || matches!(ch, '"' | '\'' | '\\' | ',' | '|' | '&' | '!' | '(' | ')')
        })
        || raw.contains("and")
        || raw.contains("or");
    if !needs_quotes {
        return raw.to_string();
    }
    let escaped = raw.replace('\\', "\\\\").replace('"', "\\\"");
    format!("\"{}\"", escaped)
}

fn is_expr_boundary(ch: char) -> bool {
    ch.is_whitespace() || matches!(ch, ',' | '|' | '&' | '(' | ')' | '[' | ']')
}

fn exact_value_matches(actual: &Value, expected: &str) -> bool {
    if value_token(actual) == expected {
        return true;
    }
    match actual {
        Value::String(s) => s == expected,
        Value::Number(n) => n.to_string() == expected,
        Value::Bool(b) => b.to_string() == expected,
        Value::Null => expected == "null",
        Value::Array(_) | Value::Object(_) => false,
    }
}

pub fn value_at_path<'a>(v: &'a Value, path: &str) -> Option<&'a Value> {
    let mut cur = v;
    for part in path.split('.') {
        let (key, ops) = parse_segment(part)?;
        if !key.is_empty() {
            cur = cur.get(key)?;
        }
        for op in ops {
            match op {
                Some(index) => cur = cur.as_array()?.get(index)?,
                None => cur = cur.as_array()?.first()?,
            }
        }
    }
    Some(cur)
}

pub fn values_at_path<'a>(v: &'a Value, path: &str) -> Vec<&'a Value> {
    let parts: Vec<&str> = path.split('.').collect();
    let mut out = Vec::new();
    collect_values_at_path(v, &parts, &mut out);
    out
}

pub fn normalize_path(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    let bytes = path.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'[' {
            let mut j = i + 1;
            while j < bytes.len() && bytes[j].is_ascii_digit() {
                j += 1;
            }
            if j > i + 1 && j < bytes.len() && bytes[j] == b']' {
                out.push_str("[]");
                i = j + 1;
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

/// Parse a dot-split path segment into a key prefix and an ordered list of
/// bracket operations. Each op is `None` for a wildcard `[]` or `Some(n)` for
/// a concrete index `[n]`. Returns `None` on syntactically malformed input.
fn parse_segment(part: &str) -> Option<(&str, Vec<Option<usize>>)> {
    let bracket_start = part.find('[').unwrap_or(part.len());
    let key = &part[..bracket_start];
    let mut ops: Vec<Option<usize>> = Vec::new();
    let mut rest = &part[bracket_start..];
    while !rest.is_empty() {
        let inner = rest.strip_prefix('[')?;
        let close = inner.find(']')?;
        let idx_str = &inner[..close];
        if idx_str.is_empty() {
            ops.push(None);
        } else if idx_str.chars().all(|c| c.is_ascii_digit()) {
            ops.push(Some(idx_str.parse().ok()?));
        } else {
            return None;
        }
        rest = &inner[close + 1..];
    }
    Some((key, ops))
}

fn collect_values_at_path<'a>(cur: &'a Value, parts: &[&str], out: &mut Vec<&'a Value>) {
    let Some((part, rest)) = parts.split_first() else {
        out.push(cur);
        return;
    };
    let Some((key, ops)) = parse_segment(part) else {
        return;
    };
    let mut nodes: Vec<&Value> = vec![cur];
    if !key.is_empty() {
        nodes = nodes.into_iter().filter_map(|n| n.get(key)).collect();
    }
    for op in ops {
        match op {
            Some(index) => {
                nodes = nodes
                    .into_iter()
                    .filter_map(|n| n.as_array()?.get(index))
                    .collect();
            }
            None => {
                nodes = nodes
                    .into_iter()
                    .filter_map(|n| n.as_array())
                    .flatten()
                    .collect();
            }
        }
    }
    for node in nodes {
        collect_values_at_path(node, rest, out);
    }
}

fn fuzzy_match(haystack: &str, needle: &str) -> bool {
    if needle.is_empty() {
        return true;
    }
    let mut n = needle.chars();
    let mut target = n.next();
    for c in haystack.chars() {
        if Some(c) == target {
            target = n.next();
            if target.is_none() {
                return true;
            }
        }
    }
    false
}

pub fn extract_shape(v: &Value) -> Value {
    match v {
        Value::Null => Value::String("null".to_string()),
        Value::Bool(_) => Value::String("boolean".to_string()),
        Value::Number(_) => Value::String("number".to_string()),
        Value::String(_) => Value::String("string".to_string()),
        Value::Array(arr) => {
            if arr.is_empty() {
                return Value::Array(vec![Value::String("empty".to_string())]);
            }
            let mut uniq: HashMap<String, Value> = HashMap::new();
            for item in arr.iter().take(5) {
                let shape = extract_shape(item);
                let key = serde_json::to_string(&shape).unwrap_or_default();
                uniq.insert(key, shape);
            }
            let mut vals: Vec<(String, Value)> = uniq.into_iter().collect();
            vals.sort_by(|a, b| a.0.cmp(&b.0));
            Value::Array(vals.into_iter().map(|(_, v)| v).collect())
        }
        Value::Object(map) => {
            let mut entries: Vec<(&String, &Value)> = map.iter().collect();
            entries.sort_by(|a, b| a.0.cmp(b.0));
            let mut out = Map::new();
            for (k, child) in entries {
                out.insert(k.clone(), extract_shape(child));
            }
            Value::Object(out)
        }
    }
}

pub fn structural_hash(shape: &Value) -> String {
    let payload = serde_json::to_vec(shape).unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(payload);
    let digest = hasher.finalize();
    let full = format!("{:x}", digest);
    full[..12].to_string()
}

fn collect_all_paths(v: &Value) -> Vec<String> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    collect_paths(v, "", &mut out, &mut seen);
    out
}

pub fn collect_indexed_paths(v: &Value) -> Vec<String> {
    let mut out = Vec::new();
    collect_paths_indexed(v, "", &mut out);
    out
}

fn collect_paths(v: &Value, path: &str, out: &mut Vec<String>, seen: &mut HashSet<String>) {
    match v {
        Value::Object(map) => {
            for (k, child) in map {
                let p = child_path(path, k);
                if seen.insert(p.clone()) {
                    out.push(p.clone());
                }
                collect_paths(child, &p, out, seen);
            }
        }
        Value::Array(arr) => {
            let p = format!("{}[]", path);
            if seen.insert(p.clone()) {
                out.push(p.clone());
            }
            for child in arr {
                collect_paths(child, &p, out, seen);
            }
        }
        _ => {}
    }
}

fn collect_paths_indexed(v: &Value, path: &str, out: &mut Vec<String>) {
    match v {
        Value::Object(map) => {
            for (k, child) in map {
                let p = child_path(path, k);
                out.push(p.clone());
                collect_paths_indexed(child, &p, out);
            }
        }
        Value::Array(arr) => {
            for (idx, child) in arr.iter().enumerate() {
                let p = indexed_child_path(path, idx);
                if !matches!(child, Value::Object(_) | Value::Array(_)) {
                    out.push(p.clone());
                }
                collect_paths_indexed(child, &p, out);
            }
        }
        _ => {}
    }
}

fn indexed_child_path(path: &str, idx: usize) -> String {
    if path.is_empty() {
        format!("[{idx}]")
    } else {
        format!("{path}[{idx}]")
    }
}

fn normalized_rate_anomaly(action_rate: f64, baseline_rate: f64) -> f64 {
    if action_rate <= 0.0 && baseline_rate <= 0.0 {
        return 0.0;
    }
    if baseline_rate <= 0.0 {
        return 1.0;
    }
    if action_rate <= 0.0 {
        // type went silent during action period
        return 1.0;
    }
    1.0 - action_rate.min(baseline_rate) / action_rate.max(baseline_rate)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn shape_and_hash_are_structural_not_value_based() {
        let a = json!({
            "z": 1,
            "nested": { "b": true, "a": "x" },
            "arr": [1, "s", {"k": 1}]
        });
        let b = json!({
            "arr": [5, "other", {"k": 9}],
            "nested": { "a": "y", "b": false },
            "z": 999
        });

        let shape_a = extract_shape(&a);
        let shape_b = extract_shape(&b);
        assert_eq!(shape_a, shape_b);
        assert_eq!(structural_hash(&shape_a), structural_hash(&shape_b));
    }

    #[test]
    fn value_at_path_handles_array_notation() {
        let v = json!({
            "items": [{"name": "first"}, {"name": "second"}],
            "meta": {"ok": true}
        });
        assert_eq!(value_at_path(&v, "items[].name"), Some(&json!("first")));
        assert_eq!(value_at_path(&v, "items[1].name"), Some(&json!("second")));
        let values = values_at_path(&v, "items[].name");
        assert_eq!(values.len(), 2);
        assert_eq!(values[0], &json!("first"));
        assert_eq!(values[1], &json!("second"));
        assert_eq!(value_at_path(&v, "meta.ok"), Some(&json!(true)));
        assert_eq!(value_at_path(&v, "items[].missing"), None);
    }

    #[test]
    fn value_at_path_handles_nested_arrays() {
        let v = json!({
            "matrix": [[1, 2], [3, 4]],
            "grid": [[{"x": 1, "y": 2}, {"x": 3}], [{"x": 4, "y": 5}]]
        });
        // Concrete indices into nested arrays
        assert_eq!(value_at_path(&v, "matrix[0][0]"), Some(&json!(1)));
        assert_eq!(value_at_path(&v, "matrix[0][1]"), Some(&json!(2)));
        assert_eq!(value_at_path(&v, "matrix[1][0]"), Some(&json!(3)));
        assert_eq!(value_at_path(&v, "matrix[1][1]"), Some(&json!(4)));
        // Object field inside a nested array
        assert_eq!(value_at_path(&v, "grid[0][0].x"), Some(&json!(1)));
        assert_eq!(value_at_path(&v, "grid[0][1].x"), Some(&json!(3)));
        assert_eq!(value_at_path(&v, "grid[1][0].x"), Some(&json!(4)));
        // values_at_path with wildcard notation collects all matching scalars
        let vals = values_at_path(&v, "matrix[][]");
        assert_eq!(vals, vec![&json!(1), &json!(2), &json!(3), &json!(4)]);
        let xs = values_at_path(&v, "grid[][].x");
        assert_eq!(xs, vec![&json!(1), &json!(3), &json!(4)]);
    }

    #[test]
    fn normalize_path_collapses_indexed_array_segments() {
        assert_eq!(normalize_path("items[17].name"), "items[].name");
        assert_eq!(
            normalize_path("payload.list[0].values[4]"),
            "payload.list[].values[]"
        );
        assert_eq!(normalize_path("matrix[0][1]"), "matrix[][]");
        assert_eq!(normalize_path("grid[0][0].x"), "grid[][].x");
    }

    #[test]
    fn collect_indexed_paths_preserves_array_instances() {
        let v = json!({
            "items": [
                {"shared": 1, "only_first": true},
                {"shared": 2, "only_second": true}
            ]
        });
        assert_eq!(
            collect_indexed_paths(&v),
            vec![
                "items",
                "items[0].shared",
                "items[0].only_first",
                "items[1].shared",
                "items[1].only_second",
            ]
        );
    }

    #[test]
    fn collect_indexed_paths_keeps_scalar_array_items_selectable() {
        let v = json!({
            "items": ["first", "second"]
        });
        assert_eq!(
            collect_indexed_paths(&v),
            vec!["items", "items[0]", "items[1]"]
        );
    }

    #[test]
    fn prepare_event_collects_keys_from_all_array_items() {
        let prepared = prepare_event(json!({
            "items": [
                {"shared": 1},
                {"shared": 2},
                {"late_key": true}
            ]
        }));
        assert!(prepared.keys.contains(&"items[].shared".to_string()));
        assert!(prepared.keys.contains(&"items[].late_key".to_string()));
    }

    #[test]
    fn data_filters_count_active_fields() {
        let mut f = DataFilters::default();
        assert_eq!(f.active_count(), 0);
        assert!(!f.has_active());

        f.key_filter = "a,b".to_string();
        f.exact_filter = "x=1".to_string();
        assert_eq!(f.active_count(), 2);
        assert!(f.has_active());
    }

    #[test]
    fn period_toggle_requires_event_and_closes_with_last_timestamp() {
        let mut model = AnalyzerModel::new();
        assert!(!model.toggle_period());
        assert!(model.periods.is_empty());

        model.ingest(json!({"event":"baseline"}), 100.0);
        assert!(model.toggle_period());
        assert_eq!(model.periods.len(), 1);
        let p = model.active_period().expect("period should be open");
        assert!(p.start > 100.0);
        assert_eq!(p.end, None);

        model.ingest(json!({"event":"inside"}), 101.0);
        assert!(model.toggle_period());
        assert!(model.active_period().is_none());
        assert_eq!(model.periods[0].end, Some(101.0));
    }

    #[test]
    fn rename_and_display_type_name_behaves_as_expected() {
        let mut model = AnalyzerModel::new();
        model.ingest(json!({"event":"login","user":"a"}), 1.0);
        let type_id = model.events.first().expect("event exists").type_id.clone();

        let default_name = model.type_display_name(&type_id);
        assert!(default_name.starts_with("type-"));

        model.rename_type(&type_id, "  Login Event  ".to_string());
        let renamed = model.type_display_name(&type_id);
        assert!(renamed.starts_with("Login Event (type-"));

        model.rename_type(&type_id, "   ".to_string());
        assert!(model.type_display_name(&type_id).starts_with("type-"));
    }

    #[test]
    fn filtered_events_support_type_key_fuzzy_and_exact() {
        let mut model = AnalyzerModel::new();
        model.ingest(
            json!({"event":"login","user":"alice","meta":{"host":"web"}}),
            10.0,
        );
        model.ingest(json!({"event":"purchase","user":"bob","amount":42}), 20.0);

        let login_type = model.events.last().expect("latest exists").type_id.clone();
        model.rename_type(&login_type, "Purchase".to_string());

        let type_filter = DataFilters {
            type_filter: "purchase".to_string(),
            ..DataFilters::default()
        };
        let type_filtered = model.filtered_events(&type_filter);
        assert_eq!(type_filtered.len(), 1);
        assert_eq!(
            value_at_path(&type_filtered[0].obj, "event"),
            Some(&json!("purchase"))
        );

        let key_filter = DataFilters {
            key_filter: "meta.host".to_string(),
            ..DataFilters::default()
        };
        let key_filtered = model.filtered_events(&key_filter);
        assert_eq!(key_filtered.len(), 1);
        assert_eq!(
            value_at_path(&key_filtered[0].obj, "user"),
            Some(&json!("alice"))
        );

        let fuzzy_filter = DataFilters {
            fuzzy_filter: "pch".to_string(),
            ..DataFilters::default()
        };
        let fuzzy_filtered = model.filtered_events(&fuzzy_filter);
        assert_eq!(fuzzy_filtered.len(), 1);

        let exact_filter = DataFilters {
            exact_filter: "user=alice".to_string(),
            ..DataFilters::default()
        };
        let exact_filtered = model.filtered_events(&exact_filter);
        assert_eq!(exact_filtered.len(), 1);
        assert_eq!(
            value_at_path(&exact_filtered[0].obj, "event"),
            Some(&json!("login"))
        );

        let exact_token_filter = DataFilters {
            exact_filter: "user=s:alice".to_string(),
            ..DataFilters::default()
        };
        let exact_token_filtered = model.filtered_events(&exact_token_filter);
        assert_eq!(exact_token_filtered.len(), 1);
    }

    #[test]
    fn known_unrelated_flag_no_longer_suppresses_events() {
        let mut model = AnalyzerModel::new();
        model.ingest(json!({"event":"noise","k":1}), 1.0);
        model.ingest(json!({"event":"signal","k":2,"ctx":{"source":"web"}}), 2.0);

        let signal_type = model.events.last().expect("event exists").type_id.clone();
        model.toggle_known_unrelated_type(&signal_type);

        let filters = DataFilters::default();
        let events = model.filtered_events(&filters);
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn composed_and_negative_filters_work() {
        let mut model = AnalyzerModel::new();
        model.ingest(json!({"event":"login","user":"alice","host":"api"}), 1.0);
        model.ingest(json!({"event":"login","user":"bob","host":"worker"}), 2.0);
        model.ingest(json!({"event":"logout","user":"alice","host":"api"}), 3.0);

        let filters = DataFilters {
            exact_filter: "user=alice && !event=logout".to_string(),
            ..DataFilters::default()
        };
        let events = model.filtered_events(&filters);
        assert_eq!(events.len(), 1);
        assert_eq!(value_at_path(&events[0].obj, "user"), Some(&json!("alice")));
    }

    #[test]
    fn quoted_terms_support_whitespace_and_negation() {
        let mut model = AnalyzerModel::new();
        model.ingest(json!({"msg":"alpha beta gamma"}), 1.0);
        model.ingest(json!({"msg":"alpha and beta"}), 2.0);

        let filters = DataFilters {
            substring_filter: "!\"and beta\" && \"alpha beta\"".to_string(),
            ..DataFilters::default()
        };
        let events = model.filtered_events(&filters);
        assert_eq!(events.len(), 1);
        assert_eq!(
            value_at_path(&events[0].obj, "msg"),
            Some(&json!("alpha beta gamma"))
        );
    }

    #[test]
    fn exact_filter_matches_any_array_item_value() {
        let mut model = AnalyzerModel::new();
        model.ingest(json!({"items":[{"name":"first"},{"name":"second"}]}), 1.0);

        let filters = DataFilters {
            exact_filter: "items[].name=s:second".to_string(),
            ..DataFilters::default()
        };
        let events = model.filtered_events(&filters);
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn exact_filter_matches_any_scalar_array_item_type() {
        let mut model = AnalyzerModel::new();
        model.ingest(json!({"items":["alpha", true, null]}), 1.0);

        for exact in ["items[]=s:alpha", "items[]=b:true", "items[]=null"] {
            let filters = DataFilters {
                exact_filter: exact.to_string(),
                ..DataFilters::default()
            };
            let events = model.filtered_events(&filters);
            assert_eq!(events.len(), 1, "filter {exact} should match");
        }
    }

    #[test]
    fn toggle_negated_type_in_filter_adds_term_to_empty_filter() {
        let filter = toggle_negated_type_in_filter("", "Order");
        assert_eq!(filter, "!Order");
        assert!(type_is_negated_in_filter(&filter, "Order"));
    }

    #[test]
    fn toggle_negated_type_in_filter_round_trips_across_or_groups() {
        let filter = toggle_negated_type_in_filter("foo || bar", "Order");
        assert_eq!(filter, "foo && !Order || bar && !Order");
        assert!(type_is_negated_in_filter(&filter, "Order"));

        let cleared = toggle_negated_type_in_filter(&filter, "Order");
        assert_eq!(cleared, "foo || bar");
    }

    #[test]
    fn positive_type_filter_is_not_treated_as_explicit_negation() {
        assert!(!type_is_negated_in_filter("Order", "Other"));
        assert!(!type_is_negated_in_filter("foo || bar", "Order"));
    }

    #[test]
    fn replace_and_clear_positive_type_filters_preserve_negations() {
        let filter = replace_positive_type_filters("!Noise && OldType", "Order");
        assert_eq!(filter, "!Noise && Order");

        let cleared = clear_positive_type_filters(&filter);
        assert_eq!(cleared, "!Noise");
    }

    #[test]
    fn replace_positive_type_filters_drops_positive_terms_even_when_mixed_in_middle() {
        let filter = replace_positive_type_filters("!Noise && OldType && !Other", "Order");
        assert_eq!(filter, "!Noise && !Other && Order");

        let cleared = clear_positive_type_filters(&filter);
        assert_eq!(cleared, "!Noise && !Other");
    }

    #[test]
    fn replace_positive_type_filters_collapses_multiple_positive_terms() {
        let filter = replace_positive_type_filters("Foo && !Noise && Bar && Baz", "Order");
        assert_eq!(filter, "!Noise && Order");

        let cleared = clear_positive_type_filters(&filter);
        assert_eq!(cleared, "!Noise");
    }

    #[test]
    fn applying_t_strips_matching_negation_for_same_type() {
        let filter = replace_positive_type_filters("!login", "login");
        assert_eq!(filter, "login");
        assert!(!type_is_negated_in_filter(&filter, "login"));
    }

    #[test]
    fn applying_t_preserves_unrelated_negations_for_same_type() {
        let filter = replace_positive_type_filters("!login && !other", "login");
        assert_eq!(filter, "!other && login");
        assert!(!type_is_negated_in_filter(&filter, "login"));
    }

    #[test]
    fn applying_u_strips_matching_positive_for_same_type() {
        let filter = toggle_negated_type_in_filter("login", "login");
        assert_eq!(filter, "!login");
        assert!(type_is_negated_in_filter(&filter, "login"));
    }

    #[test]
    fn applying_u_strips_positive_alongside_unrelated_term() {
        let filter = toggle_negated_type_in_filter("foo && login", "login");
        assert_eq!(filter, "foo && !login");
        assert!(type_is_negated_in_filter(&filter, "login"));
    }

    #[test]
    fn applying_u_strips_positive_across_or_groups() {
        let filter = toggle_negated_type_in_filter("foo || login", "login");
        assert_eq!(filter, "foo && !login");
        assert!(type_is_negated_in_filter(&filter, "login"));
    }

    #[test]
    fn type_field_overrides_structural_hash() {
        let a = prepare_event(json!({"_type": "login", "user": "alice"}));
        let b = prepare_event(json!({"_type": "login", "user": "bob", "extra": 1}));
        let c = prepare_event(json!({"_type": "logout", "user": "alice"}));
        assert_eq!(a.type_id, "t:login");
        assert_eq!(b.type_id, "t:login");
        assert_eq!(c.type_id, "t:logout");
        assert_ne!(a.type_id, c.type_id);
    }

    #[test]
    fn type_field_label_renders_without_type_prefix() {
        assert_eq!(default_type_label("t:login"), "login");
        assert_eq!(default_type_label("abcdef012345"), "type-abcdef01");
    }

    #[test]
    fn type_field_overridden_event_can_be_renamed() {
        let mut model = AnalyzerModel::new();
        model.ingest(json!({"_type": "login", "user": "a"}), 1.0);
        model.rename_type("t:login", "User Login".to_string());
        assert_eq!(model.canonical_type_name("t:login"), "User Login");
        assert_eq!(model.type_display_name("t:login"), "User Login (login)");
    }

    #[test]
    fn type_field_non_string_falls_back_to_shape_in_prepare() {
        // prepare_event itself is silent on bad _type; strict validation lives
        // in the ingest pipeline. Here, the override is simply ignored.
        let p = prepare_event(json!({"_type": 42, "k": 1}));
        assert!(!p.type_id.starts_with("t:"));
    }

    #[test]
    fn type_field_empty_or_whitespace_falls_back_to_shape() {
        let a = prepare_event(json!({"_type": "", "k": 1}));
        let b = prepare_event(json!({"_type": "   ", "k": 1}));
        assert!(!a.type_id.starts_with("t:"));
        assert!(!b.type_id.starts_with("t:"));
    }

    fn type_id_of(model: &AnalyzerModel, predicate: impl Fn(&Value) -> bool) -> String {
        model
            .events
            .iter()
            .find(|e| predicate(&e.obj))
            .map(|e| e.type_id.clone())
            .expect("matching event")
    }

    fn structural_type_id(v: &Value) -> String {
        structural_hash(&extract_shape(v))
    }

    #[test]
    fn merge_two_types_sums_baseline_counts() {
        let mut model = AnalyzerModel::new();
        let login_id =
            structural_type_id(&json!({"event": "login", "user": "u0"}));
        let purchase_id = structural_type_id(&json!({"event": "purchase", "amount": 0}));
        // Baseline events of two distinct shapes.
        for i in 0..5 {
            model.ingest_baseline(json!({"event": "login", "user": format!("u{i}")}), i as f64);
        }
        for i in 0..3 {
            model.ingest_baseline(
                json!({"event": "purchase", "amount": i}),
                (10 + i) as f64,
            );
        }
        assert_ne!(login_id, purchase_id);

        let group_id = model
            .merge_types(&[login_id.clone(), purchase_id.clone()], "Auth".to_string())
            .expect("merged");
        assert_eq!(*model.baseline_counts.get(&group_id).unwrap(), 5 + 3);
        assert!(!model.baseline_counts.contains_key(&login_id));
        assert!(!model.baseline_counts.contains_key(&purchase_id));
    }

    #[test]
    fn merge_two_types_unions_path_stats() {
        let mut model = AnalyzerModel::new();
        // Distinct shapes (different key sets) so structural hashes differ.
        for _ in 0..7 {
            model.ingest(json!({"event": "login", "user": "a"}), 1.0);
        }
        for _ in 0..7 {
            model.ingest(json!({"event": "logout", "session": "z"}), 2.0);
        }
        let login_id = type_id_of(&model, |v| v.get("user").is_some());
        let logout_id = type_id_of(&model, |v| v.get("session").is_some());
        assert_ne!(login_id, logout_id);
        let group_id = model
            .merge_types(&[login_id, logout_id], "AuthEvent".to_string())
            .expect("merged");
        let tp = model.types.get(&group_id).expect("group exists");
        let stats = tp.path_stats.get("event").expect("event path");
        assert_eq!(stats.total, 14);
        assert_eq!(stats.values.get("s:login").copied(), Some(7));
        assert_eq!(stats.values.get("s:logout").copied(), Some(7));
    }

    #[test]
    fn merge_then_unmerge_restores_state() {
        // Build a control model that never merges.
        let mut control = AnalyzerModel::new();
        for _ in 0..3 {
            control.ingest(json!({"event": "login", "user": "a"}), 1.0);
        }
        for _ in 0..2 {
            control.ingest(json!({"event": "logout", "session": "z"}), 2.0);
        }

        let mut subj = AnalyzerModel::new();
        for _ in 0..3 {
            subj.ingest(json!({"event": "login", "user": "a"}), 1.0);
        }
        for _ in 0..2 {
            subj.ingest(json!({"event": "logout", "session": "z"}), 2.0);
        }
        let login_id = type_id_of(&subj, |v| v.get("user").is_some());
        let logout_id = type_id_of(&subj, |v| v.get("session").is_some());
        let group_id = subj
            .merge_types(&[login_id.clone(), logout_id.clone()], "Auth".to_string())
            .expect("merge");
        subj.unmerge_group(&group_id).expect("unmerge");

        // Both members should be present, with the same counts.
        assert!(subj.types.contains_key(&login_id));
        assert!(subj.types.contains_key(&logout_id));
        assert_eq!(
            subj.types.get(&login_id).unwrap().count,
            control.types.get(&login_id).unwrap().count
        );
        assert_eq!(
            subj.types.get(&logout_id).unwrap().count,
            control.types.get(&logout_id).unwrap().count
        );
        assert!(!subj.merge_groups.contains_key(&group_id));
        assert!(subj.type_aliases.is_empty());
    }

    #[test]
    fn merge_with_conflicting_path_overrides_prefers_forced_off() {
        let mut model = AnalyzerModel::new();
        model.ingest(json!({"k": "a"}), 1.0);
        model.ingest(json!({"k": "b", "y": 1}), 2.0);
        let a_id = type_id_of(&model, |v| v.get("y").is_none());
        let b_id = type_id_of(&model, |v| v.get("y").is_some());
        // a forces 'k' OFF; b forces 'k' ON.
        if let Some(tp) = model.types.get_mut(&a_id) {
            tp.path_overrides.insert("k".to_string(), PathOverride::ForcedOff);
        }
        if let Some(tp) = model.types.get_mut(&b_id) {
            tp.path_overrides.insert("k".to_string(), PathOverride::ForcedOn);
        }
        let group_id = model
            .merge_types(&[a_id, b_id], "Mixed".to_string())
            .expect("merge");
        let merged = model.types.get(&group_id).expect("merged");
        assert_eq!(
            merged.path_overrides.get("k").copied(),
            Some(PathOverride::ForcedOff)
        );
    }

    #[test]
    fn merge_group_id_is_deterministic() {
        let mut m1 = AnalyzerModel::new();
        m1.ingest(json!({"event": "x", "user": "a"}), 1.0);
        m1.ingest(json!({"event": "y", "session": "b"}), 2.0);
        let ids1: Vec<String> = m1.types.keys().cloned().collect();
        assert_eq!(ids1.len(), 2);

        let mut m2 = AnalyzerModel::new();
        // Reverse ingest order, same shapes.
        m2.ingest(json!({"event": "y", "session": "b"}), 1.0);
        m2.ingest(json!({"event": "x", "user": "a"}), 2.0);

        let g1 = m1.merge_types(&ids1, "G".to_string()).expect("merge");
        let g2 = m2.merge_types(&ids1, "G".to_string()).expect("merge");
        assert_eq!(g1, g2);
    }

    #[test]
    fn merge_filter_rewrite_replaces_member_names_with_label() {
        let original = "TypeA && something";
        let renamed = rename_type_terms_in_filter(original, "TypeA", "Merged");
        assert_eq!(renamed, "Merged && something");
    }

    #[test]
    fn expand_merged_label_in_filter_negative_stacks() {
        let out = expand_merged_label_in_filter(
            "!Merged",
            "Merged",
            &["A".to_string(), "B".to_string()],
        );
        assert_eq!(out, "!A && !B");
    }

    #[test]
    fn expand_merged_label_in_filter_positive_fans_out() {
        let out = expand_merged_label_in_filter(
            "Merged && other",
            "Merged",
            &["A".to_string(), "B".to_string()],
        );
        assert_eq!(out, "A && other || B && other");
    }

    #[test]
    fn negative_type_filter_via_u_works_on_group_label() {
        let mut model = AnalyzerModel::new();
        model.ingest(json!({"event": "x", "user": "a"}), 1.0);
        model.ingest(json!({"event": "y", "session": "b"}), 2.0);
        let ids: Vec<String> = model.types.keys().cloned().collect();
        let _ = model
            .merge_types(&ids, "Merged".to_string())
            .expect("merge");
        let label = model.canonical_type_name(
            &model
                .merge_groups
                .keys()
                .next()
                .expect("group exists")
                .clone(),
        );
        assert_eq!(label, "Merged");
        let filter = toggle_negated_type_in_filter("", &label);
        assert!(type_is_negated_in_filter(&filter, &label));
    }
}
