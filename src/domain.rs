use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, VecDeque};

const MAX_EVENTS: usize = 80_000;
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
    pub action_period_id: Option<u64>,
    pub in_action_period: bool,
    pub live_rate_score: f64,
    pub live_uniq_score: f64,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
}

#[derive(Debug, Clone, Default)]
pub struct DataFilters {
    pub key_filter: String,
    pub type_filter: String,
    pub fuzzy_filter: String,
    pub exact_filter: String,
}

impl DataFilters {
    pub fn active_count(&self) -> usize {
        let mut n = 0;
        if !self.key_filter.is_empty() {
            n += 1;
        }
        if !self.type_filter.is_empty() {
            n += 1;
        }
        if !self.fuzzy_filter.is_empty() {
            n += 1;
        }
        if !self.exact_filter.is_empty() {
            n += 1;
        }
        n
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
}

impl FilterField {
    pub fn title(self) -> &'static str {
        match self {
            Self::Key => "keys",
            Self::Type => "type",
            Self::Fuzzy => "fuzzy",
            Self::Exact => "exact key=value",
        }
    }
}

#[derive(Debug, Clone)]
pub struct AnalyzerModel {
    pub types: IndexMap<String, TypeProfile>,
    pub events: VecDeque<EventRecord>,
    pub periods: Vec<ActionPeriod>,
    pub current_label: String,

    baseline_elapsed_secs: f64,
    baseline_counts: HashMap<String, u64>,
    period_rate_states: HashMap<u64, PeriodRateState>,
    last_rate_ts: Option<f64>,
    last_event_ts: Option<f64>,
    next_period_id: u64,
}

impl AnalyzerModel {
    pub fn new() -> Self {
        Self {
            types: IndexMap::new(),
            events: VecDeque::new(),
            periods: Vec::new(),
            current_label: "action".to_string(),
            baseline_elapsed_secs: 0.0,
            baseline_counts: HashMap::new(),
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

    pub fn ingest(&mut self, obj: Value, ts: f64) {
        self.account_rate_elapsed(ts);
        self.last_event_ts = Some(ts);
        let shape = extract_shape(&obj);
        let type_id = structural_hash(&shape);
        let keys = collect_all_paths(&obj);
        let active_period_id = self.active_period().map(|p| p.id);
        let in_action_period = active_period_id.is_some();

        let uniq = {
            let entry = self
                .types
                .entry(type_id.clone())
                .or_insert_with(|| TypeProfile {
                    type_id: type_id.clone(),
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
                });

            entry.count += 1;
            if entry.count == 1 {
                entry.example = obj.clone();
            }
            update_uniqueness(entry, &obj, in_action_period)
        };
        let rate = self.update_rate_scores(&type_id, active_period_id);
        if let Some(entry) = self.types.get_mut(&type_id) {
            entry.latest_rate = rate;
            entry.latest_uniq = uniq;
        }

        self.events.push_back(EventRecord {
            ts,
            type_id,
            obj,
            keys,
            action_period_id: active_period_id,
            in_action_period,
            live_rate_score: rate,
            live_uniq_score: uniq,
        });
        while self.events.len() > MAX_EVENTS {
            self.events.pop_front();
        }
    }

    pub fn refresh_live_anomaly_scores(&mut self) {
        let mut updates = Vec::new();
        for (idx, e) in self.events.iter().enumerate() {
            let Some(period_id) = e.action_period_id else {
                continue;
            };
            let rate = self.recomputed_rate_for(&e.type_id, period_id);
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

    fn update_rate_scores(&mut self, type_id: &str, active_period_id: Option<u64>) -> f64 {
        if let Some(pid) = active_period_id {
            let pr = self.period_rate_states.entry(pid).or_default();
            *pr.counts.entry(type_id.to_string()).or_insert(0) += 1;
            if pr.first_event_ts.is_none() {
                pr.first_event_ts = self.last_event_ts;
            }
            if pr.elapsed_secs < MIN_ACTION_RATE_DURATION_SECS {
                return 0.0;
            }
            let action_count = pr.counts.get(type_id).copied().unwrap_or(0) as f64;
            let action_rate = action_count / pr.elapsed_secs;
            let baseline_count = self.baseline_counts.get(type_id).copied().unwrap_or(0) as f64;
            let baseline_rate = baseline_count / self.baseline_elapsed_secs.max(0.001);
            normalized_rate_anomaly(action_rate, baseline_rate)
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
        let elapsed = (now_ts - last_ts).max(0.0);
        if elapsed == 0.0 {
            return;
        }
        if let Some(pid) = self.active_period().map(|p| p.id) {
            self.period_rate_states.entry(pid).or_default().elapsed_secs += elapsed;
        } else {
            self.baseline_elapsed_secs += elapsed;
        }
        self.last_rate_ts = Some(now_ts);
    }

    fn recomputed_rate_for(&self, type_id: &str, period_id: u64) -> f64 {
        let Some(pr) = self.period_rate_states.get(&period_id) else {
            return 0.0;
        };
        if pr.elapsed_secs < MIN_ACTION_RATE_DURATION_SECS {
            return 0.0;
        }
        let action_count = pr.counts.get(type_id).copied().unwrap_or(0) as f64;
        let action_rate = action_count / pr.elapsed_secs;
        let baseline_count = self.baseline_counts.get(type_id).copied().unwrap_or(0) as f64;
        let baseline_rate = baseline_count / self.baseline_elapsed_secs.max(0.001);
        normalized_rate_anomaly(action_rate, baseline_rate)
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
        let mut out = Vec::new();
        let type_query = filters.type_filter.to_lowercase();
        // Parse once outside the loop instead of per-event.
        let parsed_exact: Option<(String, String)> = if !filters.exact_filter.is_empty() {
            parse_exact_filter(&filters.exact_filter).map(|(k, v)| (k.to_string(), v))
        } else {
            None
        };
        for e in self.events.iter().rev() {
            if self
                .types
                .get(e.type_id.as_str())
                .map(|tp| tp.known_unrelated)
                .unwrap_or(false)
            {
                continue;
            }
            if let Some((start, end)) = range {
                if e.ts < start || e.ts > end {
                    continue;
                }
            }
            if !filters.type_filter.is_empty() {
                let canonical = self.canonical_type_name(&e.type_id).to_lowercase();
                if !canonical.contains(&type_query) {
                    continue;
                }
            }
            if !filters.key_filter.is_empty() {
                let wanted: Vec<&str> = filters
                    .key_filter
                    .split(',')
                    .map(|s| s.trim())
                    .filter(|s| !s.is_empty())
                    .collect();
                if !wanted.iter().all(|k| e.keys.iter().any(|ek| ek == k)) {
                    continue;
                }
            }
            if !filters.fuzzy_filter.is_empty() {
                let s = serde_json::to_string(&e.obj)
                    .unwrap_or_default()
                    .to_lowercase();
                if !fuzzy_match(&s, &filters.fuzzy_filter.to_lowercase()) {
                    continue;
                }
            }
            if let Some((ref k, ref v)) = parsed_exact {
                let found = value_at_path(&e.obj, k)
                    .map(|actual| exact_value_matches(actual, v))
                    .unwrap_or(false);
                if !found {
                    continue;
                }
            }
            out.push(e);
        }
        out
    }

    pub fn closed_periods(&self) -> Vec<&ActionPeriod> {
        self.periods.iter().filter(|p| p.end.is_some()).collect()
    }
}

fn default_type_label(type_id: &str) -> String {
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

fn update_uniqueness(tp: &mut TypeProfile, obj: &Value, in_action_period: bool) -> f64 {
    let mut scalar_paths = Vec::new();
    collect_scalar_paths(obj, "", &mut scalar_paths);

    let mut max_score = 0.0;
    for (path, token) in scalar_paths {
        let stats = tp.path_stats.entry(path.clone()).or_default();
        let prev = stats.values.get(&token).copied().unwrap_or(0);
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
            *baseline_stats.values.entry(token).or_insert(0) += 1;
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

fn collect_scalar_paths(v: &Value, path: &str, out: &mut Vec<(String, String)>) {
    match v {
        Value::Object(map) => {
            for (k, child) in map {
                let p = if path.is_empty() {
                    k.clone()
                } else {
                    format!("{}.{}", path, k)
                };
                collect_scalar_paths(child, &p, out);
            }
        }
        Value::Array(arr) => {
            for child in arr.iter().take(3) {
                let p = format!("{}[]", path);
                collect_scalar_paths(child, &p, out);
            }
        }
        _ => {
            out.push((path.to_string(), value_token(v)));
        }
    }
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

fn parse_exact_filter(filter: &str) -> Option<(&str, String)> {
    let (k, v) = filter.split_once('=')?;
    Some((k.trim(), v.trim().to_string()))
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
        if part.ends_with("[]") {
            let key = &part[..part.len().saturating_sub(2)];
            cur = cur.get(key)?;
            if let Value::Array(arr) = cur {
                cur = arr.first()?;
            } else {
                return None;
            }
        } else {
            cur = cur.get(part)?;
        }
    }
    Some(cur)
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
    collect_paths(v, "", &mut out);
    out.sort();
    out.dedup();
    out
}

fn collect_paths(v: &Value, path: &str, out: &mut Vec<String>) {
    match v {
        Value::Object(map) => {
            for (k, child) in map {
                let p = if path.is_empty() {
                    k.clone()
                } else {
                    format!("{}.{}", path, k)
                };
                out.push(p.clone());
                collect_paths(child, &p, out);
            }
        }
        Value::Array(arr) => {
            let p = format!("{}[]", path);
            out.push(p.clone());
            for child in arr.iter().take(3) {
                collect_paths(child, &p, out);
            }
        }
        _ => {}
    }
}

fn normalized_rate_anomaly(action_rate: f64, baseline_rate: f64) -> f64 {
    if action_rate <= 0.0 {
        return 0.0;
    }
    if baseline_rate <= 0.0 {
        return 1.0;
    }
    ((action_rate - baseline_rate) / action_rate).clamp(0.0, 1.0)
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
        assert_eq!(value_at_path(&v, "meta.ok"), Some(&json!(true)));
        assert_eq!(value_at_path(&v, "items[].missing"), None);
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
        let type_id = model.events.front().expect("event exists").type_id.clone();

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

        let login_type = model.events.back().expect("latest exists").type_id.clone();
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
    fn known_unrelated_types_are_suppressed_from_filtered_events() {
        let mut model = AnalyzerModel::new();
        model.ingest(json!({"event":"noise","k":1}), 1.0);
        model.ingest(json!({"event":"signal","k":2,"ctx":{"source":"web"}}), 2.0);

        let signal_type = model.events.back().expect("event exists").type_id.clone();
        model.toggle_known_unrelated_type(&signal_type);

        let filters = DataFilters::default();
        let events = model.filtered_events(&filters);
        assert_eq!(events.len(), 1);
        assert_eq!(
            value_at_path(&events[0].obj, "event"),
            Some(&json!("noise"))
        );
    }
}
