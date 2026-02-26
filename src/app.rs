use crate::domain::{
    prepare_event, value_at_path, value_token, ActionPeriod, AnalyzerModel, DataFilters,
    EventRecord, FilterField, PreparedEvent,
};
use crate::io::StreamReader;
use crate::persistence::{
    export_session, invalidate_state, load_state, save_profile, save_state, RestoredState,
    SessionEvent, SessionExport, SourceProfile, NormalizedFieldOverride,
};
use crate::tui::{draw_ui, InputMode, UiMode};
use anyhow::{anyhow, bail, Result};
use crossterm::event::{
    self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers, KeyboardEnhancementFlags,
    PopKeyboardEnhancementFlags, PushKeyboardEnhancementFlags,
};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, supports_keyboard_enhancement, EnterAlternateScreen,
    LeaveAlternateScreen,
};
use ratatui::prelude::CrosstermBackend;
use ratatui::Terminal;
use rayon::prelude::*;
use sha2::{Digest, Sha256};
use serde_json::Value;
use std::env;
use std::fs;
use std::io::stdout;
use std::path::PathBuf;
use std::collections::{HashSet, HashSet as StdHashSet};
use std::thread;
use std::time::{Duration, Instant};
use std::time::{SystemTime, UNIX_EPOCH};

const LIVE_WINDOW_DEFAULT: usize = 120;
const LIVE_FALLBACK_POLL_INTERVAL: Duration = Duration::from_millis(10);
const UI_FRAME_SLEEP: Duration = Duration::from_millis(16);
const UI_BURST_SLEEP: Duration = Duration::from_millis(1);
const MENU_PAGE_STEP: usize = 30;
const QUIT_CONFIRM_WINDOW: Duration = Duration::from_secs(2);

pub struct ObjectInspector {
    pub event: EventRecord,
    pub key_paths: Vec<String>,
    pub key_index: usize,
}

pub struct LiveRenderData<'a> {
    pub rows: Vec<&'a EventRecord>,
    pub row_indices: Vec<usize>,
    pub selected_visible: Option<usize>,
    pub selected: Option<&'a EventRecord>,
    pub total: usize,
}

pub struct ModalConfirmation {
    pub title: String,
    pub lines: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeriodsFocus {
    Periods,
    Events,
    Json,
}

#[derive(Clone)]
struct LiveAnchor {
    ts: f64,
    type_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NavIntent {
    LineUp,
    LineDown,
    PageUp,
    PageDown,
    Home,
    End,
    Left,
    Right,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WhitelistMode {
    AlwaysShow,
    OnlyWhitelist,
    Off,
}

pub struct App {
    pub model: AnalyzerModel,
    pub mode: UiMode,
    pub input_mode: InputMode,
    pub input_buffer: String,
    pub event_filters: DataFilters,
    pub types_filter: String,
    pub type_index: usize,
    pub path_index: usize,
    pub types_path_focus: bool,
    pub data_index: usize,
    pub periods_index: usize,
    pub period_event_index: usize,
    pub period_json_key_index: usize,
    pub period_value_focus: bool,
    pub periods_focus: PeriodsFocus,
    pub live_event_index: usize, // absolute index in full live rows
    pub live_view_start: usize,
    pub live_window_rows: usize,
    pub live_follow: bool,
    pub live_edge_until_center: bool,
    pub live_key_focus: bool,
    pub live_value_focus: bool,
    pub live_resume_follow_on_key_exit: bool,
    pub return_to_live_object_on_types_esc: bool,
    pub return_to_types_on_live_esc: bool,
    pub live_key_index: usize,
    pub show_help_overlay: bool,

    pub offline: bool,
    pub status: String,
    pub inspector: Option<ObjectInspector>,
    stashed_event_filters: Option<DataFilters>,
    reader: StreamReader,
    baseline_reader: Option<StreamReader>,
    baseline_events: Vec<EventRecord>,
    baseline_loaded: bool,
    offline_loaded: bool,
    offline_fallback_ts: f64,
    pending_restore: Option<RestoredState>,
    startup_hint: Option<String>,
    live_visible_indices: Vec<usize>,
    baseline_visible_indices: Vec<usize>,
    live_cache_dirty: bool,
    baseline_cache_dirty: bool,
    initial_load_target_bytes: Option<u64>,
    initial_load_complete: bool,
    initial_load_is_directory: bool,
    initial_load_polled_once: bool,
    pending_live_recompute: bool,
    show_status_debug: bool,
    quit_pending_until: Option<Instant>,
    pending_delete_period_id: Option<u64>,
    pending_profile_override: Option<SourceProfile>,
    baseline_tab_enabled: bool,
    export_path: Option<PathBuf>,
    whitelist_terms: Vec<String>,
    whitelist_mode: WhitelistMode,
    profile_renames: Vec<(String, String)>,
    profile_known_unrelated_types: Vec<String>,
    profile_normalized_field_overrides: Vec<NormalizedFieldOverride>,
    user_renamed_types: HashSet<String>,
    user_toggled_unrelated_types: HashSet<String>,
    user_toggled_paths: HashSet<String>,
    type_preview_open: bool,
}

impl App {
    pub fn new(
        stream_path: PathBuf,
        baseline_path: Option<PathBuf>,
        offline: bool,
        show_status_debug: bool,
        reset_state: bool,
    ) -> Self {
        let baseline_enabled = baseline_path.is_some();
        let is_directory_input = stream_path.is_dir();
        let initial_load_target_bytes = if is_directory_input {
            None
        } else {
            fs::metadata(&stream_path)
                .ok()
                .map(|m| m.len())
                .filter(|len| *len > 0)
        };
        let initial_load_complete = if is_directory_input {
            false
        } else {
            initial_load_target_bytes.is_none()
        };
        let mut app = Self {
            model: AnalyzerModel::new(),
            mode: UiMode::Live,
            input_mode: InputMode::None,
            input_buffer: String::new(),
            event_filters: DataFilters::default(),
            types_filter: String::new(),
            type_index: 0,
            path_index: 0,
            types_path_focus: false,
            data_index: 0,
            periods_index: 0,
            period_event_index: 0,
            period_json_key_index: 0,
            period_value_focus: false,
            periods_focus: PeriodsFocus::Periods,
            live_event_index: 0,
            live_view_start: 0,
            live_window_rows: LIVE_WINDOW_DEFAULT,
            live_follow: true,
            live_edge_until_center: false,
            live_key_focus: false,
            live_value_focus: false,
            live_resume_follow_on_key_exit: false,
            return_to_live_object_on_types_esc: false,
            return_to_types_on_live_esc: false,
            live_key_index: 0,
            show_help_overlay: false,

            offline,
            status: if offline {
                format!(
                    "Offline mode: analyzing {} (no live tail)",
                    stream_path.display()
                )
            } else {
                format!("Watching {}", stream_path.display())
            },
            inspector: None,
            stashed_event_filters: None,
            reader: StreamReader::new(stream_path),
            baseline_reader: baseline_path.map(StreamReader::new),
            baseline_events: Vec::new(),
            baseline_loaded: false,
            offline_loaded: false,
            offline_fallback_ts: unix_ts(),
            pending_restore: None,
            startup_hint: None,
            live_visible_indices: Vec::new(),
            baseline_visible_indices: Vec::new(),
            live_cache_dirty: true,
            baseline_cache_dirty: true,
            initial_load_target_bytes,
            initial_load_complete,
            initial_load_is_directory: is_directory_input,
            initial_load_polled_once: false,
            pending_live_recompute: false,
            show_status_debug,
            quit_pending_until: None,
            pending_delete_period_id: None,
            pending_profile_override: None,
            baseline_tab_enabled: baseline_enabled,
            export_path: None,
            whitelist_terms: Vec::new(),
            whitelist_mode: WhitelistMode::Off,
            profile_renames: Vec::new(),
            profile_known_unrelated_types: Vec::new(),
            profile_normalized_field_overrides: Vec::new(),
            user_renamed_types: HashSet::new(),
            user_toggled_unrelated_types: HashSet::new(),
            user_toggled_paths: HashSet::new(),
            type_preview_open: false,
        };
        if !reset_state {
            app.restore_persisted_state();
        }
        app.update_loading_status();
        if app.loading_locked() {
            app.startup_hint = None;
        }
        app
    }

    pub fn run(&mut self) -> Result<()> {
        enable_raw_mode()?;
        let mut out = stdout();
        let keyboard_enhanced = supports_keyboard_enhancement().unwrap_or(false);
        if keyboard_enhanced {
            execute!(
                out,
                EnterAlternateScreen,
                PushKeyboardEnhancementFlags(
                    KeyboardEnhancementFlags::DISAMBIGUATE_ESCAPE_CODES
                        | KeyboardEnhancementFlags::REPORT_EVENT_TYPES
                )
            )?;
        } else {
            execute!(out, EnterAlternateScreen)?;
        }

        let backend = CrosstermBackend::new(out);
        let mut terminal = Terminal::new(backend)?;
        terminal.draw(|f| draw_ui(f, self))?;

        let mut last_poll = Instant::now() - LIVE_FALLBACK_POLL_INTERVAL;

        let loop_result = (|| -> Result<()> {
            loop {
                let loop_started_at = Instant::now();
                let was_loading_locked = self.loading_locked();

                if !self.baseline_loaded {
                    self.ingest_baseline_corpus()?;
                }

                let mut ingested_any = false;
                if !self.offline || !self.offline_loaded {
                    let mut should_poll = self.offline && !self.offline_loaded;
                    if !self.offline {
                        if last_poll.elapsed() >= LIVE_FALLBACK_POLL_INTERVAL {
                            should_poll = true;
                        }
                    }
                    if should_poll {
                        ingested_any = self.ingest_new_events()?;
                        last_poll = Instant::now();
                    }
                }

                self.update_loading_status();

                let just_finished_loading = was_loading_locked && !self.loading_locked();
                if just_finished_loading {
                    self.pending_live_recompute = true;
                }
                let should_refresh_scores = if self.loading_locked() {
                    false
                } else {
                    self.pending_live_recompute
                };
                if should_refresh_scores {
                    self.model.refresh_live_anomaly_scores();
                    self.pending_live_recompute = false;
                    if ingested_any {
                        self.mark_live_cache_dirty();
                    }
                }

                let mut should_quit = false;
                while event::poll(Duration::from_millis(0))? {
                    if let Event::Key(key) = event::read()? {
                        if !matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) {
                            continue;
                        }
                        if self.handle_key(key) {
                            should_quit = true;
                            break;
                        }
                    }
                }
                if should_quit {
                    break;
                }

                self.rebuild_live_cache_if_needed();
                terminal.draw(|f| draw_ui(f, self))?;

                let target_sleep = if ingested_any {
                    UI_BURST_SLEEP
                } else {
                    UI_FRAME_SLEEP
                };
                let elapsed = loop_started_at.elapsed();
                if elapsed < target_sleep {
                    thread::sleep(target_sleep - elapsed);
                }
            }
            Ok(())
        })();

        disable_raw_mode()?;
        if keyboard_enhanced {
            execute!(
                terminal.backend_mut(),
                PopKeyboardEnhancementFlags,
                LeaveAlternateScreen
            )?;
        } else {
            execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
        }
        terminal.show_cursor()?;
        self.model.close_open_period(unix_ts());
        if let Err(err) = self.persist_state() {
            eprintln!("warning: failed to persist session state: {err}");
        }
        if let Err(err) = self.export_session_if_configured() {
            eprintln!("warning: failed to export session: {err}");
        }
        loop_result
    }

    pub fn apply_profile(&mut self, profile: SourceProfile, prompt_on_conflict: bool) {
        let same_profile = self.profile_matches_current_state(&profile);
        if same_profile {
            self.status = "Profile matches restored session (no changes)".to_string();
            return;
        }
        if prompt_on_conflict && self.has_nonempty_profile_state() {
            self.pending_profile_override = Some(profile);
            self.status = "Apply profile over restored session state? (y/N, whitelist merges additively)".to_string();
            return;
        }
        self.apply_profile_seeded(profile);
        self.status = "Loaded source profile".to_string();
    }

    fn apply_profile_seeded(&mut self, profile: SourceProfile) {
        self.profile_renames = profile.renames.clone();
        self.profile_known_unrelated_types = profile.known_unrelated_types.clone();
        self.profile_normalized_field_overrides = profile.normalized_field_overrides.clone();
        self.apply_profile_overrides_to_types();
        self.add_whitelist_terms(profile.whitelist_terms);
        self.apply_profile_filters(profile.negative_filters);
    }

    fn apply_profile_forced(&mut self, profile: SourceProfile) {
        let SourceProfile {
            renames,
            known_unrelated_types,
            normalized_field_overrides,
            negative_filters,
            whitelist_terms,
        } = profile;
        self.profile_renames = renames.clone();
        self.profile_known_unrelated_types = known_unrelated_types.clone();
        self.profile_normalized_field_overrides = normalized_field_overrides.clone();
        self.user_renamed_types.clear();
        self.user_toggled_unrelated_types.clear();
        self.user_toggled_paths.clear();

        if !renames.is_empty() {
            self.model.apply_renames(&renames);
        }
        let unrelated_set: HashSet<String> = known_unrelated_types.into_iter().collect();
        for (type_id, tp) in self.model.types.iter_mut() {
            tp.known_unrelated = unrelated_set.contains(type_id);
        }
        self.model.apply_normalized_field_overrides(
            &normalized_field_overrides
                .iter()
                .map(|r| (r.type_id.clone(), r.path.clone(), r.mode))
                .collect::<Vec<_>>(),
        );

        self.add_whitelist_terms(whitelist_terms);
        self.stashed_event_filters = None;
        self.event_filters = negative_filters;
        self.mark_live_cache_dirty();
        self.refresh_live_position();
    }

    pub fn set_whitelist_terms(&mut self, terms: Vec<String>) {
        self.whitelist_terms.clear();
        self.add_whitelist_terms(terms);
    }

    pub fn add_whitelist_terms(&mut self, terms: Vec<String>) {
        for t in terms
            .into_iter()
            .map(|s| s.trim().to_lowercase())
            .filter(|s| !s.is_empty())
        {
            if !self.whitelist_terms.iter().any(|existing| existing == &t) {
                self.whitelist_terms.push(t);
            }
        }
        self.whitelist_mode = if self.whitelist_terms.is_empty() {
            WhitelistMode::Off
        } else {
            WhitelistMode::AlwaysShow
        };
        self.mark_live_cache_dirty();
        self.refresh_live_position();
    }

    pub fn import_session(
        &mut self,
        session: SessionExport,
        profile_override: Option<SourceProfile>,
    ) -> Result<()> {
        let SessionExport {
            stream_path: _,
            periods,
            renames,
            known_unrelated_types,
            normalized_field_overrides,
            current_label,
            event_filters,
            stashed_event_filters,
            types_filter,
            profile: session_profile,
            events,
            baseline_events,
        } = session;
        self.offline = true;
        self.offline_loaded = true;
        self.initial_load_complete = true;
        self.baseline_loaded = true;
        self.pending_restore = None;

        self.model = AnalyzerModel::new();
        self.baseline_events.clear();
        for ev in &baseline_events {
            let prepared = prepare_event(ev.obj.clone());
            let obj_size = prepared.obj.to_string().len() as u32;
            self.model.ingest_baseline_prepared(&prepared, ev.ts);
            self.baseline_events.push(EventRecord {
                ts: ev.ts,
                type_id: prepared.type_id,
                obj: prepared.obj,
                keys: prepared.keys,
                size_bytes: obj_size,
                action_period_id: None,
                in_action_period: false,
                live_rate_score: 0.0,
                live_uniq_score: 0.0,
            });
        }
        for ev in &events {
            self.model.ingest(ev.obj.clone(), ev.ts);
        }

        self.model.set_periods(periods);
        if !renames.is_empty() {
            self.model.apply_renames(&renames);
        }
        if !normalized_field_overrides.is_empty() {
            self.model.apply_normalized_field_overrides(
                &normalized_field_overrides
                    .iter()
                    .map(|r| (r.type_id.clone(), r.path.clone(), r.mode))
                    .collect::<Vec<_>>(),
            );
        }
        for type_id in known_unrelated_types {
            if let Some(tp) = self.model.types.get_mut(&type_id) {
                tp.known_unrelated = true;
            }
        }
        self.apply_profile_overrides_to_types();
        self.model.current_label = current_label;
        self.event_filters = event_filters;
        self.stashed_event_filters = stashed_event_filters;
        self.types_filter = types_filter;
        match (profile_override, session_profile) {
            (Some(override_profile), Some(bundled_profile)) => {
                if profile_fingerprint(&override_profile) == profile_fingerprint(&bundled_profile) {
                    self.apply_profile(override_profile, false);
                } else {
                    self.apply_profile(override_profile, true);
                }
            }
            (Some(override_profile), None) => self.apply_profile(override_profile, true),
            (None, Some(bundled_profile)) => self.apply_profile(bundled_profile, false),
            (None, None) => {}
        }
        self.baseline_tab_enabled = !self.baseline_events.is_empty();
        self.pending_live_recompute = true;
        self.mark_live_cache_dirty();
        self.refresh_live_position();
        self.status = format!(
            "Imported session: {} events, {} baseline",
            self.model.total_objects(),
            self.baseline_events.len()
        );
        Ok(())
    }

    fn ingest_new_events(&mut self) -> Result<bool> {
        self.initial_load_polled_once = true;
        self.rebuild_live_cache_if_needed();
        let use_snapshot_parallel = self.offline || self.loading_locked();
        let events = if use_snapshot_parallel {
            self.reader.poll_snapshot_parallel()
        } else {
            self.reader.poll()
        }?;

        let selected_anchor = if self.mode == UiMode::Live && !self.live_follow {
            self.live_anchor_at(self.live_event_index)
        } else {
            None
        };
        let view_anchor = if self.mode == UiMode::Live && !self.live_follow {
            self.live_anchor_at(self.live_view_start)
        } else {
            None
        };

        let n = events.len();
        let batch_now = unix_ts();
        let prepared_events: Vec<PreparedEvent> =
            events.into_par_iter().map(prepare_event).collect();
        for (idx, prepared) in prepared_events.into_iter().enumerate() {
            let ts = self.resolve_event_ts(&prepared.obj, batch_now, idx)?;
            if let Some(last) = self.model.events.back() {
                if ts < last.ts - 1e-9 {
                    bail!(
                        "Events must be sorted by timestamp. Found event at {ts} after event at {}. Sort your JSONL file with: jq -cs 'sort_by(._timestamp)[]' input.jsonl > sorted.jsonl",
                        last.ts
                    );
                }
            }
            self.model.ingest_prepared(prepared, ts);
        }
        if n > 0 {
            self.mark_live_cache_dirty();
            self.pending_live_recompute = true;
        }
        self.apply_persisted_overrides_if_ready();

        if n > 0 {
            if self.pending_profile_override.is_none() {
                if self.offline && !self.offline_loaded {
                    self.status = self.offline_load_status();
                } else {
                    self.status = format!("Ingested {} events", n);
                }
            }
            if self.mode == UiMode::Live && self.live_follow {
                self.live_edge_until_center = false;
                self.pin_live_to_latest();
            } else if self.mode == UiMode::Live {
                if let Some(anchor) = selected_anchor.as_ref() {
                    if let Some(idx) = self.find_live_index(anchor) {
                        self.live_event_index = idx;
                    }
                }
                if let Some(anchor) = view_anchor.as_ref() {
                    if let Some(idx) = self.find_live_index(anchor) {
                        self.live_view_start = idx;
                    }
                }
                self.clamp_live_indices();
                self.ensure_live_selection_visible();
            }
        } else if self.offline && !self.offline_loaded {
            let progress = self.reader.progress();
            if self.pending_profile_override.is_none() {
                if progress.total_bytes > 0 && progress.loaded_bytes >= progress.total_bytes {
                    self.status = format!(
                        "Offline load complete: {} objects",
                        self.model.total_objects()
                    );
                } else if self.model.total_objects() == 0 {
                    self.status = "Offline mode: no events found".to_string();
                } else {
                    self.status = self.offline_load_status();
                }
            }
        }

        if self.offline {
            let progress = self.reader.progress();
            self.offline_loaded =
                progress.total_bytes == 0 || progress.loaded_bytes >= progress.total_bytes;
        }
        if let Some(prompt) = self.delete_confirmation_status() {
            self.status = prompt;
        }
        Ok(n > 0)
    }

    fn ingest_baseline_corpus(&mut self) -> Result<()> {
        let Some(reader) = self.baseline_reader.as_mut() else {
            self.baseline_loaded = true;
            return Ok(());
        };

        let events = reader.poll_snapshot_parallel()?;
        let seed_ts = unix_ts();
        let prepared_events: Vec<PreparedEvent> =
            events.into_par_iter().map(prepare_event).collect();
        for (idx, prepared) in prepared_events.into_iter().enumerate() {
            let ts = match parse_event_timestamp_millis(&prepared.obj)? {
                Some(ts) => ts,
                None => seed_ts + (idx as f64 * 0.001),
            };
            self.model.ingest_baseline_prepared(&prepared, ts);
            let PreparedEvent {
                obj,
                type_id,
                keys,
                scalar_paths: _,
            } = prepared;
            let size_bytes = obj.to_string().len() as u32;
            self.baseline_events.push(EventRecord {
                ts,
                type_id,
                obj,
                keys,
                size_bytes,
                action_period_id: None,
                in_action_period: false,
                live_rate_score: 0.0,
                live_uniq_score: 0.0,
            });
        }
        self.baseline_tab_enabled = !self.baseline_events.is_empty();
        let progress = reader.progress();
        let baseline_path_display = reader.path().display().to_string();
        self.baseline_loaded = progress.total_bytes == 0 || progress.loaded_bytes >= progress.total_bytes;
        self.pending_live_recompute = true;
        self.mark_live_cache_dirty();
        if self.baseline_loaded {
            self.status = format!(
                "Baseline loaded: {} events from {}",
                self.baseline_events.len(),
                baseline_path_display
            );
        } else {
            self.status = self.baseline_load_status();
        }
        Ok(())
    }

    fn resolve_event_ts(&mut self, obj: &Value, batch_now: f64, idx: usize) -> Result<f64> {
        match parse_event_timestamp_millis(obj)? {
            Some(ts) => return Ok(ts),
            None => {}
        }
        if self.offline {
            let seed = batch_now + (idx as f64 * 0.001);
            let next = (self.offline_fallback_ts + 0.001).max(seed);
            self.offline_fallback_ts = next;
            return Ok(next);
        }
        Err(anyhow!(
            "Unsupported input: live mode requires root `_timestamp` as epoch milliseconds (e.g. 1739952000123). Missing `_timestamp` is unsupported. Use `--offline` for offline analysis without live timestamps."
        ))
    }

    fn restore_persisted_state(&mut self) {
        match load_state(self.reader.path()) {
            Ok(Some(saved)) => {
                let msg = format!(
                    "Restored session: {} periods, {} renames, {} unrelated, {} normalized fields, filters {}/5{}{}",
                    saved.periods.len(),
                    saved.renames.len(),
                    saved.known_unrelated_types.len(),
                    saved.normalized_field_overrides.len(),
                    saved.event_filters.active_count(),
                    if saved.stashed_event_filters.is_some() {
                        " (suspended set saved)"
                    } else {
                        ""
                    },
                    if saved.types_filter.is_empty() {
                        ""
                    } else {
                        ", type list filter set"
                    },
                );
                if !saved.current_label.trim().is_empty() {
                    self.model.current_label = saved.current_label.clone();
                }
                self.event_filters = saved.event_filters.clone();
                self.stashed_event_filters = saved.stashed_event_filters.clone();
                self.types_filter = saved.types_filter.clone();
                self.mark_live_cache_dirty();
                self.refresh_live_position();
                if !saved.periods.is_empty() {
                    self.model.set_periods(saved.periods.clone());
                    self.pending_live_recompute = true;
                }
                self.pending_restore = Some(saved);
                self.status = msg.clone();
                self.startup_hint = Some(msg);
            }
            Ok(None) => {}
            Err(err) => {
                self.status = format!("State restore skipped: {err}");
                self.startup_hint = Some(self.status.clone());
            }
        }
    }

    fn apply_persisted_overrides_if_ready(&mut self) {
        if self.pending_restore.is_none() || self.model.total_objects() == 0 {
            self.apply_profile_overrides_to_types();
            return;
        }
        let saved = self.pending_restore.take().unwrap();
        if !saved.renames.is_empty() {
            self.model.apply_renames(&saved.renames);
        }
        for type_id in &saved.known_unrelated_types {
            if let Some(tp) = self.model.types.get_mut(type_id) {
                tp.known_unrelated = true;
            }
        }
        if !saved.normalized_field_overrides.is_empty() {
            self.model.apply_normalized_field_overrides(
                &saved
                    .normalized_field_overrides
                    .iter()
                    .map(|r| (r.type_id.clone(), r.path.clone(), r.mode))
                    .collect::<Vec<_>>(),
            );
        }
        self.apply_profile_overrides_to_types();
    }

    fn apply_profile_overrides_to_types(&mut self) {
        if !self.profile_renames.is_empty() {
            for (type_id, name) in &self.profile_renames {
                if self.user_renamed_types.contains(type_id) {
                    continue;
                }
                if let Some(tp) = self.model.types.get(type_id) {
                    if tp.name.is_none() {
                        self.model.rename_type(type_id, name.clone());
                    }
                }
            }
        }
        for type_id in &self.profile_known_unrelated_types {
            if self.user_toggled_unrelated_types.contains(type_id) {
                continue;
            }
            if let Some(tp) = self.model.types.get_mut(type_id) {
                if !tp.known_unrelated {
                    tp.known_unrelated = true;
                }
            }
        }
        if !self.profile_normalized_field_overrides.is_empty() {
            let mut current = self.model.normalized_field_overrides();
            for rule in &self.profile_normalized_field_overrides {
                let key = path_override_key(&rule.type_id, &rule.path);
                if self.user_toggled_paths.contains(&key) {
                    continue;
                }
                if let Some(existing) = current
                    .iter_mut()
                    .find(|(tid, path, _)| tid == &rule.type_id && path == &rule.path)
                {
                    existing.2 = rule.mode;
                } else {
                    current.push((rule.type_id.clone(), rule.path.clone(), rule.mode));
                }
            }
            self.model.apply_normalized_field_overrides(&current);
        }
    }

    fn persist_state(&self) -> Result<()> {
        if !self.reader.path().exists() {
            // The stream file was deleted while we were running. The reader resets
            // its offset to 0 on detecting a missing file, so saved_len would be 0
            // and any new file at the same path would pass the hash check. Invalidate
            // the state file instead so nothing is restored in the next session.
            return invalidate_state(self.reader.path());
        }
        save_state(
            self.reader.path(),
            self.reader.offset(),
            &self.model.periods,
            &self.model.renamed_types(),
            &self
                .model
                .types
                .iter()
                .filter_map(|(type_id, tp)| tp.known_unrelated.then_some(type_id.clone()))
                .collect::<Vec<_>>(),
            &self.current_normalized_field_overrides(),
            &self.model.current_label,
            &self.event_filters,
            self.stashed_event_filters.as_ref(),
            &self.types_filter,
        )
    }

    fn export_session_if_configured(&self) -> Result<()> {
        let Some(path) = self.export_path.as_ref() else {
            return Ok(());
        };
        let snapshot = self.build_session_export();
        export_session(path, &snapshot)
    }

    fn export_session_to_path(&mut self, path: PathBuf) {
        match export_session(&path, &self.build_session_export()) {
            Ok(_) => {
                self.status = format!("Session exported to {}", path.display());
                self.export_path = Some(path);
            }
            Err(err) => {
                self.status = format!("Session export failed: {err}");
            }
        }
    }

    fn export_profile_to_path(&mut self, path: PathBuf) {
        let profile = SourceProfile {
            renames: self.model.renamed_types(),
            known_unrelated_types: self
                .model
                .types
                .iter()
                .filter_map(|(type_id, tp)| tp.known_unrelated.then_some(type_id.clone()))
                .collect(),
            normalized_field_overrides: self.current_normalized_field_overrides(),
            negative_filters: self.event_filters.clone(),
            whitelist_terms: self.whitelist_terms.clone(),
        };
        match save_profile(&path, &profile) {
            Ok(_) => self.status = format!("Profile exported to {}", path.display()),
            Err(err) => self.status = format!("Profile export failed: {err}"),
        }
    }

    fn build_session_export(&self) -> SessionExport {
        let mut snapshot = SessionExport::new(self.reader.path().display().to_string());
        snapshot.periods = self.model.periods.clone();
        snapshot.renames = self.model.renamed_types();
        snapshot.known_unrelated_types = self
            .model
            .types
            .iter()
            .filter_map(|(type_id, tp)| tp.known_unrelated.then_some(type_id.clone()))
            .collect();
        snapshot.normalized_field_overrides = self.current_normalized_field_overrides();
        snapshot.current_label = self.model.current_label.clone();
        snapshot.event_filters = self.event_filters.clone();
        snapshot.stashed_event_filters = self.stashed_event_filters.clone();
        snapshot.types_filter = self.types_filter.clone();
        snapshot.profile = Some(SourceProfile {
            renames: self.model.renamed_types(),
            known_unrelated_types: self
                .model
                .types
                .iter()
                .filter_map(|(type_id, tp)| tp.known_unrelated.then_some(type_id.clone()))
                .collect(),
            normalized_field_overrides: self.current_normalized_field_overrides(),
            negative_filters: self.event_filters.clone(),
            whitelist_terms: self.whitelist_terms.clone(),
        });
        snapshot.events = self
            .model
            .events
            .iter()
            .map(|e| SessionEvent {
                ts: e.ts,
                obj: e.obj.clone(),
            })
            .collect();
        snapshot.baseline_events = self
            .baseline_events
            .iter()
            .map(|e| SessionEvent {
                ts: e.ts,
                obj: e.obj.clone(),
            })
            .collect();
        snapshot
    }

    fn current_normalized_field_overrides(&self) -> Vec<NormalizedFieldOverride> {
        self.model
            .normalized_field_overrides()
            .into_iter()
            .map(|(type_id, path, mode)| NormalizedFieldOverride {
                type_id,
                path,
                mode,
            })
            .collect()
    }

    fn default_session_export_path(&self) -> PathBuf {
        let mut p = self.reader.path().clone();
        let fname = p
            .file_name()
            .and_then(|n| n.to_str())
            .map(|n| format!("{n}.session.json"))
            .unwrap_or_else(|| "session-export.json".to_string());
        p.set_file_name(fname);
        p
    }

    fn default_profile_export_path(&self) -> PathBuf {
        let mut p = self.reader.path().clone();
        let fname = p
            .file_name()
            .and_then(|n| n.to_str())
            .map(|n| format!("{n}.profile.json"))
            .unwrap_or_else(|| "source-profile.json".to_string());
        p.set_file_name(fname);
        p
    }

    fn handle_key(&mut self, key: KeyEvent) -> bool {
        self.startup_hint = None;
        if key.modifiers.contains(KeyModifiers::CONTROL)
            && matches!(key.code, KeyCode::Char('c') | KeyCode::Char('C'))
        {
            return true;
        }
        if matches!(key.code, KeyCode::Char('q')) {
            let now = Instant::now();
            if self
                .quit_pending_until
                .is_some_and(|deadline| deadline >= now)
            {
                return true;
            }
            self.quit_pending_until = Some(now + QUIT_CONFIRM_WINDOW);
            self.status = "Press q again within 2s to quit".to_string();
            return false;
        }
        if self.pending_delete_period_id.is_some() {
            match key.code {
                KeyCode::Char('y') | KeyCode::Char('Y') => {
                    self.confirm_pending_period_delete();
                    return false;
                }
                KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => {
                    self.cancel_pending_period_delete();
                    return false;
                }
                _ => {
                    return false;
                }
            }
        }
        if self.pending_profile_override.is_some() {
            match key.code {
                KeyCode::Char('y') | KeyCode::Char('Y') => {
                    self.confirm_pending_profile_override();
                    return false;
                }
                KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc | KeyCode::Enter => {
                    self.cancel_pending_profile_override();
                    return false;
                }
                _ => {
                    return false;
                }
            }
        }
        if self.loading_locked() {
            self.update_loading_status();
            return false;
        }
        let code = normalize_navigation_code(key);

        if self.input_mode != InputMode::None {
            return self.handle_input(code);
        }

        if self.inspector.is_some() {
            return self.handle_inspector(code);
        }
        if self.mode == UiMode::Types && self.type_preview_open {
            match code {
                KeyCode::Esc | KeyCode::Char('j') | KeyCode::Char('J') => {
                    self.type_preview_open = false;
                    return false;
                }
                _ => {}
            }
        }

        if key.modifiers.contains(KeyModifiers::CONTROL)
            && matches!(key.code, KeyCode::Char('u') | KeyCode::Char('U'))
        {
            self.handle_navigation_intent(NavIntent::PageUp);
            return false;
        }
        if key.modifiers.contains(KeyModifiers::CONTROL)
            && matches!(key.code, KeyCode::Char('d') | KeyCode::Char('D'))
        {
            self.handle_navigation_intent(NavIntent::PageDown);
            return false;
        }

        match code {
            KeyCode::Esc if self.mode == UiMode::Live && self.live_key_focus => {
                self.exit_live_key_focus();
            }
            KeyCode::Char('h') | KeyCode::Char('?') => {
                self.show_help_overlay = !self.show_help_overlay;
            }
            KeyCode::Char('x') | KeyCode::Char('X') => {
                self.input_mode = InputMode::ExportSessionPath;
                self.input_buffer = self.default_session_export_path().display().to_string();
            }
            KeyCode::Char('p') | KeyCode::Char('P') => {
                self.input_mode = InputMode::ExportProfilePath;
                self.input_buffer = self.default_profile_export_path().display().to_string();
            }
            KeyCode::Char('1') => {
                self.set_ui_mode(UiMode::Live);
                self.clamp_live_key_selection();
            }
            KeyCode::Char('2') => self.set_ui_mode(UiMode::Periods),
            KeyCode::Char('3') => self.set_ui_mode(UiMode::Types),
            KeyCode::Char('4') => {
                if self.baseline_tab_enabled {
                    self.set_ui_mode(UiMode::Data)
                } else {
                    self.status = "Baseline view is unavailable (start with --baseline)".to_string();
                }
            }
            KeyCode::Esc if self.mode == UiMode::Live && self.return_to_types_on_live_esc => {
                self.mode = UiMode::Types;
                self.return_to_types_on_live_esc = false;
                self.event_filters.type_filter.clear();
                self.stashed_event_filters = None;
                self.mark_live_cache_dirty();
                self.refresh_live_position();
                self.live_key_focus = false;
                self.live_value_focus = false;
                self.types_path_focus = false;
                self.periods_focus = PeriodsFocus::Periods;
                self.status = "Returned to Types (type filter cleared)".to_string();
            }
            KeyCode::Esc
                if self.mode == UiMode::Types && self.return_to_live_object_on_types_esc =>
            {
                self.mode = UiMode::Live;
                self.return_to_live_object_on_types_esc = false;
                self.return_to_types_on_live_esc = false;
                self.types_path_focus = false;
                self.live_key_focus = true;
                self.live_value_focus = false;
                self.clamp_live_indices();
                self.ensure_live_selection_visible();
                self.clamp_live_key_selection();
                self.periods_focus = PeriodsFocus::Periods;
                self.status = "Returned to selected JSON".to_string();
            }
            KeyCode::Char('m') => {
                if self.offline {
                    self.status = "Cannot mark action periods in offline mode".to_string();
                } else if self.model.toggle_period() {
                    self.pending_live_recompute = true;
                    self.status = if let Some(p) = self.model.active_period() {
                        format!("Action started: {} #{}", p.label, p.id)
                    } else {
                        "Action ended".to_string()
                    };
                } else {
                    self.status =
                        "Cannot toggle action period before first event timestamp is ingested"
                            .to_string();
                }
            }

            KeyCode::Char('f') if self.mode == UiMode::Live => {
                if self.live_key_focus {
                    self.status =
                        "Exit key focus first (left or enter) to toggle follow".to_string();
                    return false;
                }
                self.live_follow = !self.live_follow;
                if self.live_follow {
                    self.live_edge_until_center = false;
                    self.pin_live_to_latest();
                } else {
                    // Keep selected row away from the top when leaving follow mode.
                    self.live_view_start = self.live_event_index.saturating_sub(10);
                    self.clamp_live_indices();
                    self.live_edge_until_center = true;
                    self.ensure_live_selection_visible();
                }
                self.status = if self.live_follow {
                    "Live follow: ON".to_string()
                } else {
                    "Live follow: OFF".to_string()
                };
            }
            KeyCode::Char('n') => {
                self.input_mode = InputMode::Label;
                self.input_buffer = self.model.current_label.clone();
            }
            KeyCode::Char('k')
                if self.mode == UiMode::Periods && self.periods_focus == PeriodsFocus::Json =>
            {
                self.apply_period_selected_key_filter();
            }
            KeyCode::Char('i')
                if self.mode == UiMode::Periods && self.periods_focus == PeriodsFocus::Periods =>
            {
                self.input_mode = InputMode::InsertPeriodRange;
                self.input_buffer.clear();
            }
            KeyCode::Char('e')
                if self.mode == UiMode::Periods && self.periods_focus == PeriodsFocus::Periods =>
            {
                self.input_mode = InputMode::EditPeriodRange;
                self.input_buffer = self.selected_period_row_range_input().unwrap_or_default();
            }
            KeyCode::Char('d')
                if self.mode == UiMode::Periods && self.periods_focus == PeriodsFocus::Periods =>
            {
                self.start_delete_selected_period_confirmation();
            }
            KeyCode::Char('k') if self.mode == UiMode::Live && self.live_key_focus => {
                self.apply_live_selected_key_filter();
            }
            KeyCode::Char('e')
                if self.mode == UiMode::Live && self.live_key_focus && self.live_value_focus =>
            {
                self.apply_live_selected_value_filter();
            }
            KeyCode::Char('e')
                if self.mode == UiMode::Periods
                    && self.periods_focus == PeriodsFocus::Json
                    && self.period_value_focus =>
            {
                self.apply_period_selected_value_filter();
            }
            KeyCode::Char('k') if self.mode != UiMode::Types => {
                self.start_event_filter_input(FilterField::Key)
            }
            KeyCode::Char('t') if self.mode == UiMode::Live && self.live_key_focus => {
                self.jump_to_live_selected_event_type()
            }
            KeyCode::Char('t')
                if self.mode == UiMode::Periods && self.periods_focus == PeriodsFocus::Json =>
            {
                self.jump_to_period_selected_event_type()
            }
            KeyCode::Char('t') if self.mode != UiMode::Types => {
                self.start_event_filter_input(FilterField::Type)
            }
            KeyCode::Char('t') if self.mode == UiMode::Types => self.apply_selected_type_filter(),
            KeyCode::Char('/') if self.mode != UiMode::Types => {
                self.start_event_filter_input(FilterField::Substring)
            }
            KeyCode::Char('z') if self.mode != UiMode::Types => {
                self.start_event_filter_input(FilterField::Fuzzy)
            }
            KeyCode::Char('e') if self.mode != UiMode::Types => {
                self.start_event_filter_input(FilterField::Exact)
            }
            KeyCode::Char('y') if self.mode != UiMode::Types => self.toggle_event_filters_enabled(),
            KeyCode::Char('w') if self.mode != UiMode::Types => self.cycle_whitelist_mode(),
            KeyCode::Char('/') if self.mode == UiMode::Types => {
                self.input_mode = InputMode::TypesFilter;
                self.input_buffer = self.types_filter.clone();
            }
            KeyCode::Char('j') | KeyCode::Char('J') if self.mode == UiMode::Types => {
                self.type_preview_open = !self.type_preview_open;
            }
            KeyCode::Char('r') if self.mode == UiMode::Types => {
                let visible = self.visible_types();
                if let Some(type_id) = visible.get(self.type_index) {
                    let tp = self.model.types.get(type_id);
                    self.input_mode = InputMode::RenameType;
                    self.input_buffer = tp.and_then(|t| t.name.clone()).unwrap_or_default();
                }
            }
            KeyCode::Char('c') if self.mode != UiMode::Types => {
                self.stashed_event_filters = None;
                self.event_filters = DataFilters::default();
                self.mark_live_cache_dirty();
                self.refresh_live_position();
                self.status = "Event filters cleared".to_string();
            }
            KeyCode::Up => self.handle_navigation_intent(NavIntent::LineUp),
            KeyCode::Down => self.handle_navigation_intent(NavIntent::LineDown),
            KeyCode::Home => self.handle_navigation_intent(NavIntent::Home),
            KeyCode::End => self.handle_navigation_intent(NavIntent::End),
            KeyCode::PageUp => self.handle_navigation_intent(NavIntent::PageUp),
            KeyCode::PageDown => self.handle_navigation_intent(NavIntent::PageDown),
            KeyCode::Left => self.handle_navigation_intent(NavIntent::Left),
            KeyCode::Right => self.handle_navigation_intent(NavIntent::Right),
            KeyCode::Enter if self.mode == UiMode::Live && self.live_key_focus => {
                if self.live_value_focus {
                    self.apply_live_selected_value_filter();
                } else {
                    self.apply_live_selected_key_filter();
                }
            }
            KeyCode::Enter
                if self.mode == UiMode::Periods && self.periods_focus == PeriodsFocus::Json =>
            {
                if self.period_value_focus {
                    self.apply_period_selected_value_filter();
                } else {
                    self.apply_period_selected_key_filter();
                }
            }
            KeyCode::Enter if self.mode == UiMode::Live => self.toggle_live_key_focus(),
            KeyCode::Enter if self.mode == UiMode::Types => self.enter_types_path_focus(),
            KeyCode::Enter if self.mode == UiMode::Periods => self.advance_periods_focus(),
            KeyCode::Enter => self.open_selected_event(),
            KeyCode::Char(' ') => self.toggle_current_path(),
            KeyCode::Char('u') => self.toggle_known_unrelated(),
            _ => {}
        }
        self.quit_pending_until = None;
        false
    }

    fn handle_inspector(&mut self, code: KeyCode) -> bool {
        match code {
            KeyCode::Esc => self.inspector = None,
            KeyCode::Up => {
                if let Some(ins) = self.inspector.as_mut() {
                    if ins.key_index > 0 {
                        ins.key_index -= 1;
                    }
                }
            }
            KeyCode::Down => {
                if let Some(ins) = self.inspector.as_mut() {
                    if ins.key_index + 1 < ins.key_paths.len() {
                        ins.key_index += 1;
                    }
                }
            }
            KeyCode::PageUp => {
                if let Some(ins) = self.inspector.as_mut() {
                    ins.key_index = ins.key_index.saturating_sub(MENU_PAGE_STEP);
                }
            }
            KeyCode::PageDown => {
                if let Some(ins) = self.inspector.as_mut() {
                    ins.key_index =
                        (ins.key_index + MENU_PAGE_STEP).min(ins.key_paths.len().saturating_sub(1));
                }
            }
            KeyCode::Home => {
                if let Some(ins) = self.inspector.as_mut() {
                    ins.key_index = 0;
                }
            }
            KeyCode::End => {
                if let Some(ins) = self.inspector.as_mut() {
                    ins.key_index = ins.key_paths.len().saturating_sub(1);
                }
            }
            KeyCode::Char('k') => {
                if let Some(path) = self
                    .inspector
                    .as_ref()
                    .and_then(|ins| ins.key_paths.get(ins.key_index))
                    .cloned()
                {
                    self.apply_key_filter_in_place(&path);
                    self.inspector = None;
                }
            }
            KeyCode::Char('t') => {
                if let Some(ins) = self.inspector.as_ref() {
                    if let Some(idx) = self.model.find_type_index(&ins.event.type_id) {
                        self.mode = UiMode::Types;
                        self.return_to_types_on_live_esc = false;
                        self.type_index = idx;
                        self.path_index = 0;
                        self.types_path_focus = false;
                        self.status = format!(
                            "Jumped to type {}",
                            self.model.type_display_name(&ins.event.type_id)
                        );
                    }
                }
            }
            _ => {}
        }
        false
    }

    fn handle_input(&mut self, code: KeyCode) -> bool {
        match code {
            KeyCode::Esc => {
                self.input_mode = InputMode::None;
                self.input_buffer.clear();
            }
            KeyCode::Enter => {
                match self.input_mode {
                    InputMode::Label => {
                        if !self.input_buffer.trim().is_empty() {
                            self.model.current_label = self.input_buffer.trim().to_string();
                            self.status = format!("Current label: {}", self.model.current_label);
                        }
                    }
                    InputMode::EventFilter(field) => {
                        let text = self.input_buffer.trim().to_string();
                        self.stashed_event_filters = None;
                        match field {
                            FilterField::Key => self.event_filters.key_filter = text,
                            FilterField::Type => self.event_filters.type_filter = text,
                            FilterField::Fuzzy => self.event_filters.fuzzy_filter = text,
                            FilterField::Exact => self.event_filters.exact_filter = text,
                            FilterField::Substring => self.event_filters.substring_filter = text,
                        }
                        self.mark_live_cache_dirty();
                        self.data_index = 0;
                        self.live_event_index = 0;
                        self.period_event_index = 0;
                        self.refresh_live_position();
                    }
                    InputMode::TypesFilter => {
                        self.types_filter = self.input_buffer.trim().to_string();
                        self.type_index = 0;
                        self.path_index = 0;
                        self.types_path_focus = false;
                    }
                    InputMode::RenameType => {
                        let visible = self.visible_types();
                        if let Some(type_id) = visible.get(self.type_index) {
                            let type_id = type_id.clone();
                            self.model.rename_type(&type_id, self.input_buffer.clone());
                            self.user_renamed_types.insert(type_id);
                            self.mark_live_cache_dirty();
                        }
                    }
                    InputMode::InsertPeriodRange => {
                        let input = self.input_buffer.trim().to_string();
                        match self.parse_inclusive_event_range(&input) {
                            Ok((start_idx, end_idx)) => {
                                if let Err(err) =
                                    self.insert_period_from_event_range(start_idx, end_idx)
                                {
                                    self.status = err;
                                }
                            }
                            Err(err) => self.status = err,
                        }
                    }
                    InputMode::EditPeriodRange => {
                        let input = self.input_buffer.trim().to_string();
                        match self.parse_inclusive_event_range(&input) {
                            Ok((start_idx, end_idx)) => {
                                if let Err(err) =
                                    self.edit_selected_period_from_event_range(start_idx, end_idx)
                                {
                                    self.status = err;
                                }
                            }
                            Err(err) => self.status = err,
                        }
                    }
                    InputMode::ExportSessionPath => {
                        let raw = self.input_buffer.trim();
                        let path = if raw.is_empty() {
                            self.default_session_export_path()
                        } else {
                            PathBuf::from(raw)
                        };
                        self.export_session_to_path(path);
                    }
                    InputMode::ExportProfilePath => {
                        let raw = self.input_buffer.trim();
                        let path = if raw.is_empty() {
                            self.default_profile_export_path()
                        } else {
                            PathBuf::from(raw)
                        };
                        self.export_profile_to_path(path);
                    }
                    InputMode::None => {}
                }
                self.input_mode = InputMode::None;
                self.input_buffer.clear();
            }
            KeyCode::Backspace => {
                self.input_buffer.pop();
            }
            KeyCode::Char(c) => self.input_buffer.push(c),
            _ => {}
        }
        false
    }

    fn start_event_filter_input(&mut self, field: FilterField) {
        self.input_mode = InputMode::EventFilter(field);
        self.input_buffer = match field {
            FilterField::Key => self.event_filters.key_filter.clone(),
            FilterField::Type => self.event_filters.type_filter.clone(),
            FilterField::Fuzzy => self.event_filters.fuzzy_filter.clone(),
            FilterField::Exact => self.event_filters.exact_filter.clone(),
            FilterField::Substring => self.event_filters.substring_filter.clone(),
        };
    }

    pub fn filters_suspended(&self) -> bool {
        self.stashed_event_filters.is_some()
    }

    fn toggle_event_filters_enabled(&mut self) {
        if let Some(saved) = self.stashed_event_filters.take() {
            self.event_filters = saved;
            self.mark_live_cache_dirty();
            self.refresh_live_position();
            self.status = "Event filters restored".to_string();
            return;
        }

        if !self.event_filters.has_active() {
            self.status = "No active event filters to suspend".to_string();
            return;
        }

        self.stashed_event_filters = Some(self.event_filters.clone());
        self.event_filters = DataFilters::default();
        self.mark_live_cache_dirty();
        self.refresh_live_position();
        self.status = "Event filters suspended (press y to restore)".to_string();
    }

    fn apply_profile_filters(&mut self, filters: DataFilters) {
        if filters.has_active() {
            self.stashed_event_filters = None;
            self.event_filters = filters;
            self.mark_live_cache_dirty();
            self.refresh_live_position();
        }
    }

    fn confirm_pending_profile_override(&mut self) {
        let Some(profile) = self.pending_profile_override.take() else {
            return;
        };
        self.apply_profile_forced(profile);
        self.status = format!(
            "Profile applied (whitelist merged: {} terms)",
            self.whitelist_terms.len()
        );
    }

    fn cancel_pending_profile_override(&mut self) {
        self.pending_profile_override = None;
        self.status = "Kept restored session filters".to_string();
    }

    fn profile_matches_current_state(&self, profile: &SourceProfile) -> bool {
        self.current_profile_fingerprint() == profile_fingerprint(profile)
    }

    fn current_profile_fingerprint(&self) -> String {
        if let Some(saved) = self.pending_restore.as_ref() {
            let profile = SourceProfile {
                renames: saved.renames.clone(),
                known_unrelated_types: saved.known_unrelated_types.clone(),
                normalized_field_overrides: saved.normalized_field_overrides.clone(),
                negative_filters: self.event_filters.clone(),
                whitelist_terms: self.whitelist_terms.clone(),
            };
            return profile_fingerprint(&profile);
        }
        let profile = SourceProfile {
            renames: self.model.renamed_types(),
            known_unrelated_types: self
                .model
                .types
                .iter()
                .filter_map(|(type_id, tp)| tp.known_unrelated.then_some(type_id.clone()))
                .collect(),
            normalized_field_overrides: self.current_normalized_field_overrides(),
            negative_filters: self.event_filters.clone(),
            whitelist_terms: self.whitelist_terms.clone(),
        };
        profile_fingerprint(&profile)
    }

    fn has_nonempty_profile_state(&self) -> bool {
        self.event_filters.has_active()
            || !self.model.renamed_types().is_empty()
            || self
                .model
                .types
                .values()
                .any(|tp| tp.known_unrelated)
            || !self.model.normalized_field_overrides().is_empty()
            || !self.whitelist_terms.is_empty()
    }

    fn cycle_whitelist_mode(&mut self) {
        if self.whitelist_terms.is_empty() {
            self.status = "No whitelist loaded".to_string();
            return;
        }
        self.whitelist_mode = match self.whitelist_mode {
            WhitelistMode::AlwaysShow => WhitelistMode::OnlyWhitelist,
            WhitelistMode::OnlyWhitelist => WhitelistMode::Off,
            WhitelistMode::Off => WhitelistMode::AlwaysShow,
        };
        self.mark_live_cache_dirty();
        self.refresh_live_position();
        self.status = format!("Whitelist mode: {}", self.whitelist_mode_label());
    }

    fn handle_navigation_intent(&mut self, intent: NavIntent) {
        match self.mode {
            UiMode::Live => self.navigate_live(intent),
            UiMode::Periods => self.navigate_periods(intent),
            UiMode::Types => self.navigate_types(intent),
            UiMode::Data => self.navigate_data(intent),
        }
    }

    fn navigate_live(&mut self, intent: NavIntent) {
        if self.live_key_focus {
            match intent {
                NavIntent::LineUp => {
                    self.live_key_index = self.live_key_index.saturating_sub(1);
                    self.live_value_focus = false;
                    return;
                }
                NavIntent::LineDown => {
                    let keys = self.live_selected_key_paths();
                    if self.live_key_index + 1 < keys.len() {
                        self.live_key_index += 1;
                    }
                    self.live_value_focus = false;
                    return;
                }
                NavIntent::Left => {
                    if self.live_value_focus {
                        self.live_value_focus = false;
                        return;
                    }
                    self.exit_live_key_focus();
                    return;
                }
                NavIntent::Right => {
                    if self.selected_live_value_token().is_some() {
                        self.live_value_focus = true;
                    } else {
                        self.status = "Selected path has no value".to_string();
                    }
                    return;
                }
                NavIntent::PageUp => {
                    self.live_key_index = self.live_key_index.saturating_sub(MENU_PAGE_STEP);
                    self.live_value_focus = false;
                    return;
                }
                NavIntent::PageDown => {
                    let keys = self.live_selected_key_paths();
                    self.live_key_index =
                        (self.live_key_index + MENU_PAGE_STEP).min(keys.len().saturating_sub(1));
                    self.live_value_focus = false;
                    return;
                }
                NavIntent::Home => {
                    self.live_key_index = 0;
                    self.live_value_focus = false;
                    return;
                }
                NavIntent::End => {
                    let keys = self.live_selected_key_paths();
                    self.live_key_index = keys.len().saturating_sub(1);
                    self.live_value_focus = false;
                    return;
                }
            }
        }
        self.rebuild_live_cache_if_needed();
        let total = self.live_visible_total();
        if total == 0 {
            self.live_event_index = 0;
            self.live_view_start = 0;
            self.live_key_index = 0;
            self.live_key_focus = false;
            return;
        }

        let was_follow = self.live_follow;
        let step = self.live_page_step();
        self.live_event_index = match intent {
            NavIntent::LineUp => self.live_event_index.saturating_sub(1),
            NavIntent::LineDown => (self.live_event_index + 1).min(total.saturating_sub(1)),
            NavIntent::PageUp => self.live_event_index.saturating_sub(step),
            NavIntent::PageDown => (self.live_event_index + step).min(total.saturating_sub(1)),
            NavIntent::Home => 0,
            NavIntent::End => total.saturating_sub(1),
            NavIntent::Left => {
                self.exit_live_key_focus();
                return;
            }
            NavIntent::Right => {
                let has_keys = !self.live_selected_key_paths().is_empty();
                if has_keys {
                    self.enter_live_key_focus();
                } else {
                    self.live_key_focus = false;
                    self.status = "Selected event has no keys".to_string();
                }
                return;
            }
        };

        self.live_follow = false;
        if matches!(intent, NavIntent::Home) {
            self.live_edge_until_center = false;
            self.live_view_start = 0;
            self.clamp_live_indices_n(total);
            return;
        }
        if matches!(intent, NavIntent::End) {
            self.live_edge_until_center = false;
            let window = self.live_window_rows.max(1);
            self.live_view_start = total.saturating_sub(window);
            self.clamp_live_indices_n(total);
            return;
        }

        if was_follow {
            // When leaving follow, keep context from the stream head first, then converge to centered.
            self.live_view_start = self.live_event_index.saturating_sub(10);
            self.live_edge_until_center = true;
        }

        self.clamp_live_indices_n(total);
        self.reposition_live_selection_n(total);
        self.clamp_live_key_selection();
    }

    fn navigate_periods(&mut self, intent: NavIntent) {
        let periods_len = self.model.closed_periods().len();
        if periods_len == 0 {
            self.periods_index = 0;
            self.period_event_index = 0;
            self.period_json_key_index = 0;
            self.period_value_focus = false;
            self.periods_focus = PeriodsFocus::Periods;
            return;
        }
        self.periods_index = self.periods_index.min(periods_len.saturating_sub(1));
        let event_count = self.visible_period_events().len();
        if event_count == 0 {
            self.period_event_index = 0;
            self.period_json_key_index = 0;
            self.period_value_focus = false;
            if self.periods_focus != PeriodsFocus::Periods {
                self.periods_focus = PeriodsFocus::Periods;
            }
        } else {
            self.period_event_index = self.period_event_index.min(event_count.saturating_sub(1));
            self.clamp_period_key_selection();
        }

        match intent {
            NavIntent::Left => {
                self.periods_focus = match self.periods_focus {
                    PeriodsFocus::Periods => PeriodsFocus::Periods,
                    PeriodsFocus::Events => PeriodsFocus::Periods,
                    PeriodsFocus::Json => {
                        if self.period_value_focus {
                            self.period_value_focus = false;
                            PeriodsFocus::Json
                        } else {
                            PeriodsFocus::Events
                        }
                    }
                };
            }
            NavIntent::Right => {
                if self.periods_focus == PeriodsFocus::Json {
                    if self.selected_period_value_token().is_some() {
                        self.period_value_focus = true;
                    } else {
                        self.status = "Selected path has no value".to_string();
                    }
                } else {
                    self.advance_periods_focus();
                }
            }
            NavIntent::LineUp => match self.periods_focus {
                PeriodsFocus::Periods => {
                    if self.periods_index > 0 {
                        self.periods_index -= 1;
                        self.period_event_index = 0;
                        self.period_json_key_index = 0;
                        self.period_value_focus = false;
                    }
                }
                PeriodsFocus::Events => {
                    self.period_event_index = self.period_event_index.saturating_sub(1);
                    self.period_json_key_index = 0;
                    self.period_value_focus = false;
                }
                PeriodsFocus::Json => {
                    self.period_json_key_index = self.period_json_key_index.saturating_sub(1);
                    self.period_value_focus = false;
                }
            },
            NavIntent::LineDown => match self.periods_focus {
                PeriodsFocus::Periods => {
                    if self.periods_index + 1 < periods_len {
                        self.periods_index += 1;
                        self.period_event_index = 0;
                        self.period_json_key_index = 0;
                        self.period_value_focus = false;
                    }
                }
                PeriodsFocus::Events => {
                    if event_count > 0 && self.period_event_index + 1 < event_count {
                        self.period_event_index += 1;
                    }
                    self.period_json_key_index = 0;
                    self.period_value_focus = false;
                }
                PeriodsFocus::Json => {
                    let keys = self.period_selected_key_paths();
                    if self.period_json_key_index + 1 < keys.len() {
                        self.period_json_key_index += 1;
                    }
                    self.period_value_focus = false;
                }
            },
            NavIntent::Home => match self.periods_focus {
                PeriodsFocus::Periods => {
                    self.periods_index = 0;
                    self.period_event_index = 0;
                    self.period_json_key_index = 0;
                    self.period_value_focus = false;
                }
                PeriodsFocus::Events => {
                    self.period_event_index = 0;
                    self.period_json_key_index = 0;
                    self.period_value_focus = false;
                }
                PeriodsFocus::Json => {
                    self.period_json_key_index = 0;
                    self.period_value_focus = false;
                }
            },
            NavIntent::End => match self.periods_focus {
                PeriodsFocus::Periods => {
                    self.periods_index = periods_len.saturating_sub(1);
                    self.period_event_index = 0;
                    self.period_json_key_index = 0;
                    self.period_value_focus = false;
                }
                PeriodsFocus::Events => {
                    if event_count > 0 {
                        self.period_event_index = event_count.saturating_sub(1);
                    }
                    self.period_json_key_index = 0;
                    self.period_value_focus = false;
                }
                PeriodsFocus::Json => {
                    let keys = self.period_selected_key_paths();
                    if !keys.is_empty() {
                        self.period_json_key_index = keys.len().saturating_sub(1);
                    }
                    self.period_value_focus = false;
                }
            },
            NavIntent::PageUp => match self.periods_focus {
                PeriodsFocus::Periods => {
                    if self.periods_index > 0 {
                        self.periods_index = self.periods_index.saturating_sub(MENU_PAGE_STEP);
                        self.period_event_index = 0;
                        self.period_json_key_index = 0;
                        self.period_value_focus = false;
                    }
                }
                PeriodsFocus::Events => {
                    self.period_event_index =
                        self.period_event_index.saturating_sub(MENU_PAGE_STEP);
                    self.period_json_key_index = 0;
                    self.period_value_focus = false;
                }
                PeriodsFocus::Json => {
                    self.period_json_key_index =
                        self.period_json_key_index.saturating_sub(MENU_PAGE_STEP);
                    self.period_value_focus = false;
                }
            },
            NavIntent::PageDown => match self.periods_focus {
                PeriodsFocus::Periods => {
                    if self.periods_index + 1 < periods_len {
                        self.periods_index = (self.periods_index + MENU_PAGE_STEP)
                            .min(periods_len.saturating_sub(1));
                        self.period_event_index = 0;
                        self.period_json_key_index = 0;
                        self.period_value_focus = false;
                    }
                }
                PeriodsFocus::Events => {
                    if event_count > 0 {
                        self.period_event_index = (self.period_event_index + MENU_PAGE_STEP)
                            .min(event_count.saturating_sub(1));
                    }
                    self.period_json_key_index = 0;
                    self.period_value_focus = false;
                }
                PeriodsFocus::Json => {
                    let keys = self.period_selected_key_paths();
                    if !keys.is_empty() {
                        self.period_json_key_index = (self.period_json_key_index + MENU_PAGE_STEP)
                            .min(keys.len().saturating_sub(1));
                    }
                    self.period_value_focus = false;
                }
            },
        }
        self.clamp_period_key_selection();
    }

    fn navigate_types(&mut self, intent: NavIntent) {
        let visible = self.visible_types();
        let n = visible.len();
        if n == 0 {
            self.type_index = 0;
            self.path_index = 0;
            self.types_path_focus = false;
            return;
        }
        let selected_type = visible.get(self.type_index);
        let path_count = selected_type
            .and_then(|type_id| self.model.types.get(type_id))
            .map(|tp| tp.considered_paths.len())
            .unwrap_or(0);
        match intent {
            NavIntent::LineUp => {
                if self.types_path_focus {
                    self.path_index = self.path_index.saturating_sub(1);
                } else if self.type_index > 0 {
                    self.type_index -= 1;
                    self.path_index = 0;
                }
            }
            NavIntent::LineDown => {
                if self.types_path_focus {
                    if self.path_index + 1 < path_count {
                        self.path_index += 1;
                    }
                } else if self.type_index + 1 < n {
                    self.type_index += 1;
                    self.path_index = 0;
                }
            }
            NavIntent::Home => {
                if self.types_path_focus {
                    self.path_index = 0;
                } else {
                    self.type_index = 0;
                    self.path_index = 0;
                }
            }
            NavIntent::End => {
                if self.types_path_focus {
                    self.path_index = path_count.saturating_sub(1);
                } else {
                    self.type_index = n.saturating_sub(1);
                    self.path_index = 0;
                }
            }
            NavIntent::Left => {
                if self.types_path_focus {
                    self.types_path_focus = false;
                }
            }
            NavIntent::Right => {
                self.enter_types_path_focus();
            }
            NavIntent::PageUp => {
                if self.types_path_focus {
                    self.path_index = self.path_index.saturating_sub(MENU_PAGE_STEP);
                } else {
                    self.type_index = self.type_index.saturating_sub(MENU_PAGE_STEP);
                    self.path_index = 0;
                }
            }
            NavIntent::PageDown => {
                if self.types_path_focus {
                    if path_count > 0 {
                        self.path_index =
                            (self.path_index + MENU_PAGE_STEP).min(path_count.saturating_sub(1));
                    }
                } else {
                    self.type_index = (self.type_index + MENU_PAGE_STEP).min(n.saturating_sub(1));
                    self.path_index = 0;
                }
            }
        }
    }

    fn navigate_data(&mut self, intent: NavIntent) {
        self.ensure_baseline_cache();
        let total = self.visible_baseline_events().len();
        let page_step = MENU_PAGE_STEP;
        self.data_index = match intent {
            NavIntent::LineUp => self.data_index.saturating_sub(1),
            NavIntent::LineDown => (self.data_index + 1).min(total.saturating_sub(1)),
            NavIntent::PageUp => self.data_index.saturating_sub(page_step),
            NavIntent::PageDown => (self.data_index + page_step).min(total.saturating_sub(1)),
            NavIntent::Home => 0,
            NavIntent::End => total.saturating_sub(1),
            NavIntent::Left | NavIntent::Right => self.data_index,
        };
    }

    fn live_selected_event(&self) -> Option<&EventRecord> {
        self.live_event_at_visible_index(self.live_event_index)
    }

    pub fn live_selected_key_paths(&self) -> Vec<String> {
        let Some(event) = self.live_selected_event() else {
            return Vec::new();
        };
        // keys are already sorted and deduped by collect_all_paths
        event.keys.clone()
    }

    fn clamp_live_key_selection(&mut self) {
        let key_count = self.live_selected_key_paths().len();
        if key_count == 0 {
            self.live_key_index = 0;
            self.live_key_focus = false;
            return;
        }
        self.live_key_index = self.live_key_index.min(key_count.saturating_sub(1));
    }

    fn set_ui_mode(&mut self, mode: UiMode) {
        self.mode = mode;
        self.return_to_live_object_on_types_esc = false;
        self.return_to_types_on_live_esc = false;
        self.types_path_focus = false;
        self.type_preview_open = false;
        self.periods_focus = PeriodsFocus::Periods;
        self.period_value_focus = false;
        self.exit_live_key_focus();
    }

    fn toggle_live_key_focus(&mut self) {
        if self.mode != UiMode::Live {
            return;
        }
        self.clamp_live_key_selection();
        if self.live_selected_key_paths().is_empty() {
            self.status = "Selected event has no keys".to_string();
            self.live_key_focus = false;
            return;
        }
        let next = !self.live_key_focus;
        if next {
            self.enter_live_key_focus();
            self.status = "Live JSON keys focus: ON".to_string()
        } else {
            self.exit_live_key_focus();
            self.status = "Live JSON keys focus: OFF".to_string()
        };
    }

    fn enter_live_key_focus(&mut self) {
        if !self.live_key_focus {
            self.live_resume_follow_on_key_exit = self.live_follow;
            self.live_follow = false;
        }
        self.live_key_focus = true;
        self.live_value_focus = false;
    }

    fn apply_live_selected_key_filter(&mut self) {
        let keys = self.live_selected_key_paths();
        if let Some(path) = keys.get(self.live_key_index) {
            self.apply_key_filter_in_place(path);
        }
    }

    fn apply_period_selected_key_filter(&mut self) {
        let keys = self.period_selected_key_paths();
        if let Some(path) = keys.get(self.period_json_key_index) {
            self.apply_key_filter_in_place(path);
        } else {
            self.status = "Selected event has no keys".to_string();
        }
    }

    fn apply_period_selected_value_filter(&mut self) {
        let keys = self.period_selected_key_paths();
        let Some(path) = keys.get(self.period_json_key_index) else {
            return;
        };
        let Some(token) = self.selected_period_value_token() else {
            self.status = "Selected path has no value".to_string();
            return;
        };
        let exact = format!("{}={}", path, token);
        if self.event_filters.exact_filter == exact {
            self.event_filters.exact_filter.clear();
            self.status = format!("Removed exact filter: {}", exact);
        } else {
            self.event_filters.exact_filter = exact.clone();
            self.status = format!("Applied exact filter: {}", exact);
        }
        self.after_filter_change(None);
    }

    fn exit_live_key_focus(&mut self) {
        let was_focus = self.live_key_focus;
        self.live_key_focus = false;
        self.live_value_focus = false;
        if was_focus && self.live_resume_follow_on_key_exit {
            self.live_resume_follow_on_key_exit = false;
            self.live_follow = true;
            self.live_edge_until_center = false;
            self.pin_live_to_latest();
        } else {
            self.live_resume_follow_on_key_exit = false;
        }
    }

    fn after_filter_change(&mut self, selected_anchor: Option<LiveAnchor>) {
        self.stashed_event_filters = None;
        self.mark_live_cache_dirty();
        match self.mode {
            UiMode::Live => {
                self.refresh_live_position();
                if let Some(anchor) = selected_anchor.as_ref() {
                    if let Some(idx) = self.find_live_index(anchor) {
                        self.live_event_index = idx;
                        self.ensure_live_selection_visible();
                    }
                }
                self.clamp_live_key_selection();
            }
            UiMode::Periods => {
                let n = self.visible_period_events().len();
                if n == 0 {
                    self.period_event_index = 0;
                    self.periods_focus = PeriodsFocus::Periods;
                } else {
                    self.period_event_index = self.period_event_index.min(n.saturating_sub(1));
                }
            }
            UiMode::Data => {
                self.ensure_baseline_cache();
                let n = self.visible_baseline_events().len();
                if n == 0 {
                    self.data_index = 0;
                } else {
                    self.data_index = self.data_index.min(n.saturating_sub(1));
                }
            }
            UiMode::Types => {}
        }
    }

    fn apply_key_filter_in_place(&mut self, path: &str) {
        let selected_anchor = if self.mode == UiMode::Live {
            self.live_anchor_at(self.live_event_index)
        } else {
            None
        };
        if self.event_filters.key_filter == path {
            self.event_filters.key_filter.clear();
            self.status = format!("Removed key filter: {}", path);
        } else {
            self.event_filters.key_filter = path.to_string();
            self.status = format!("Applied key filter: {}", path);
        }
        self.after_filter_change(selected_anchor);
    }

    fn apply_live_selected_value_filter(&mut self) {
        let selected_anchor = self.live_anchor_at(self.live_event_index);
        let keys = self.live_selected_key_paths();
        let Some(path) = keys.get(self.live_key_index) else {
            return;
        };
        let Some(token) = self.selected_live_value_token() else {
            self.status = "Selected path has no value".to_string();
            return;
        };
        let exact = format!("{}={}", path, token);
        if self.event_filters.exact_filter == exact {
            self.event_filters.exact_filter.clear();
            self.status = format!("Removed exact filter: {}", exact);
        } else {
            self.event_filters.exact_filter = exact.clone();
            self.status = format!("Applied exact filter: {}", exact);
        }
        self.after_filter_change(selected_anchor);
    }

    fn selected_live_value_token(&self) -> Option<String> {
        let event = self.live_selected_event()?;
        let key = self
            .live_selected_key_paths()
            .get(self.live_key_index)?
            .clone();
        value_at_path(&event.obj, &key).map(value_token)
    }

    fn selected_period_value_token(&self) -> Option<String> {
        let event = self.selected_period_event()?;
        let key = self
            .period_selected_key_paths()
            .get(self.period_json_key_index)?
            .clone();
        value_at_path(&event.obj, &key).map(value_token)
    }

    fn jump_to_live_selected_event_type(&mut self) {
        let Some(event) = self.live_selected_event() else {
            return;
        };
        let type_id = event.type_id.clone();
        if let Some(idx) = self.model.find_type_index(&type_id) {
            let type_name = self.model.type_display_name(&type_id);
            self.mode = UiMode::Types;
            self.return_to_types_on_live_esc = false;
            self.type_index = idx;
            self.path_index = 0;
            self.types_path_focus = false;
            self.live_key_focus = false;
            self.live_value_focus = false;
            self.return_to_live_object_on_types_esc = true;
            self.status = format!("Jumped to type {}", type_name);
        }
    }

    fn jump_to_period_selected_event_type(&mut self) {
        let Some(event) = self.selected_period_event() else {
            return;
        };
        let type_id = event.type_id.clone();
        if let Some(idx) = self.model.find_type_index(&type_id) {
            let type_name = self.model.type_display_name(&type_id);
            self.mode = UiMode::Types;
            self.return_to_types_on_live_esc = false;
            self.type_index = idx;
            self.path_index = 0;
            self.types_path_focus = false;
            self.live_key_focus = false;
            self.live_value_focus = false;
            self.return_to_live_object_on_types_esc = false;
            self.status = format!("Jumped to type {}", type_name);
        }
    }

    fn live_page_step(&self) -> usize {
        let window = self.live_window_rows.max(1);
        // Page by almost a full viewport while keeping a tiny context overlap.
        if window <= 3 {
            1
        } else {
            window.saturating_sub(2)
        }
    }

    fn parse_inclusive_event_range(
        &self,
        text: &str,
    ) -> std::result::Result<(usize, usize), String> {
        let total = self.model.total_objects();
        if total == 0 {
            return Err("Cannot edit periods before events are loaded".to_string());
        }
        let compact = text.replace(' ', "");
        let (a, b) = if let Some((lhs, rhs)) = compact.split_once('-') {
            (lhs, rhs)
        } else if let Some((lhs, rhs)) = compact.split_once("..") {
            (lhs, rhs)
        } else if let Some((lhs, rhs)) = compact.split_once(',') {
            (lhs, rhs)
        } else {
            return Err("Range format: start-end (1-based, inclusive)".to_string());
        };
        let start = parse_usize_1based(a)?;
        let end = parse_usize_1based(b)?;
        if start == 0 || end == 0 {
            return Err("Indices must be >= 1".to_string());
        }
        let (start, end) = if start <= end {
            (start, end)
        } else {
            (end, start)
        };
        if end > total {
            return Err(format!("Range out of bounds: max event index is {}", total));
        }
        Ok((start, end))
    }

    fn event_range_to_timestamps(
        &self,
        start_idx_1based: usize,
        end_idx_1based: usize,
    ) -> std::result::Result<(f64, f64), String> {
        let start_zero = start_idx_1based.saturating_sub(1);
        let end_zero = end_idx_1based.saturating_sub(1);
        let Some(start_event) = self.model.events.get(start_zero) else {
            return Err(format!("Start index {} is out of bounds", start_idx_1based));
        };
        let Some(end_event) = self.model.events.get(end_zero) else {
            return Err(format!("End index {} is out of bounds", end_idx_1based));
        };
        Ok((start_event.ts, end_event.ts))
    }

    fn apply_periods_update(&mut self, periods: Vec<ActionPeriod>) {
        self.model.set_periods(periods);
        self.pending_live_recompute = true;
        self.mark_live_cache_dirty();
        if self.mode == UiMode::Live {
            self.refresh_live_position();
        }
    }

    fn set_period_selection_by_id(&mut self, period_id: u64) {
        let periods = self.model.closed_periods();
        if let Some(idx) = periods.iter().position(|p| p.id == period_id) {
            self.periods_index = idx;
        } else if periods.is_empty() {
            self.periods_index = 0;
        } else {
            self.periods_index = self.periods_index.min(periods.len().saturating_sub(1));
        }
    }

    fn insert_period_from_event_range(
        &mut self,
        start_idx_1based: usize,
        end_idx_1based: usize,
    ) -> std::result::Result<(), String> {
        let (start_ts, end_ts) =
            self.event_range_to_timestamps(start_idx_1based, end_idx_1based)?;
        let mut periods = self.model.periods.clone();
        let next_id = periods.iter().map(|p| p.id).max().unwrap_or(0) + 1;
        periods.push(ActionPeriod {
            id: next_id,
            label: self.model.current_label.clone(),
            start: start_ts,
            end: Some(end_ts),
        });
        self.apply_periods_update(periods);
        self.set_period_selection_by_id(next_id);
        self.period_event_index = 0;
        self.period_json_key_index = 0;
        self.pending_delete_period_id = None;
        self.status = format!(
            "Inserted period [{}] from events {}-{}",
            next_id, start_idx_1based, end_idx_1based
        );
        Ok(())
    }

    fn edit_selected_period_from_event_range(
        &mut self,
        start_idx_1based: usize,
        end_idx_1based: usize,
    ) -> std::result::Result<(), String> {
        let periods = self.model.closed_periods();
        let Some(selected) = periods.get(self.periods_index) else {
            return Err("No closed period selected to edit".to_string());
        };
        let selected_id = selected.id;
        let (start_ts, end_ts) =
            self.event_range_to_timestamps(start_idx_1based, end_idx_1based)?;
        let mut updated = self.model.periods.clone();
        let Some(target) = updated.iter_mut().find(|p| p.id == selected_id) else {
            return Err("Selected period could not be found".to_string());
        };
        target.start = start_ts;
        target.end = Some(end_ts);
        self.apply_periods_update(updated);
        self.set_period_selection_by_id(selected_id);
        self.period_event_index = 0;
        self.period_json_key_index = 0;
        self.pending_delete_period_id = None;
        self.status = format!(
            "Edited period [{}] to events {}-{}",
            selected_id, start_idx_1based, end_idx_1based
        );
        Ok(())
    }

    fn delete_period_by_id(&mut self, remove_id: u64) -> std::result::Result<(), String> {
        let periods = self.model.closed_periods();
        if periods.is_empty() {
            return Err("No closed periods to delete".to_string());
        }
        let Some(zero_idx) = periods.iter().position(|p| p.id == remove_id) else {
            return Err("Selected period could not be found".to_string());
        };
        let mut updated = self.model.periods.clone();
        updated.retain(|p| p.id != remove_id);
        self.apply_periods_update(updated);
        let closed_after = self.model.closed_periods().len();
        if closed_after == 0 {
            self.periods_index = 0;
        } else {
            self.periods_index = zero_idx.min(closed_after.saturating_sub(1));
        }
        self.period_event_index = 0;
        self.period_json_key_index = 0;
        self.pending_delete_period_id = None;
        self.status = format!("Deleted period id {}", remove_id);
        Ok(())
    }

    fn start_delete_selected_period_confirmation(&mut self) {
        let periods = self.model.closed_periods();
        let Some(selected) = periods.get(self.periods_index) else {
            self.status = "No closed period selected to delete".to_string();
            return;
        };
        self.pending_delete_period_id = Some(selected.id);
        self.status = format!(
            "Delete period [{}] #{} '{}' ? Press y to confirm or n to cancel",
            self.periods_index + 1,
            selected.id,
            selected.label
        );
    }

    fn delete_confirmation_status(&self) -> Option<String> {
        let period_id = self.pending_delete_period_id?;
        let periods = self.model.closed_periods();
        if let Some((idx, period)) = periods.iter().enumerate().find(|(_, p)| p.id == period_id) {
            return Some(format!(
                "Delete period [{}] #{} '{}' ? Press y to confirm or n to cancel",
                idx + 1,
                period.id,
                period.label
            ));
        }
        Some(format!(
            "Delete period #{} ? Press y to confirm or n to cancel",
            period_id
        ))
    }

    fn confirm_pending_period_delete(&mut self) {
        let Some(period_id) = self.pending_delete_period_id else {
            return;
        };
        if let Err(err) = self.delete_period_by_id(period_id) {
            self.pending_delete_period_id = None;
            self.status = err;
        }
    }

    fn cancel_pending_period_delete(&mut self) {
        self.pending_delete_period_id = None;
        self.status = "Delete cancelled".to_string();
    }

    pub fn period_row_range_for(&self, period: &ActionPeriod) -> Option<(usize, usize)> {
        let end = period.end?;
        let mut first_row: Option<usize> = None;
        let mut last_row: Option<usize> = None;
        for (idx, event) in self.model.events.iter().enumerate() {
            if event.ts < period.start || event.ts > end {
                continue;
            }
            let row = idx.saturating_add(1);
            if first_row.is_none() {
                first_row = Some(row);
            }
            last_row = Some(row);
        }
        match (first_row, last_row) {
            (Some(a), Some(b)) => Some((a, b)),
            _ => None,
        }
    }

    fn selected_period_row_range_input(&self) -> Option<String> {
        let periods = self.model.closed_periods();
        let period = periods.get(self.periods_index)?;
        self.period_row_range_for(period)
            .map(|(a, b)| format!("{a}-{b}"))
    }

    fn open_selected_event(&mut self) {
        self.rebuild_live_cache_if_needed();
        let selected = match self.mode {
            UiMode::Live => self
                .live_event_at_visible_index(self.live_event_index)
                .cloned(),
            UiMode::Periods => self
                .visible_period_events()
                .get(self.period_event_index)
                .cloned()
                .cloned(),
            UiMode::Data => {
                self.ensure_baseline_cache();
                self.visible_baseline_events()
                    .get(self.data_index)
                    .cloned()
                    .cloned()
            }
            UiMode::Types => None,
        };

        if let Some(event) = selected {
            let mut key_paths = event.keys.clone();
            key_paths.sort();
            key_paths.dedup();
            self.inspector = Some(ObjectInspector {
                event,
                key_paths,
                key_index: 0,
            });
        }
    }

    fn mark_live_cache_dirty(&mut self) {
        self.live_cache_dirty = true;
        self.baseline_cache_dirty = true;
    }

    fn rebuild_live_cache_if_needed(&mut self) {
        if self.loading_locked() {
            return;
        }
        if !self.live_cache_dirty {
            return;
        }
        let base = self.model.filtered_event_indices(&self.event_filters, None);
        self.live_visible_indices = self.apply_whitelist_to_indices(base, None);
        self.live_cache_dirty = false;
    }

    pub fn ensure_live_cache(&mut self) {
        self.rebuild_live_cache_if_needed();
    }

    pub fn ensure_baseline_cache(&mut self) {
        self.rebuild_baseline_cache_if_needed();
    }

    fn rebuild_baseline_cache_if_needed(&mut self) {
        if !self.baseline_cache_dirty {
            return;
        }
        let base = self
            .model
            .filtered_event_indices_in_slice(&self.baseline_events, &self.event_filters);
        self.baseline_visible_indices = self.apply_whitelist_to_baseline_indices(base);
        self.baseline_cache_dirty = false;
    }

    fn live_visible_total(&self) -> usize {
        self.live_visible_indices.len()
    }

    fn live_event_at_visible_index(&self, index: usize) -> Option<&EventRecord> {
        let event_idx = *self.live_visible_indices.get(index)?;
        self.model.events.get(event_idx)
    }

    pub fn visible_baseline_events(&self) -> Vec<&EventRecord> {
        self.baseline_visible_indices
            .iter()
            .filter_map(|idx| self.baseline_events.get(*idx))
            .collect()
    }

    fn apply_whitelist_to_baseline_indices(&self, indices: Vec<usize>) -> Vec<usize> {
        match self.whitelist_mode {
            WhitelistMode::Off => indices,
            WhitelistMode::OnlyWhitelist => (0..self.baseline_events.len())
                .rev()
                .filter(|idx| self.event_matches_whitelist(&self.baseline_events[*idx]))
                .collect(),
            WhitelistMode::AlwaysShow => {
                let mut seen: StdHashSet<usize> = StdHashSet::with_capacity(indices.len());
                let mut out = Vec::with_capacity(indices.len());
                for idx in indices {
                    if seen.insert(idx) {
                        out.push(idx);
                    }
                }
                for idx in (0..self.baseline_events.len()).rev() {
                    if self.event_matches_whitelist(&self.baseline_events[idx]) && seen.insert(idx) {
                        out.push(idx);
                    }
                }
                out
            }
        }
    }

    fn live_anchor_at(&self, index: usize) -> Option<LiveAnchor> {
        self.live_event_at_visible_index(index).map(|e| LiveAnchor {
            ts: e.ts,
            type_id: e.type_id.clone(),
        })
    }

    fn find_live_index(&self, anchor: &LiveAnchor) -> Option<usize> {
        self.live_visible_indices.iter().position(|&event_idx| {
            self.model
                .events
                .get(event_idx)
                .map(|e| e.ts == anchor.ts && e.type_id == anchor.type_id)
                .unwrap_or(false)
        })
    }

    pub fn set_live_window_rows(&mut self, rows: usize) {
        self.live_window_rows = rows.max(1);
    }

    pub fn live_render_data_for_window(&self, max_rows: usize) -> LiveRenderData<'_> {
        let total = self.live_visible_total();
        if total == 0 {
            return LiveRenderData {
                rows: Vec::new(),
                row_indices: Vec::new(),
                selected_visible: None,
                selected: None,
                total: 0,
            };
        }
        let window = max_rows.max(1);
        let mut start = if self.live_follow {
            total.saturating_sub(window)
        } else {
            self.live_view_start.min(total.saturating_sub(1))
        };
        if self.live_event_index < start {
            start = self.live_event_index;
        } else if self.live_event_index >= start + window {
            start = self.live_event_index + 1 - window;
        }
        if start + window > total {
            start = total.saturating_sub(window);
        }
        let end = (start + window).min(total);
        let mut rows: Vec<&EventRecord> = Vec::new();
        let mut row_indices: Vec<usize> = Vec::new();
        for &event_idx in &self.live_visible_indices[start..end] {
            if let Some(event) = self.model.events.get(event_idx) {
                rows.push(event);
                row_indices.push(event_idx.saturating_add(1));
            }
        }
        let selected = self.live_event_at_visible_index(self.live_event_index);
        let selected_visible = if self.live_event_index >= start && self.live_event_index < end {
            Some(self.live_event_index - start)
        } else {
            None
        };
        LiveRenderData {
            rows,
            row_indices,
            selected_visible,
            selected,
            total,
        }
    }

    fn clamp_live_indices_n(&mut self, total: usize) {
        if total == 0 {
            self.live_event_index = 0;
            self.live_view_start = 0;
            return;
        }
        self.live_event_index = self.live_event_index.min(total - 1);
        self.live_view_start = self.live_view_start.min(total - 1);
        let window = self.live_window_rows.max(1);
        if self.live_view_start + window > total {
            self.live_view_start = total.saturating_sub(window);
        }
    }

    fn clamp_live_indices(&mut self) {
        self.rebuild_live_cache_if_needed();
        let total = self.live_visible_total();
        self.clamp_live_indices_n(total);
    }

    fn pin_live_to_latest_n(&mut self, total: usize) {
        if total == 0 {
            self.live_event_index = 0;
            self.live_view_start = 0;
            self.live_key_index = 0;
            self.live_key_focus = false;
            return;
        }
        let window = self.live_window_rows.max(1);
        self.live_event_index = total - 1;
        self.live_view_start = total.saturating_sub(window);
        self.clamp_live_key_selection();
    }

    fn pin_live_to_latest(&mut self) {
        self.rebuild_live_cache_if_needed();
        let total = self.live_visible_total();
        self.pin_live_to_latest_n(total);
    }

    fn refresh_live_position(&mut self) {
        if self.live_follow {
            self.live_edge_until_center = false;
            self.pin_live_to_latest();
        } else {
            self.clamp_live_indices();
            self.reposition_live_selection();
            self.clamp_live_key_selection();
        }
    }

    fn ensure_live_selection_visible_n(&mut self, total: usize) {
        if total == 0 {
            self.live_event_index = 0;
            self.live_view_start = 0;
            self.live_key_index = 0;
            self.live_key_focus = false;
            return;
        }
        if self.live_event_index < self.live_view_start {
            self.live_view_start = self.live_event_index;
        } else if self.live_event_index >= self.live_view_start + self.live_window_rows.max(1) {
            let window = self.live_window_rows.max(1);
            self.live_view_start = self.live_event_index + 1 - window;
        }
        let window = self.live_window_rows.max(1);
        if self.live_view_start + window > total {
            self.live_view_start = total.saturating_sub(window);
        }
    }

    fn ensure_live_selection_visible(&mut self) {
        self.rebuild_live_cache_if_needed();
        let total = self.live_visible_total();
        self.ensure_live_selection_visible_n(total);
    }

    fn center_live_selection_in_view_n(&mut self, total: usize) {
        if total == 0 {
            self.live_event_index = 0;
            self.live_view_start = 0;
            return;
        }
        let window = self.live_window_rows.max(1);
        let half = window / 2;
        let max_start = total.saturating_sub(window);
        let desired_start = self.live_event_index.saturating_sub(half);
        self.live_view_start = desired_start.min(max_start);
    }

    fn reposition_live_selection_n(&mut self, total: usize) {
        if self.live_edge_until_center {
            self.ensure_live_selection_visible_n(total);
            if total == 0 {
                return;
            }
            let window = self.live_window_rows.max(1);
            let half = window / 2;
            let max_start = total.saturating_sub(window);
            let target_start = self.live_event_index.saturating_sub(half).min(max_start);
            // During the transition out of follow mode, do not auto-scroll toward center.
            // Let user movement naturally move the selected row to center, then lock it there.
            if self.live_view_start == target_start && target_start > 0 && target_start < max_start
            {
                self.live_edge_until_center = false;
            }
        } else {
            self.center_live_selection_in_view_n(total);
        }
    }

    fn reposition_live_selection(&mut self) {
        self.rebuild_live_cache_if_needed();
        let total = self.live_visible_total();
        self.reposition_live_selection_n(total);
    }

    pub fn visible_period_event_rows(&self) -> Vec<(usize, &EventRecord)> {
        let periods = self.model.closed_periods();
        if let Some(p) = periods.get(self.periods_index) {
            let start = p.start;
            let end = p.end.unwrap_or(p.start);
            let base = self
                .model
                .filtered_event_indices(&self.event_filters, Some((start, end)));
            let indices = match self.whitelist_mode {
                WhitelistMode::Off => base,
                WhitelistMode::OnlyWhitelist => self
                    .model
                    .events
                    .iter()
                    .enumerate()
                    .filter(|(_, e)| {
                        e.ts >= start && e.ts <= end && self.event_matches_whitelist(e)
                    })
                    .map(|(idx, _)| idx)
                    .collect(),
                WhitelistMode::AlwaysShow => {
                    let mut out = base;
                    for (idx, e) in self.model.events.iter().enumerate() {
                        if e.ts < start || e.ts > end {
                            continue;
                        }
                        if self.event_matches_whitelist(e) && !out.contains(&idx) {
                            out.push(idx);
                        }
                    }
                    out.sort_unstable();
                    out
                }
            };
            indices
                .into_iter()
                .filter_map(|idx| self.model.events.get(idx).map(|e| (idx, e)))
                .collect()
        } else {
            Vec::new()
        }
    }

    fn visible_period_events(&self) -> Vec<&EventRecord> {
        self.visible_period_event_rows()
            .into_iter()
            .map(|(_, e)| e)
            .collect()
    }

    pub fn visible_types(&self) -> Vec<String> {
        let query = self.types_filter.to_lowercase();
        let mut result: Vec<(String, u64)> = self
            .model
            .types
            .iter()
            .filter(|(type_id, tp)| {
                if query.is_empty() {
                    return true;
                }
                let name = tp.name.clone().unwrap_or_default().to_lowercase();
                let default = format!("type-{}", &type_id[..8]).to_lowercase();
                type_id.to_lowercase().contains(&query)
                    || name.contains(&query)
                    || default.contains(&query)
            })
            .map(|(type_id, tp)| (type_id.clone(), tp.count))
            .collect();
        result.sort_by(|a, b| b.1.cmp(&a.1));
        result.into_iter().map(|(id, _)| id).collect()
    }

    pub fn startup_hint(&self) -> Option<&str> {
        self.startup_hint.as_deref()
    }

    pub fn should_show_status_line(&self) -> bool {
        self.show_status_debug || self.loading_locked()
    }

    pub fn has_modal_confirmation(&self) -> bool {
        self.pending_delete_period_id.is_some() || self.pending_profile_override.is_some()
    }

    pub fn type_preview_open(&self) -> bool {
        self.type_preview_open
    }

    pub fn modal_confirmation(&self) -> Option<ModalConfirmation> {
        if let Some(period_id) = self.pending_delete_period_id {
            let periods = self.model.closed_periods();
            let detail = periods
                .iter()
                .enumerate()
                .find(|(_, p)| p.id == period_id)
                .map(|(idx, p)| {
                    let rows = self
                        .period_row_range_for(p)
                        .map(|(a, b)| format!("{a}-{b}"))
                        .unwrap_or_else(|| "-".to_string());
                    format!("Period [{}] #{} '{}' rows {}", idx + 1, p.id, p.label, rows)
                })
                .unwrap_or_else(|| format!("Period id {}", period_id));
            return Some(ModalConfirmation {
                title: "Delete Period".to_string(),
                lines: vec![
                    "`y` to delete, `n`/`Esc` to disregard".to_string(),
                    "".to_string(),
                    "Delete the selected action period?".to_string(),
                    detail,
                    "Events stay; only period boundaries are removed.".to_string(),
                ],
            });
        }
        if let Some(profile) = self.pending_profile_override.as_ref() {
            return Some(ModalConfirmation {
                title: "Apply Profile".to_string(),
                lines: vec![
                    "`y` to apply, `n`/`Esc` to disregard".to_string(),
                    "".to_string(),
                    "Apply profile over restored session state?".to_string(),
                    format!(
                        "Profile: {} renames, {} unrelated, {} normalized fields, filters {}/5, whitelist {} terms",
                        profile.renames.len(),
                        profile.known_unrelated_types.len(),
                        profile.normalized_field_overrides.len(),
                        profile.negative_filters.active_count(),
                        profile.whitelist_terms.len()
                    ),
                    "Whitelist merges additively with --whitelist terms.".to_string(),
                ],
            });
        }
        None
    }

    pub fn selected_period_event(&self) -> Option<&EventRecord> {
        self.visible_period_events()
            .get(self.period_event_index)
            .copied()
    }

    pub fn period_selected_key_paths(&self) -> Vec<String> {
        let Some(event) = self.selected_period_event() else {
            return Vec::new();
        };
        event.keys.clone()
    }

    pub fn baseline_tab_enabled(&self) -> bool {
        self.baseline_tab_enabled
    }

    pub fn type_excluded_by_type_filter(&self, type_id: &str) -> bool {
        let term = negated_type_term(&self.model.canonical_type_name(type_id));
        self.event_filters
            .type_filter
            .split("&&")
            .map(|s| s.trim())
            .any(|s| s == term)
    }

    pub fn whitelist_mode_label(&self) -> &'static str {
        match self.whitelist_mode {
            WhitelistMode::AlwaysShow => "always-show",
            WhitelistMode::OnlyWhitelist => "only-whitelist",
            WhitelistMode::Off => "off",
        }
    }

    pub fn whitelist_loaded(&self) -> bool {
        !self.whitelist_terms.is_empty()
    }

    pub fn whitelist_highlight_enabled(&self) -> bool {
        self.whitelist_loaded() && self.whitelist_mode != WhitelistMode::Off
    }

    pub fn whitelist_matches_event(&self, event: &EventRecord) -> bool {
        self.event_matches_whitelist(event)
    }

    pub fn whitelist_terms(&self) -> &[String] {
        &self.whitelist_terms
    }

    fn event_matches_whitelist(&self, event: &EventRecord) -> bool {
        if self.whitelist_terms.is_empty() {
            return false;
        }
        let obj = serde_json::to_string(&event.obj).unwrap_or_default().to_lowercase();
        self.whitelist_terms.iter().any(|needle| obj.contains(needle))
    }

    fn apply_whitelist_to_indices(
        &self,
        indices: Vec<usize>,
        range: Option<(f64, f64)>,
    ) -> Vec<usize> {
        match self.whitelist_mode {
            WhitelistMode::Off => indices,
            WhitelistMode::AlwaysShow => {
                let mut out = indices;
                for (idx, event) in self.model.events.iter().enumerate() {
                    if let Some((start, end)) = range {
                        if event.ts < start || event.ts > end {
                            continue;
                        }
                    }
                    if self.event_matches_whitelist(event) && !out.contains(&idx) {
                        out.push(idx);
                    }
                }
                out.sort_unstable();
                out
            }
            WhitelistMode::OnlyWhitelist => self
                .model
                .events
                .iter()
                .enumerate()
                .filter(|(_, e)| {
                    if let Some((start, end)) = range {
                        if e.ts < start || e.ts > end {
                            return false;
                        }
                    }
                    self.event_matches_whitelist(e)
                })
                .map(|(idx, _)| idx)
                .collect(),
        }
    }

    fn toggle_current_path(&mut self) {
        if self.mode != UiMode::Types || !self.types_path_focus {
            return;
        }
        let visible = self.visible_types();
        if let Some(type_id) = visible.get(self.type_index) {
            let type_id = type_id.clone();
            if let Some(tp) = self.model.types.get(&type_id) {
                let keys: Vec<String> = tp.considered_paths.keys().cloned().collect();
                if let Some(path) = keys.get(self.path_index) {
                    self.model.toggle_type_path(&type_id, path);
                    self.user_toggled_paths
                        .insert(path_override_key(&type_id, path));
                    self.pending_live_recompute = true;
                }
            }
        }
    }

    fn apply_selected_type_filter(&mut self) {
        if self.mode != UiMode::Types {
            return;
        }
        let visible = self.visible_types();
        if let Some(type_id) = visible.get(self.type_index) {
            self.stashed_event_filters = None;
            self.event_filters.type_filter = self.model.canonical_type_name(type_id);
            self.mode = UiMode::Live;
            self.return_to_types_on_live_esc = true;
            self.period_event_index = 0;
            self.live_event_index = 0;
            self.types_path_focus = false;
            self.mark_live_cache_dirty();
            self.refresh_live_position();
            self.status = format!(
                "Applied type filter in Live: {} (Esc to return)",
                self.model.type_display_name(type_id)
            );
        }
    }

    fn enter_types_path_focus(&mut self) {
        if self.mode != UiMode::Types {
            return;
        }
        let visible = self.visible_types();
        let Some(type_id) = visible.get(self.type_index) else {
            self.types_path_focus = false;
            return;
        };
        let Some(tp) = self.model.types.get(type_id) else {
            self.types_path_focus = false;
            return;
        };
        if tp.considered_paths.is_empty() {
            self.types_path_focus = false;
            self.status = "Selected type has no paths".to_string();
            return;
        }
        self.types_path_focus = true;
        self.path_index = self
            .path_index
            .min(tp.considered_paths.len().saturating_sub(1));
    }

    fn advance_periods_focus(&mut self) {
        if self.mode != UiMode::Periods {
            return;
        }
        let n = self.visible_period_events().len();
        if n == 0 {
            self.periods_focus = PeriodsFocus::Periods;
            self.period_event_index = 0;
            self.status = "Selected period has no events".to_string();
            return;
        }
        self.periods_focus = match self.periods_focus {
            PeriodsFocus::Periods => PeriodsFocus::Events,
            PeriodsFocus::Events => PeriodsFocus::Json,
            PeriodsFocus::Json => PeriodsFocus::Json,
        };
        self.period_value_focus = false;
        self.period_event_index = self.period_event_index.min(n.saturating_sub(1));
        self.clamp_period_key_selection();
    }

    fn clamp_period_key_selection(&mut self) {
        let key_count = self.period_selected_key_paths().len();
        if key_count == 0 {
            self.period_json_key_index = 0;
            return;
        }
        self.period_json_key_index = self.period_json_key_index.min(key_count.saturating_sub(1));
    }

    fn toggle_known_unrelated(&mut self) {
        if self.mode != UiMode::Types {
            return;
        }
        let visible = self.visible_types();
        if let Some(type_id) = visible.get(self.type_index) {
            let name = self.model.canonical_type_name(type_id);
            let term = negated_type_term(&name);
            let mut parts: Vec<String> = self
                .event_filters
                .type_filter
                .split("&&")
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            if parts.iter().any(|p| p == &term) {
                parts.retain(|p| p != &term);
                self.status = format!("Removed negative type filter: {}", name);
            } else {
                parts.push(term);
                self.status = format!("Added negative type filter: {}", name);
            }
            self.event_filters.type_filter = parts.join(" && ");
            self.mark_live_cache_dirty();
            self.refresh_live_position();
        }
    }

    fn loading_locked(&self) -> bool {
        if self.baseline_reader.is_some() && !self.baseline_loaded {
            return true;
        }
        if self.offline && !self.offline_loaded {
            return true;
        }
        if self.initial_load_complete {
            return false;
        }
        let target = self.initial_load_target_bytes.or_else(|| {
            let p = self.reader.progress();
            if p.total_bytes > 0 {
                Some(p.total_bytes)
            } else {
                None
            }
        });
        let Some(target) = target else {
            if self.initial_load_is_directory && !self.initial_load_complete {
                return true;
            }
            return false;
        };
        self.reader.progress().loaded_bytes < target
    }

    fn update_loading_status(&mut self) {
        if self.pending_profile_override.is_some() {
            return;
        }
        let loading_primary = if self.offline {
            !self.offline_loaded
        } else {
            !self.initial_load_complete
        };
        if self.baseline_reader.is_some() && !self.baseline_loaded && loading_primary {
            self.status = self.combined_load_status();
            return;
        }
        if self.baseline_reader.is_some() && !self.baseline_loaded {
            self.status = self.baseline_load_status();
            return;
        }
        if self.offline && !self.offline_loaded {
            self.status = self.offline_load_status();
            return;
        }

        if self.initial_load_complete {
            return;
        }

        let target = self.initial_load_target_bytes.or_else(|| {
            let p = self.reader.progress();
            if p.total_bytes > 0 {
                Some(p.total_bytes)
            } else {
                None
            }
        });
        let Some(target) = target else {
            if self.initial_load_is_directory {
                if self.initial_load_polled_once {
                    self.initial_load_complete = true;
                    self.status = format!(
                        "Initial load complete: {} objects",
                        self.model.total_objects()
                    );
                } else {
                    self.status = "Initial load: scanning directory...".to_string();
                }
            } else {
                self.initial_load_complete = true;
            }
            return;
        };
        if self.initial_load_target_bytes.is_none() {
            self.initial_load_target_bytes = Some(target);
        }
        let loaded = self.reader.progress().loaded_bytes;
        if loaded >= target {
            self.initial_load_complete = true;
            self.status = format!(
                "Initial load complete: {} objects",
                self.model.total_objects()
            );
            return;
        }
        self.status = self.initial_live_load_status(target);
    }

    fn offline_load_status(&self) -> String {
        let progress = self.reader.progress();
        let loaded = progress.loaded_bytes as f64 / (1024.0 * 1024.0);
        let total = progress.total_bytes as f64 / (1024.0 * 1024.0);
        let pct = if progress.total_bytes == 0 {
            0.0
        } else {
            (progress.loaded_bytes as f64 * 100.0 / progress.total_bytes as f64).clamp(0.0, 100.0)
        };
        let bar = progress_bar(pct / 100.0, 24);
        format!(
            "Loading {} {:>6.1}% ({:.1} / {:.1} MiB)  objects {}",
            bar,
            pct,
            loaded,
            total,
            self.model.total_objects()
        )
    }

    fn combined_load_status(&self) -> String {
        let baseline = self
            .baseline_reader
            .as_ref()
            .map(|r| r.progress())
            .unwrap_or(crate::io::StreamProgress {
                loaded_bytes: 0,
                total_bytes: 0,
            });
        let primary = self.reader.progress();
        let loaded_total = baseline.loaded_bytes.saturating_add(primary.loaded_bytes);
        let total_total = baseline.total_bytes.saturating_add(primary.total_bytes);
        if total_total == 0 {
            return format!(
                "Loading baseline + primary: scanned {} objects",
                self.model.total_objects()
            );
        }
        let pct = (loaded_total as f64 * 100.0 / total_total as f64).clamp(0.0, 100.0);
        let bar = progress_bar(pct / 100.0, 24);
        format!(
            "Loading baseline + primary: {} {:>5.1}% ({:.2}/{:.2} MB)",
            bar,
            pct,
            loaded_total as f64 / (1024.0 * 1024.0),
            total_total as f64 / (1024.0 * 1024.0)
        )
    }

    fn baseline_load_status(&self) -> String {
        let Some(reader) = self.baseline_reader.as_ref() else {
            return String::new();
        };
        let progress = reader.progress();
        let loaded = progress.loaded_bytes as f64 / (1024.0 * 1024.0);
        let total = progress.total_bytes as f64 / (1024.0 * 1024.0);
        let pct = if progress.total_bytes == 0 {
            0.0
        } else {
            (progress.loaded_bytes as f64 * 100.0 / progress.total_bytes as f64).clamp(0.0, 100.0)
        };
        let bar = progress_bar(pct / 100.0, 24);
        format!(
            "Loading baseline {} {:>6.1}% ({:.1} / {:.1} MiB)  objects {}",
            bar,
            pct,
            loaded,
            total,
            self.baseline_events.len()
        )
    }

    fn initial_live_load_status(&self, target_bytes: u64) -> String {
        let progress = self.reader.progress();
        let loaded_bytes = progress.loaded_bytes.min(target_bytes);
        let loaded = loaded_bytes as f64 / (1024.0 * 1024.0);
        let total = target_bytes as f64 / (1024.0 * 1024.0);
        let pct = if target_bytes == 0 {
            100.0
        } else {
            (loaded_bytes as f64 * 100.0 / target_bytes as f64).clamp(0.0, 100.0)
        };
        let bar = progress_bar(pct / 100.0, 24);
        format!(
            "Loading {} {:>6.1}% ({:.1} / {:.1} MiB)  objects {}",
            bar,
            pct,
            loaded,
            total,
            self.model.total_objects()
        )
    }
}

fn progress_bar(progress: f64, width: usize) -> String {
    if supports_unicode_blocks() {
        return progress_bar_unicode(progress, width);
    }
    progress_bar_ascii(progress, width)
}

fn progress_bar_unicode(progress: f64, width: usize) -> String {
    const PARTIALS: [char; 9] = [' ', '▏', '▎', '▍', '▌', '▋', '▊', '▉', '█'];
    let clamped = progress.clamp(0.0, 1.0);
    let exact = clamped * width as f64;
    let mut full = exact.floor() as usize;
    let partial_steps = ((exact - full as f64) * 8.0).floor() as usize;
    let mut s = String::with_capacity(width + 2);
    s.push('[');
    if full >= width {
        for _ in 0..width {
            s.push('█');
        }
    } else {
        for _ in 0..full {
            s.push('█');
        }
        if partial_steps > 0 {
            s.push(PARTIALS[partial_steps.min(8)]);
            full += 1;
        }
        for _ in full..width {
            s.push(' ');
        }
    }
    s.push(']');
    s
}

fn progress_bar_ascii(progress: f64, width: usize) -> String {
    let clamped = progress.clamp(0.0, 1.0);
    let filled = (clamped * width as f64).floor() as usize;
    let filled = filled.min(width);
    let mut s = String::with_capacity(width + 2);
    s.push('[');
    if filled == width {
        for _ in 0..width {
            s.push('=');
        }
    } else {
        for i in 0..width {
            if i < filled {
                s.push('=');
            } else if i == filled {
                s.push('>');
            } else {
                s.push(' ');
            }
        }
    }
    s.push(']');
    s
}

fn supports_unicode_blocks() -> bool {
    if env::var("JSON_TUI_ASCII")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
    {
        return false;
    }
    if env::var("JSON_TUI_UNICODE")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
    {
        return true;
    }
    for key in ["LC_ALL", "LC_CTYPE", "LANG"] {
        if let Ok(v) = env::var(key) {
            let lower = v.to_ascii_lowercase();
            if lower.contains("utf-8") || lower.contains("utf8") {
                return true;
            }
        }
    }
    false
}

fn parse_event_timestamp_millis(obj: &Value) -> Result<Option<f64>> {
    let Some(raw) = obj.get("_timestamp") else {
        return Ok(None);
    };
    let ms = if let Some(v) = raw.as_i64() {
        v
    } else if let Some(v) = raw.as_u64() {
        if v > i64::MAX as u64 {
            bail!("Unsupported input: `_timestamp` is out of range for epoch milliseconds.");
        }
        v as i64
    } else if let Some(v) = raw.as_f64() {
        if !v.is_finite() || v.fract() != 0.0 {
            bail!("Unsupported input: `_timestamp` must be an integer epoch-milliseconds value, got non-integer number.");
        }
        v as i64
    } else {
        bail!("Unsupported input: `_timestamp` must be a number in epoch milliseconds.");
    };

    if ms < 1_000_000_000_000 || ms > 9_999_999_999_999 {
        bail!(
            "Unsupported input: `_timestamp` must be epoch milliseconds (13-digit) like 1739952000123."
        );
    }
    Ok(Some(ms as f64 / 1000.0))
}

fn unix_ts() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

fn normalize_navigation_code(key: KeyEvent) -> KeyCode {
    match key.code {
        // Some terminals encode fn+arrows with modifier variants instead of PageUp/PageDown.
        KeyCode::Up
            if key.modifiers.intersects(
                KeyModifiers::ALT | KeyModifiers::SHIFT | KeyModifiers::SUPER | KeyModifiers::META,
            ) =>
        {
            KeyCode::PageUp
        }
        KeyCode::Down
            if key.modifiers.intersects(
                KeyModifiers::ALT | KeyModifiers::SHIFT | KeyModifiers::SUPER | KeyModifiers::META,
            ) =>
        {
            KeyCode::PageDown
        }
        _ => key.code,
    }
}

fn parse_usize_1based(raw: &str) -> std::result::Result<usize, String> {
    raw.parse::<usize>()
        .map_err(|_| format!("Invalid number: {raw}"))
}

fn negated_type_term(name: &str) -> String {
    let escaped = name.replace('\\', "\\\\").replace('"', "\\\"");
    format!("!\"{}\"", escaped)
}

fn path_override_key(type_id: &str, path: &str) -> String {
    format!("{}\n{}", type_id, path)
}

fn profile_fingerprint(profile: &SourceProfile) -> String {
    let normalized = normalize_profile(profile.clone());
    let bytes = serde_json::to_vec(&normalized).unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

fn normalize_profile(mut profile: SourceProfile) -> SourceProfile {
    profile.renames.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    profile.renames.dedup();
    profile.known_unrelated_types.sort();
    profile.known_unrelated_types.dedup();
    profile.normalized_field_overrides.sort_by(|a, b| {
        a.type_id
            .cmp(&b.type_id)
            .then(a.path.cmp(&b.path))
            .then((a.mode as u8).cmp(&(b.mode as u8)))
    });
    profile.normalized_field_overrides.dedup_by(|a, b| {
        a.type_id == b.type_id && a.path == b.path && a.mode == b.mode
    });
    let mut wl: Vec<String> = profile
        .whitelist_terms
        .into_iter()
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty())
        .collect();
    wl.sort();
    wl.dedup();
    profile.whitelist_terms = wl;
    profile
}

#[cfg(test)]
mod tests {
    use super::{
        normalize_navigation_code, parse_event_timestamp_millis, App, NavIntent, PeriodsFocus,
        MENU_PAGE_STEP,
    };
    use crate::persistence::{SessionEvent, SessionExport, SourceProfile};
    use crate::tui::UiMode;
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use serde_json::{json, Value};

    #[test]
    fn leaves_home_and_end_unchanged() {
        assert_eq!(
            normalize_navigation_code(KeyEvent::new(KeyCode::Home, KeyModifiers::NONE)),
            KeyCode::Home
        );
        assert_eq!(
            normalize_navigation_code(KeyEvent::new(KeyCode::End, KeyModifiers::NONE)),
            KeyCode::End
        );
    }

    #[test]
    fn normalizes_modified_arrows_to_page_navigation() {
        assert_eq!(
            normalize_navigation_code(KeyEvent::new(KeyCode::Up, KeyModifiers::ALT)),
            KeyCode::PageUp
        );
        assert_eq!(
            normalize_navigation_code(KeyEvent::new(KeyCode::Down, KeyModifiers::ALT)),
            KeyCode::PageDown
        );
        assert_eq!(
            normalize_navigation_code(KeyEvent::new(KeyCode::Up, KeyModifiers::SHIFT)),
            KeyCode::PageUp
        );
        assert_eq!(
            normalize_navigation_code(KeyEvent::new(KeyCode::Down, KeyModifiers::SUPER)),
            KeyCode::PageDown
        );
    }

    #[test]
    fn parses_timestamp_millis_and_enforces_13_digit_range() {
        assert_eq!(
            parse_event_timestamp_millis(&json!({"_timestamp": 1_739_952_000_123i64}))
                .expect("valid i64 timestamp"),
            Some(1_739_952_000.123)
        );
        assert_eq!(
            parse_event_timestamp_millis(&json!({"_timestamp": 1_739_952_000_123u64}))
                .expect("valid u64 timestamp"),
            Some(1_739_952_000.123)
        );
        assert_eq!(
            parse_event_timestamp_millis(&json!({"_timestamp": 1_739_952_000_123.0f64}))
                .expect("valid integer float timestamp"),
            Some(1_739_952_000.123)
        );

        assert!(parse_event_timestamp_millis(&json!({"_timestamp": 999_999_999_999i64})).is_err());
        assert!(parse_event_timestamp_millis(&json!({"_timestamp": 1.5f64})).is_err());
        assert!(parse_event_timestamp_millis(&json!({"_timestamp": "1739952000123"})).is_err());
    }

    #[test]
    fn resolve_event_ts_requires_timestamp_in_live_mode_but_not_offline() {
        let mut app = App::new(
            std::path::PathBuf::from("/tmp/json_demo/stream.jsonl"),
            None,
            false,
            false,
            false,
        );
        app.offline = false;
        let err = app
            .resolve_event_ts(&json!({"event":"login"}), 2000.0, 0)
            .expect_err("live mode should reject missing _timestamp");
        assert!(err
            .to_string()
            .contains("live mode requires root `_timestamp` as epoch milliseconds"));

        app.offline = true;
        app.offline_fallback_ts = 1000.0;
        let first = app
            .resolve_event_ts(&json!({"event":"offline"}), 5.0, 0)
            .expect("offline fallback timestamp");
        let second = app
            .resolve_event_ts(&json!({"event":"offline"}), 5.0, 1)
            .expect("offline fallback timestamp increments");
        assert_eq!(first, 1000.001);
        assert_eq!(second, 1000.002);
    }

    fn test_app() -> App {
        App::new(
            std::path::PathBuf::from("/tmp/test_app.jsonl"),
            None,
            false,
            false,
            false,
        )
    }

    #[test]
    fn clamp_live_indices_n_bounds_event_and_view_start() {
        let mut app = test_app();
        app.live_window_rows = 10;
        app.live_event_index = 100;
        app.live_view_start = 100;
        app.clamp_live_indices_n(20);
        // both indices clamped to max valid index
        assert_eq!(app.live_event_index, 19);
        // view_start: min(100, 19) = 19, then 19+10 > 20, so 20-10 = 10
        assert_eq!(app.live_view_start, 10);
    }

    #[test]
    fn clamp_live_indices_n_handles_zero_total() {
        let mut app = test_app();
        app.live_event_index = 5;
        app.live_view_start = 5;
        app.clamp_live_indices_n(0);
        assert_eq!(app.live_event_index, 0);
        assert_eq!(app.live_view_start, 0);
    }

    #[test]
    fn periods_events_are_chronological_with_global_indices() {
        let mut app = test_app();
        app.model.ingest(json!({"_timestamp": 1_700_000_000_000u64, "x": 1}), 1.0);
        app.model.ingest(json!({"_timestamp": 1_700_000_001_000u64, "x": 2}), 2.0);
        app.model.ingest(json!({"_timestamp": 1_700_000_002_000u64, "x": 3}), 3.0);
        app.model.set_periods(vec![crate::domain::ActionPeriod {
            id: 1,
            label: "p".to_string(),
            start: 1.0,
            end: Some(3.0),
        }]);
        let rows = app.visible_period_event_rows();
        let ts: Vec<f64> = rows.iter().map(|(_, e)| e.ts).collect();
        let idxs: Vec<usize> = rows.iter().map(|(i, _)| *i).collect();
        assert_eq!(ts, vec![1.0, 2.0, 3.0]);
        assert_eq!(idxs, vec![0, 1, 2]);
    }

    #[test]
    fn import_same_override_profile_does_not_prompt() {
        let mut app = test_app();
        let mut session = SessionExport::new("/tmp/test_app.jsonl".to_string());
        session.event_filters.type_filter = "t".to_string();
        session.events = vec![SessionEvent {
            ts: 1.0,
            obj: json!({"_timestamp": 1_700_000_000_000u64, "k":"v"}),
        }];
        let profile = SourceProfile {
            renames: vec![],
            known_unrelated_types: vec![],
            normalized_field_overrides: vec![],
            negative_filters: session.event_filters.clone(),
            whitelist_terms: vec![],
        };
        session.profile = Some(profile.clone());
        app.import_session(session, Some(profile)).expect("import");
        assert!(app.pending_profile_override.is_none());
    }

    #[test]
    fn import_different_override_profile_prompts() {
        let mut app = test_app();
        let mut session = SessionExport::new("/tmp/test_app.jsonl".to_string());
        session.event_filters.type_filter = "a".to_string();
        session.events = vec![SessionEvent {
            ts: 1.0,
            obj: json!({"_timestamp": 1_700_000_000_000u64, "k":"v"}),
        }];
        session.profile = Some(SourceProfile {
            renames: vec![],
            known_unrelated_types: vec![],
            normalized_field_overrides: vec![],
            negative_filters: crate::domain::DataFilters::default(),
            whitelist_terms: vec![],
        });
        let override_profile = SourceProfile {
            renames: vec![],
            known_unrelated_types: vec![],
            normalized_field_overrides: vec![],
            negative_filters: crate::domain::DataFilters {
                type_filter: "b".to_string(),
                ..crate::domain::DataFilters::default()
            },
            whitelist_terms: vec![],
        };
        app.import_session(session, Some(override_profile))
            .expect("import");
        assert!(app.pending_profile_override.is_some());
    }

    #[test]
    fn ensure_live_selection_visible_n_scrolls_view_to_show_selection() {
        let mut app = test_app();
        app.live_window_rows = 10;

        // Selection above view start: view should scroll up
        app.live_event_index = 3;
        app.live_view_start = 10;
        app.ensure_live_selection_visible_n(50);
        assert_eq!(app.live_view_start, 3);

        // Selection below visible window: view should scroll down
        app.live_event_index = 25;
        app.live_view_start = 10;
        app.ensure_live_selection_visible_n(50);
        assert_eq!(app.live_view_start, 16); // 25 + 1 - 10 = 16
    }

    #[test]
    fn center_live_selection_in_view_n_places_selection_at_midpoint() {
        let mut app = test_app();
        app.live_window_rows = 10;
        app.live_event_index = 25;
        app.center_live_selection_in_view_n(50);
        // half = 5, desired_start = 25 - 5 = 20, max_start = 50 - 10 = 40
        assert_eq!(app.live_view_start, 20);
    }

    #[test]
    fn center_live_selection_in_view_n_clamps_near_start() {
        let mut app = test_app();
        app.live_window_rows = 10;
        app.live_event_index = 2;
        app.center_live_selection_in_view_n(50);
        // desired_start = 2 - 5 = saturates to 0
        assert_eq!(app.live_view_start, 0);
    }

    #[test]
    fn reposition_live_selection_n_centers_when_flag_is_clear() {
        let mut app = test_app();
        app.live_window_rows = 10;
        app.live_event_index = 25;
        app.live_edge_until_center = false;
        app.reposition_live_selection_n(50);
        assert_eq!(app.live_view_start, 20); // centered
    }

    #[test]
    fn reposition_live_selection_n_uses_ensure_visible_when_flag_is_set() {
        let mut app = test_app();
        app.live_window_rows = 10;
        app.live_event_index = 3;
        app.live_view_start = 10;
        app.live_edge_until_center = true;
        app.reposition_live_selection_n(50);
        // ensure_visible: selection (3) < view_start (10) → view_start set to 3
        assert_eq!(app.live_view_start, 3);
        // flag remains set (target_start=0, view_start=3, they differ)
        assert!(app.live_edge_until_center);
    }

    #[test]
    fn set_ui_mode_resets_all_focus_flags() {
        let mut app = test_app();
        app.return_to_live_object_on_types_esc = true;
        app.return_to_types_on_live_esc = true;
        app.types_path_focus = true;
        app.periods_focus = PeriodsFocus::Json;
        app.live_key_focus = true;

        app.set_ui_mode(UiMode::Types);

        assert!(!app.return_to_live_object_on_types_esc);
        assert!(!app.return_to_types_on_live_esc);
        assert!(!app.types_path_focus);
        assert_eq!(app.periods_focus, PeriodsFocus::Periods);
        assert!(!app.live_key_focus);
    }

    #[test]
    fn navigate_types_supports_page_up_down_for_type_list() {
        let mut app = test_app();
        for i in 0..80 {
            let mut map = serde_json::Map::new();
            map.insert(format!("k{}", i), json!(i));
            app.model.ingest(Value::Object(map), i as f64);
        }
        app.type_index = 0;
        app.types_path_focus = false;

        app.navigate_types(NavIntent::PageDown);
        assert_eq!(app.type_index, MENU_PAGE_STEP);

        app.navigate_types(NavIntent::PageUp);
        assert_eq!(app.type_index, 0);
    }

    #[test]
    fn navigate_types_supports_page_up_down_for_path_list() {
        let mut app = test_app();
        let mut map = serde_json::Map::new();
        for i in 0..80 {
            map.insert(format!("k{}", i), json!(i));
        }
        app.model.ingest(Value::Object(map), 1.0);
        app.type_index = 0;
        app.types_path_focus = true;
        app.path_index = 0;

        app.navigate_types(NavIntent::PageDown);
        assert_eq!(app.path_index, MENU_PAGE_STEP);

        app.navigate_types(NavIntent::PageUp);
        assert_eq!(app.path_index, 0);
    }
}
