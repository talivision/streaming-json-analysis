use crate::browser::{JsonFocusNav, JsonFocusState};
use crate::control_http::{ControlCommand, ControlReply};
use crate::domain::{
    apply_rename_batch_to_filter, clear_positive_type_filters, collect_indexed_paths,
    dedupe_filter_terms, default_type_label, expand_merged_label_in_filter, normalize_path,
    prepare_event, rename_type_terms_in_filter, replace_positive_type_filters,
    toggle_negated_type_in_filter, type_is_negated_in_filter, value_at_path, value_token,
    values_at_path, ActionPeriod, AnalyzerModel, DataFilters, EventRecord, FilterField, MergeGroup,
    PreparedEvent,
};
use crate::io::StreamReader;
use crate::persistence::{
    export_session, save_profile, NormalizedFieldOverride, PersistedState, RestoredState,
    SessionEvent, SessionExport, SourceProfile,
};
use crate::tui::{draw_file_changed_prompt, draw_ui, InputMode, UiMode};
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
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet, HashSet as StdHashSet};
use std::env;
use std::fs;
use std::io::stdout;
use std::path::PathBuf;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::TryRecvError;
use std::thread;
use std::time::{Duration, Instant};
use std::time::{SystemTime, UNIX_EPOCH};

const LIVE_WINDOW_DEFAULT: usize = 120;
// File backend: poll every 50ms (20 Hz). Faster than the HTTP idle path's
// 100ms so a local writer's events show up promptly, but slower than the
// previous 10ms cadence which made every per-event O(n) refresh visible
// as a stutter (e.g. pressing `m` to mark a period).
const FILE_POLL_INTERVAL: Duration = Duration::from_millis(50);
const HTTP_IDLE_POLL_INTERVAL: Duration = Duration::from_millis(100);
const UI_FRAME_SLEEP: Duration = Duration::from_millis(16);
const UI_BURST_SLEEP: Duration = Duration::from_millis(1);
const MENU_PAGE_STEP: usize = 30;
const QUIT_CONFIRM_WINDOW: Duration = Duration::from_secs(2);
const AUTOSAVE_INTERVAL: Duration = Duration::from_secs(30);
const WARNING_PREFIX_ORANGE: &str = "\x1b[38;5;208mwarning:\x1b[0m";

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

#[derive(Debug, Clone)]
struct LiveAnchor {
    // Position in `model.events`. The stable, unique identifier for an event
    // — `events` only appends, never reorders or deletes, so `event_idx`
    // survives any cache rebuild. Previously anchored by `(ts, type_id)`,
    // which collided when many events shared the same millisecond
    // timestamp and type id and caused the cursor to snap to the first
    // duplicate after a re-ingest cycle.
    event_idx: usize,
}

#[derive(Debug, Clone)]
enum FilterOrigin {
    TypedInput,
    KeyShortcut { anchor: Option<LiveAnchor> },
    TypeView,
}

pub use crate::browser::NavIntent;

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
    pub data_key_index: usize,
    pub data_key_focus: bool,
    pub data_value_focus: bool,
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
    pub values_key: String,
    pub values_index: usize,
    pub values_return_mode: UiMode,
    values_cache: Option<Vec<(String, String, usize)>>,

    pub escape_strings: bool,
    pub offline: bool,
    pub status: String,
    stashed_event_filters: Option<DataFilters>,
    stashed_live_visible_indices: Option<(usize, Vec<usize>)>,
    stashed_baseline_visible_indices: Option<(usize, Vec<usize>)>,
    pending_live_anchor: Option<LiveAnchor>,
    reader: StreamReader,
    baseline_reader: Option<StreamReader>,
    baseline_events: Vec<EventRecord>,
    baseline_loaded: bool,
    offline_loaded: bool,
    offline_fallback_ts: f64,
    pending_restore: Option<RestoredState>,
    file_changed_state: Option<RestoredState>,
    startup_hint: Option<String>,
    live_visible_indices: Vec<usize>,
    baseline_visible_indices: Vec<usize>,
    live_cache_dirty: bool,
    baseline_cache_dirty: bool,
    initial_load_target_bytes: Option<u64>,
    initial_load_complete: bool,
    pending_live_recompute: bool,
    show_status_debug: bool,
    quit_pending_until: Option<Instant>,
    pending_delete_period_id: Option<u64>,
    pending_profile_override: Option<SourceProfile>,
    baseline_tab_enabled: bool,
    export_path: Option<PathBuf>,
    control_rx: Option<Receiver<ControlCommand>>,
    whitelist_terms: Vec<String>,
    whitelist_mode: WhitelistMode,
    profile_renames: Vec<(String, String)>,
    session_renames: Vec<(String, String)>,
    profile_known_unrelated_types: Vec<String>,
    profile_normalized_field_overrides: Vec<NormalizedFieldOverride>,
    user_renamed_types: HashSet<String>,
    user_toggled_unrelated_types: HashSet<String>,
    user_toggled_paths: HashSet<String>,
    type_preview_open: bool,
    pub triaged_event_indices: HashSet<usize>,
    /// Triaged identifiers loaded from the persisted state that we couldn't
    /// yet map to a Vec index (events hadn't ingested when restore ran).
    /// Drained by `apply_persisted_overrides_if_ready`.
    pending_triaged_identities: Vec<(f64, String)>,
    state_dirty: bool,
    swapfile: Option<crate::persistence::Swapfile>,
    collapsed_paths: HashMap<String, HashSet<String>>,
    pub selected_type_ids: HashSet<String>,
    pub pending_unmerge_group_id: Option<String>,
}

impl App {
    pub fn set_control_receiver(&mut self, rx: Receiver<ControlCommand>) {
        self.control_rx = Some(rx);
    }

    /// Convenience for callers that have a local path. Equivalent to
    /// `from_reader(StreamReader::from_path(stream_path), ...)`.
    pub fn new(
        stream_path: PathBuf,
        baseline_path: Option<PathBuf>,
        offline: bool,
        show_status_debug: bool,
        reset_state: bool,
        escape_strings: bool,
    ) -> Self {
        Self::from_reader(
            StreamReader::from_path(stream_path),
            baseline_path,
            offline,
            show_status_debug,
            reset_state,
            escape_strings,
        )
    }

    pub fn from_reader(
        reader: StreamReader,
        baseline_path: Option<PathBuf>,
        offline: bool,
        show_status_debug: bool,
        reset_state: bool,
        escape_strings: bool,
    ) -> Self {
        let baseline_enabled = baseline_path.is_some();
        // For local files we know the target byte size up-front (so the
        // loading bar is meaningful). For HTTP we don't HEAD here —
        // first poll will populate `last_known_len` and the bar
        // recalibrates.
        let initial_load_target_bytes = reader
            .local_path()
            .and_then(|p| fs::metadata(p).ok())
            .map(|m| m.len())
            .filter(|len| *len > 0);
        let initial_load_complete = initial_load_target_bytes.is_none() && !reader.is_http();
        let source_display_for_status = reader.source_display();
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
            data_key_index: 0,
            data_key_focus: false,
            data_value_focus: false,
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
            values_key: String::new(),
            values_index: 0,
            values_return_mode: UiMode::Live,
            values_cache: None,

            escape_strings,
            offline,
            status: if offline {
                format!(
                    "Offline mode: analyzing {} (no live tail)",
                    source_display_for_status
                )
            } else {
                format!("Watching {}", source_display_for_status)
            },
            stashed_event_filters: None,
            stashed_live_visible_indices: None,
            stashed_baseline_visible_indices: None,
            pending_live_anchor: None,
            reader,
            baseline_reader: baseline_path.map(StreamReader::from_path),
            baseline_events: Vec::new(),
            baseline_loaded: false,
            offline_loaded: false,
            offline_fallback_ts: unix_ts(),
            pending_restore: None,
            file_changed_state: None,
            startup_hint: None,
            live_visible_indices: Vec::new(),
            baseline_visible_indices: Vec::new(),
            live_cache_dirty: true,
            baseline_cache_dirty: true,
            initial_load_target_bytes,
            initial_load_complete,
            pending_live_recompute: false,
            show_status_debug,
            quit_pending_until: None,
            pending_delete_period_id: None,
            pending_profile_override: None,
            baseline_tab_enabled: baseline_enabled,
            export_path: None,
            control_rx: None,
            whitelist_terms: Vec::new(),
            whitelist_mode: WhitelistMode::Off,
            profile_renames: Vec::new(),
            session_renames: Vec::new(),
            profile_known_unrelated_types: Vec::new(),
            profile_normalized_field_overrides: Vec::new(),
            user_renamed_types: HashSet::new(),
            user_toggled_unrelated_types: HashSet::new(),
            user_toggled_paths: HashSet::new(),
            type_preview_open: false,
            triaged_event_indices: HashSet::new(),
            pending_triaged_identities: Vec::new(),
            state_dirty: false,
            swapfile: None,
            collapsed_paths: HashMap::new(),
            selected_type_ids: HashSet::new(),
            pending_unmerge_group_id: None,
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

        if let Some(changed) = self.file_changed_state.take() {
            let accepted = self.run_file_changed_prompt(&mut terminal, &changed)?;
            if accepted {
                self.apply_transferable_state(changed);
            }
        }

        terminal.draw(|f| draw_ui(f, self))?;

        let mut last_poll = Instant::now() - FILE_POLL_INTERVAL;
        let mut last_autosave = Instant::now();

        let loop_result = (|| -> Result<()> {
            loop {
                let loop_started_at = Instant::now();
                let was_loading_locked = self.loading_locked();

                if !self.baseline_loaded {
                    self.ingest_baseline_corpus()?;
                }
                self.drain_control_commands();

                let mut ingested_any = false;
                if !self.offline || !self.offline_loaded {
                    let mut should_poll = self.offline && !self.offline_loaded;
                    if !self.offline {
                        // Burst-poll during initial bulk load so a 50 MB file isn't
                        // capped at 20 polls/sec → ~1 s of wall just for the cadence.
                        // Once initial_load_complete flips, fall back to the per-
                        // backend throttle so steady-state mutations (e.g. pressing
                        // `m`) don't fight a 100 Hz poll loop.
                        let poll_interval = if !self.initial_load_complete {
                            Duration::ZERO
                        } else if self.reader.is_http() {
                            HTTP_IDLE_POLL_INTERVAL
                        } else {
                            FILE_POLL_INTERVAL
                        };
                        if last_poll.elapsed() >= poll_interval {
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

                if self.state_dirty && last_autosave.elapsed() >= AUTOSAVE_INTERVAL {
                    self.autosave_dirty_state();
                    last_autosave = Instant::now();
                }

                terminal.draw(|f| draw_ui(f, self))?;
                self.rebuild_live_cache_if_needed();
                // Re-pin after every cache rebuild: the cache may have been stale when
                // pin_live_to_latest() was called inside ingest_new_events(), which would
                // leave the cursor at row 0 even with follow on (e.g. first frame after a
                // file-changed restore).
                if self.mode == UiMode::Live && self.live_follow {
                    self.pin_live_to_latest();
                }
                self.clamp_live_indices();
                self.apply_pending_live_anchor();

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
        if self.reader.has_incomplete_final_line() {
            eprintln!(
                "{} incomplete JSON line remained at shutdown in {}",
                WARNING_PREFIX_ORANGE,
                self.reader.source_display()
            );
        }
        if let Some(reader) = self.baseline_reader.as_ref() {
            if reader.has_incomplete_final_line() {
                eprintln!(
                    "{} incomplete JSON line remained at shutdown in {}",
                    WARNING_PREFIX_ORANGE,
                    reader.source_display()
                );
            }
        }
        self.model.close_open_period(unix_ts());
        // Ensure the state file is written at shutdown, regardless of dirty
        // flag — close_open_period above may have closed a period without
        // going through a dirty-marking helper.
        self.state_dirty = true;
        self.autosave_dirty_state();
        if let Err(err) = self.export_session_if_configured() {
            eprintln!("{WARNING_PREFIX_ORANGE} failed to export session: {err}");
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
            self.status =
                "Apply profile over restored session state? (y/N, whitelist merges additively)"
                    .to_string();
            return;
        }
        self.apply_profile_seeded(profile);
        self.status = "Loaded source profile".to_string();
    }

    fn drain_control_commands(&mut self) {
        loop {
            let next = match self.control_rx.as_ref() {
                None => return,
                Some(rx) => match rx.try_recv() {
                    Ok(cmd) => Some(cmd),
                    Err(TryRecvError::Empty) => None,
                    Err(TryRecvError::Disconnected) => {
                        self.control_rx = None;
                        None
                    }
                },
            };
            let Some(cmd) = next else {
                return;
            };
            self.apply_control_command(cmd);
        }
    }

    fn apply_control_command(&mut self, cmd: ControlCommand) {
        match cmd {
            ControlCommand::Start { label, reply } => {
                let response = self.control_start_action(label);
                let _ = reply.send(response);
            }
            ControlCommand::Stop { reply } => {
                let response = self.control_stop_action();
                let _ = reply.send(response);
            }
            ControlCommand::Status { reply } => {
                let response = self.control_status_response();
                let _ = reply.send(response);
            }
        }
    }

    fn control_start_action(&mut self, label: Option<String>) -> ControlReply {
        if self.offline {
            return ControlReply {
                status: 409,
                body: json!({ "ok": false, "error": "cannot start action period in offline mode" }),
            };
        }
        if let Some(period) = self.model.active_period() {
            return ControlReply {
                status: 200,
                body: json!({
                    "ok": true,
                    "changed": false,
                    "active": true,
                    "period": {
                        "id": period.id,
                        "label": period.label,
                        "start": period.start,
                        "end": period.end
                    }
                }),
            };
        }
        if let Some(next_label) = label {
            let trimmed = next_label.trim();
            if !trimmed.is_empty() {
                self.model.current_label = trimmed.to_string();
                self.mark_dirty();
            }
        }
        if !self.do_toggle_period("HTTP") {
            return ControlReply {
                status: 409,
                body: json!({
                    "ok": false,
                    "error": "cannot start action period before first event timestamp is ingested"
                }),
            };
        }
        self.control_status_with_changed(true)
    }

    fn control_stop_action(&mut self) -> ControlReply {
        if self.offline {
            return ControlReply {
                status: 409,
                body: json!({ "ok": false, "error": "cannot stop action period in offline mode" }),
            };
        }
        if self.model.active_period().is_none() {
            return ControlReply {
                status: 200,
                body: json!({
                    "ok": true,
                    "changed": false,
                    "active": false,
                    "period": Value::Null
                }),
            };
        }
        if !self.do_toggle_period("HTTP") {
            return ControlReply {
                status: 500,
                body: json!({ "ok": false, "error": "failed to stop action period" }),
            };
        }
        self.control_status_with_changed(true)
    }

    fn control_status_response(&self) -> ControlReply {
        self.control_status_with_changed(false)
    }

    fn do_toggle_period(&mut self, source_label: &str) -> bool {
        if !self.model.toggle_period() {
            self.status = "Cannot start: no event timestamp yet".to_string();
            return false;
        }
        self.pending_live_recompute = true;
        self.mark_dirty();
        self.status = if let Some(period) = self.model.active_period() {
            format!(
                "Action started: {} #{} ({})",
                period.label, period.id, source_label
            )
        } else {
            format!("Action ended ({})", source_label)
        };
        true
    }

    fn control_status_with_changed(&self, changed: bool) -> ControlReply {
        let active_period = self.model.active_period();
        ControlReply {
            status: 200,
            body: json!({
                "ok": true,
                "changed": changed,
                "active": active_period.is_some(),
                "period": active_period.map(|p| json!({
                    "id": p.id,
                    "label": p.label,
                    "start": p.start,
                    "end": p.end
                })).unwrap_or(Value::Null),
                "events": self.model.total_objects(),
                "offline": self.offline
            }),
        }
    }

    fn apply_profile_seeded(&mut self, profile: SourceProfile) {
        self.profile_renames = profile.renames.clone();
        self.profile_known_unrelated_types = profile.known_unrelated_types.clone();
        self.profile_normalized_field_overrides = profile.normalized_field_overrides.clone();
        self.apply_profile_overrides_to_types();
        self.add_whitelist_terms(profile.whitelist_terms);
        // Apply filters BEFORE merge groups so the filter-rewrite step has the
        // profile's filter terms to operate on.
        self.apply_profile_filters(profile.negative_filters);
        if self.apply_profile_merge_groups(&profile.merge_groups) {
            self.pending_live_recompute = true;
        }
        // The profile import path mutates renames and overrides (shared) plus
        // the local known_unrelated_types list. apply_profile_filters already
        // marks local; bump shared too so other operators see the imported
        // renames/overrides and merge groups.
        self.mark_dirty();
    }

    fn apply_profile_forced(&mut self, profile: SourceProfile) {
        let SourceProfile {
            renames,
            known_unrelated_types,
            normalized_field_overrides,
            negative_filters,
            whitelist_terms,
            merge_groups,
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
        self.stashed_live_visible_indices = None;
        self.stashed_baseline_visible_indices = None;
        self.event_filters = negative_filters;
        let applied_merges = self.apply_profile_merge_groups(&merge_groups);
        self.mark_live_cache_dirty();
        self.mark_dirty();
        self.refresh_live_position();
        if applied_merges {
            self.pending_live_recompute = true;
        }
    }

    /// Apply merge groups carried by an imported profile to the current model.
    ///
    /// Two cases are handled per group:
    /// * Runtime import after events have streamed in: `merge_types` folds
    ///   each member's existing TypeProfile into the merged group.
    /// * Boot-time import (CLI `--profile` with `--reset`) when no events have
    ///   been ingested yet: `merge_types` returns None because members aren't
    ///   present, so we fall back to `model.apply_merge_groups` to register
    ///   the alias hook before ingest — same path used by session restore.
    ///
    /// Returns true if at least one group was successfully applied. Caller is
    /// responsible for triggering `pending_live_recompute`.
    fn apply_profile_merge_groups(&mut self, groups: &[MergeGroup]) -> bool {
        if groups.is_empty() {
            return false;
        }
        let mut any_applied = false;
        for group in groups {
            let cleaned_label = group.label.trim().to_string();
            // Snapshot prior display names BEFORE any mutation: prefer the
            // live canonical name (catches user renames), fall back to the
            // profile's saved prior name, then to the default `type-<hex>`
            // label. Pre-ingest we won't have a live canonical, but the
            // saved/default chain still gives us the names that any
            // hand-edited filter term would reference.
            let prior_names: Vec<String> = group
                .members
                .iter()
                .enumerate()
                .map(|(i, m)| {
                    if self.model.types.contains_key(m.as_str()) {
                        self.model.canonical_type_name(m)
                    } else {
                        group
                            .members_prior_name
                            .get(i)
                            .and_then(|o| o.clone())
                            .unwrap_or_else(|| default_type_label(m))
                    }
                })
                .collect();
            let merged_via_stats = self
                .model
                .merge_types(&group.members, cleaned_label.clone())
                .is_some();
            if !merged_via_stats && !self.model.merge_groups.contains_key(&group.group_id) {
                // Pre-ingest path: model has no member types yet, so register
                // the alias hook so the upcoming event stream redirects into
                // the group. Same approach as session restore.
                self.model.apply_merge_groups(std::slice::from_ref(group));
                if !self.model.merge_groups.contains_key(&group.group_id) {
                    continue;
                }
            }
            // Always rewrite filter terms when the group is present, even if
            // it was already registered locally. A re-applied profile whose
            // negative_filters reference member names needs the rename so the
            // resulting filter targets the merged label instead of phantom
            // member ids.
            any_applied = true;
            for prior in &prior_names {
                self.event_filters.type_filter = rename_type_terms_in_filter(
                    &self.event_filters.type_filter,
                    prior,
                    &cleaned_label,
                );
                if let Some(stashed) = self.stashed_event_filters.as_mut() {
                    stashed.type_filter =
                        rename_type_terms_in_filter(&stashed.type_filter, prior, &cleaned_label);
                }
            }
        }
        if any_applied {
            self.event_filters.type_filter = dedupe_filter_terms(&self.event_filters.type_filter);
            if let Some(stashed) = self.stashed_event_filters.as_mut() {
                stashed.type_filter = dedupe_filter_terms(&stashed.type_filter);
            }
            self.mark_live_cache_dirty();
        }
        any_applied
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
            merge_groups,
        } = session;
        self.offline = true;
        self.offline_loaded = true;
        self.initial_load_complete = true;
        self.baseline_loaded = true;
        self.pending_restore = None;

        self.model = AnalyzerModel::new();
        // Register merge groups BEFORE any ingestion so the alias hook redirects
        // every replayed event into the appropriate group.
        if !merge_groups.is_empty() {
            self.model.apply_merge_groups(&merge_groups);
        }
        self.baseline_events.clear();
        for ev in &baseline_events {
            validate_type_field(&ev.obj)?;
            let prepared = prepare_event(ev.obj.clone());
            let obj_size = prepared.obj.to_string().len() as u32;
            // Capture the original structural type id for baseline events too,
            // so unmerge can rebuild them faithfully.
            let raw_type_id = prepared.type_id.clone();
            self.model.ingest_baseline_prepared(&prepared, ev.ts);
            let effective_type_id = self
                .model
                .type_aliases
                .get(&raw_type_id)
                .cloned()
                .unwrap_or_else(|| raw_type_id.clone());
            let original = if effective_type_id != raw_type_id {
                Some(raw_type_id)
            } else {
                None
            };
            self.baseline_events.push(EventRecord {
                ts: ev.ts,
                type_id: effective_type_id,
                obj: prepared.obj,
                keys: prepared.keys,
                size_bytes: obj_size,
                action_period_id: None,
                in_action_period: false,
                live_rate_score: 0.0,
                live_uniq_score: 0.0,
                original_type_id: original,
            });
        }
        for ev in &events {
            validate_type_field(&ev.obj)?;
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
        self.rebuild_live_cache_if_needed();
        let use_snapshot_parallel = self.offline
            || self.loading_locked()
            || (self.reader.is_http() && !self.initial_load_complete);
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
            validate_type_field(&prepared.obj)?;
            let ts = self.resolve_event_ts(&prepared.obj, batch_now, idx)?;
            if let Some(last) = self.model.events.last() {
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
            validate_type_field(&prepared.obj)?;
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
                original_type_id: None,
            });
        }
        self.baseline_tab_enabled = !self.baseline_events.is_empty();
        let progress = reader.progress();
        let baseline_path_display = reader.source_display();
        self.baseline_loaded =
            progress.total_bytes == 0 || progress.loaded_bytes >= progress.total_bytes;
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
        // Unified file + HTTP restore: read by source_id, ask the reader
        // to verify the stored offset/identity, then dispatch the same
        // Clean/Changed branches as before.
        let source_id = self.reader.source_id().to_string();
        let state = match crate::persistence::read_state_for_id(&source_id) {
            Ok(Some(s)) => s,
            Ok(None) => return,
            Err(err) => {
                self.status = format!("State restore skipped: {err}");
                self.startup_hint = Some(self.status.clone());
                return;
            }
        };
        let saved_identity = crate::io::SourceIdentity {
            prefix_hash_hex: state.prefix_hash_hex.clone(),
            etag: state.source_etag.clone(),
        };
        let verdict = match self.reader.verify_resume(state.saved_len, &saved_identity) {
            Ok(v) => v,
            Err(err) => {
                self.status = format!("State verify failed: {err}");
                self.startup_hint = Some(self.status.clone());
                return;
            }
        };
        let saved = crate::persistence::restored_from(&state);
        match verdict {
            crate::io::ResumeVerdict::Clean => self.apply_clean_restore(saved),
            crate::io::ResumeVerdict::Changed => self.file_changed_state = Some(saved),
        }
    }

    fn apply_clean_restore(&mut self, saved: RestoredState) {
        let msg = format!(
                    "Restored session: {} periods, {} renames, {} unrelated, {} normalized fields, {} merge groups, filters {}/5{}{}",
                    saved.periods.len(),
                    saved.renames.len(),
                    saved.known_unrelated_types.len(),
                    saved.normalized_field_overrides.len(),
                    saved.merge_groups.len(),
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
        if !saved.merge_groups.is_empty() {
            self.model.apply_merge_groups(&saved.merge_groups);
        }
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

    /// Applies the transferable portion of a file-changed state: everything
    /// except periods, which reference timestamps in the old file content.
    fn apply_transferable_state(&mut self, saved: RestoredState) {
        if !saved.current_label.trim().is_empty() {
            self.model.current_label = saved.current_label.clone();
        }
        self.event_filters = saved.event_filters.clone();
        self.stashed_event_filters = saved.stashed_event_filters.clone();
        self.types_filter = saved.types_filter.clone();
        // Apply merge groups upfront so the alias hook is active during ingest.
        if !saved.merge_groups.is_empty() {
            self.model.apply_merge_groups(&saved.merge_groups);
        }
        self.mark_live_cache_dirty();
        self.refresh_live_position();
        // Store renames in session_renames so they persist across ingest cycles and
        // survive even if no matching type IDs appear in the new file.
        self.session_renames = saved.renames.clone();
        // Carry periods forward as empty so apply_persisted_overrides_if_ready
        // still applies renames, unrelated flags, and field overrides.
        let transferable = RestoredState {
            periods: vec![],
            ..saved
        };
        self.pending_restore = Some(transferable);
        let msg =
            "Session restored (file changed: renames and filters restored, periods discarded)"
                .to_string();
        self.status = msg.clone();
        self.startup_hint = Some(msg);
        self.mark_dirty();
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
        if !saved.merge_groups.is_empty() {
            self.model.apply_merge_groups(&saved.merge_groups);
        }
        self.apply_triaged_identities(&saved.triaged_events);
        self.apply_profile_overrides_to_types();
    }

    /// Translates `(ts, type_id)` identities into in-memory Vec indices.
    /// Identities we couldn't resolve (events not yet ingested) are stashed in
    /// `pending_triaged_identities` so they survive saves and get retried next
    /// time this function runs.
    fn apply_triaged_identities(&mut self, triaged: &[(f64, String)]) {
        let mut unresolved: Vec<(f64, String)> = Vec::new();
        for (ts, type_id) in triaged {
            let found = self
                .model
                .events
                .iter()
                .position(|e| (e.ts - ts).abs() < f64::EPSILON && &e.type_id == type_id);
            if let Some(idx) = found {
                self.triaged_event_indices.insert(idx);
            } else {
                unresolved.push((*ts, type_id.clone()));
            }
        }
        self.pending_triaged_identities = unresolved;
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
        // Apply renames restored from a previous session (file-changed path) at lowest
        // priority: skip if a profile or the user already set a name for this type.
        for (type_id, name) in &self.session_renames {
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

    fn run_file_changed_prompt(
        &self,
        terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
        state: &RestoredState,
    ) -> Result<bool> {
        loop {
            terminal.draw(|f| draw_file_changed_prompt(f, state))?;
            if event::poll(Duration::from_millis(50))? {
                if let Event::Key(key) = event::read()? {
                    if !matches!(key.kind, KeyEventKind::Press) {
                        continue;
                    }
                    match key.code {
                        KeyCode::Char('y') | KeyCode::Char('Y') | KeyCode::Enter => {
                            return Ok(true);
                        }
                        KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => {
                            return Ok(false);
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    fn autosave_dirty_state(&mut self) {
        if !self.reader.source_exists() {
            // The stream source was deleted / rotated while we were
            // running. Invalidate the state file so nothing is restored
            // next session.
            if self.state_dirty {
                let source_id = self.reader.source_id().to_string();
                if let Err(err) = crate::persistence::invalidate_state_for_id(&source_id) {
                    eprintln!("{WARNING_PREFIX_ORANGE} failed to invalidate state: {err}");
                } else {
                    self.state_dirty = false;
                }
            }
            return;
        }
        if self.state_dirty {
            match self.persist_state() {
                Ok(()) => self.state_dirty = false,
                Err(err) => {
                    eprintln!("{WARNING_PREFIX_ORANGE} failed to persist state: {err}");
                }
            }
        }
    }

    fn build_state_for_save(&self) -> Result<PersistedState> {
        // Combine in-memory renames with session_renames (renames that survive
        // a file-changed restore but reference types not yet ingested). Model
        // renames win for any type_id present in both.
        let mut all_renames = self.model.renamed_types();
        {
            let applied: std::collections::HashSet<String> =
                all_renames.iter().map(|(id, _)| id.clone()).collect();
            for (type_id, name) in &self.session_renames {
                if !applied.contains(type_id) {
                    all_renames.push((type_id.clone(), name.clone()));
                }
            }
        }
        let triaged_events: Vec<(f64, String)> = self
            .triaged_event_indices
            .iter()
            .filter_map(|&idx| {
                self.model
                    .events
                    .get(idx)
                    .map(|e| (e.ts, e.type_id.clone()))
            })
            .chain(self.pending_triaged_identities.iter().cloned())
            .collect();
        let merge_groups: Vec<MergeGroup> = self.model.merge_groups.values().cloned().collect();
        let identity = self.reader.current_identity()?;
        Ok(PersistedState {
            version: 2,
            stream_path: self.reader.source_id().to_string(),
            saved_len: self.reader.offset(),
            prefix_hash_hex: identity.prefix_hash_hex,
            source_etag: identity.etag,
            periods: self.model.periods.clone(),
            renames: all_renames,
            known_unrelated_types: self
                .model
                .types
                .iter()
                .filter_map(|(type_id, tp)| tp.known_unrelated.then_some(type_id.clone()))
                .collect(),
            normalized_field_overrides: self.current_normalized_field_overrides(),
            triaged_events,
            current_label: self.model.current_label.clone(),
            event_filters: self.event_filters.clone(),
            stashed_event_filters: self.stashed_event_filters.clone(),
            types_filter: self.types_filter.clone(),
            merge_groups,
        })
    }

    fn persist_state(&self) -> Result<()> {
        let state = self.build_state_for_save()?;
        let source_id = self.reader.source_id().to_string();
        crate::persistence::save_state_for_id(&source_id, &state)
    }

    pub fn set_swapfile(&mut self, swap: crate::persistence::Swapfile) {
        self.swapfile = Some(swap);
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
            merge_groups: self.model.merge_groups.values().cloned().collect(),
        };
        match save_profile(&path, &profile) {
            Ok(_) => self.status = format!("Profile exported to {}", path.display()),
            Err(err) => self.status = format!("Profile export failed: {err}"),
        }
    }

    fn build_session_export(&self) -> SessionExport {
        let merge_groups: Vec<_> = self.model.merge_groups.values().cloned().collect();
        let mut snapshot = SessionExport::new(self.reader.source_display());
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
        snapshot.merge_groups = merge_groups.clone();
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
            merge_groups: merge_groups.clone(),
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
        let base = self
            .reader
            .local_path()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| PathBuf::from("session-export"));
        let mut p = base;
        let fname = p
            .file_name()
            .and_then(|n| n.to_str())
            .map(|n| format!("{n}.session.json"))
            .unwrap_or_else(|| "session-export.json".to_string());
        p.set_file_name(fname);
        p
    }

    fn default_profile_export_path(&self) -> PathBuf {
        let base = self
            .reader
            .local_path()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| PathBuf::from("source-profile"));
        let mut p = base;
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
        // Don't intercept `q` for quit-confirm while the user is typing into
        // an input prompt (rename, filter, label, etc.) — let `q` reach the
        // input buffer like any other character.
        if matches!(key.code, KeyCode::Char('q')) && self.input_mode == InputMode::None {
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
        if self.pending_unmerge_group_id.is_some() {
            match key.code {
                KeyCode::Char('y') | KeyCode::Char('Y') => {
                    self.confirm_unmerge_pending();
                    return false;
                }
                KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => {
                    self.cancel_pending_unmerge();
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

        if self.mode == UiMode::Types && self.type_preview_open {
            match code {
                KeyCode::Esc => {
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
            KeyCode::Esc if self.mode == UiMode::Values => {
                self.return_from_values_browser();
            }
            KeyCode::Esc if self.mode == UiMode::Live && self.live_key_focus => {
                self.exit_live_key_focus();
            }
            KeyCode::Esc if self.mode == UiMode::Data && self.data_key_focus => {
                self.exit_data_key_focus();
            }
            KeyCode::Esc if self.mode == UiMode::Types && self.types_path_focus => {
                self.types_path_focus = false;
            }
            KeyCode::Esc if self.mode == UiMode::Types && !self.selected_type_ids.is_empty() => {
                self.selected_type_ids.clear();
                self.status = "Merge selection cleared".to_string();
            }
            KeyCode::Esc
                if self.mode == UiMode::Periods && self.periods_focus != PeriodsFocus::Periods =>
            {
                self.handle_navigation_intent(NavIntent::Left);
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
                    self.status =
                        "Baseline view is unavailable (start with --baseline)".to_string();
                }
            }
            KeyCode::Esc if self.mode == UiMode::Live && self.return_to_types_on_live_esc => {
                self.mode = UiMode::Types;
                self.return_to_types_on_live_esc = false;
                self.event_filters.type_filter =
                    clear_positive_type_filters(&self.event_filters.type_filter);
                self.stashed_event_filters = None;
                self.stashed_live_visible_indices = None;
                self.stashed_baseline_visible_indices = None;
                self.mark_live_cache_dirty();
                self.mark_dirty();
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
                } else {
                    self.do_toggle_period("keyboard");
                }
            }

            KeyCode::Char('f') if self.mode == UiMode::Live && self.live_key_focus => {
                self.toggle_collapse_live();
            }
            KeyCode::Char('f')
                if self.mode == UiMode::Periods && self.periods_focus == PeriodsFocus::Json =>
            {
                self.toggle_collapse_period();
            }
            KeyCode::Char('f') if self.mode == UiMode::Data && self.data_key_focus => {
                self.toggle_collapse_data();
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
            KeyCode::Char('r')
                if self.mode == UiMode::Periods && self.periods_focus == PeriodsFocus::Periods =>
            {
                let label = self
                    .model
                    .periods
                    .get(self.periods_index)
                    .map(|p| p.label.clone())
                    .unwrap_or_default();
                self.input_mode = InputMode::RenamePeriod;
                self.input_buffer = label;
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
            KeyCode::Char('v') if self.mode == UiMode::Live && self.live_key_focus => {
                self.enter_values_from_live();
            }
            KeyCode::Char('v')
                if self.mode == UiMode::Periods && self.periods_focus == PeriodsFocus::Json =>
            {
                self.enter_values_from_periods();
            }
            KeyCode::Char('v') if self.mode == UiMode::Data && self.data_key_focus => {
                self.enter_values_from_data();
            }
            KeyCode::Char('k') if self.mode == UiMode::Live && self.live_key_focus => {
                self.apply_live_selected_key_filter();
            }
            KeyCode::Char('k') if self.mode == UiMode::Data && self.data_key_focus => {
                self.apply_data_selected_key_filter();
            }
            KeyCode::Char('e') if self.mode == UiMode::Values => {
                self.apply_values_selection();
            }
            KeyCode::Char('e')
                if self.mode == UiMode::Live && self.live_key_focus && self.live_value_focus =>
            {
                self.apply_live_selected_value_filter();
            }
            KeyCode::Char('e')
                if self.mode == UiMode::Data && self.data_key_focus && self.data_value_focus =>
            {
                self.apply_data_selected_value_filter();
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
            KeyCode::Char('t') if self.mode == UiMode::Data && self.data_key_focus => {
                self.jump_to_data_selected_event_type()
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
            KeyCode::Char('s') if self.mode == UiMode::Types => {
                self.toggle_type_merge_selection();
            }
            KeyCode::Char('g') if self.mode == UiMode::Types => {
                self.begin_merge_or_unmerge();
            }
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
                let anchor = if self.mode == UiMode::Live {
                    self.live_anchor_at(self.live_event_index)
                } else {
                    None
                };
                self.event_filters = DataFilters::default();
                self.commit_filter_change(FilterOrigin::KeyShortcut { anchor });
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
            KeyCode::Enter if self.mode == UiMode::Values => {
                self.apply_values_selection();
            }
            KeyCode::Enter if self.mode == UiMode::Live && self.live_key_focus => {
                if self.live_value_focus || self.live_selected_path_prefers_exact_filter() {
                    self.apply_live_selected_value_filter();
                } else {
                    self.apply_live_selected_key_filter();
                }
            }
            KeyCode::Enter if self.mode == UiMode::Data && self.data_key_focus => {
                if self.data_value_focus || self.data_selected_path_prefers_exact_filter() {
                    self.apply_data_selected_value_filter();
                } else {
                    self.apply_data_selected_key_filter();
                }
            }
            KeyCode::Enter
                if self.mode == UiMode::Periods && self.periods_focus == PeriodsFocus::Json =>
            {
                if self.period_value_focus || self.period_selected_path_prefers_exact_filter() {
                    self.apply_period_selected_value_filter();
                } else {
                    self.apply_period_selected_key_filter();
                }
            }
            KeyCode::Enter if self.mode == UiMode::Live => self.toggle_live_key_focus(),
            KeyCode::Enter if self.mode == UiMode::Data => self.toggle_data_key_focus(),
            KeyCode::Enter if self.mode == UiMode::Types => self.enter_types_path_focus(),
            KeyCode::Enter if self.mode == UiMode::Periods => self.advance_periods_focus(),
            KeyCode::Char(' ')
                if self.mode == UiMode::Periods && self.periods_focus == PeriodsFocus::Events =>
            {
                self.toggle_triage_period_event();
            }
            KeyCode::Char(' ') => self.toggle_current_path(),
            KeyCode::Char('u') => self.toggle_known_unrelated(),
            _ => {}
        }
        self.quit_pending_until = None;
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
                            self.mark_dirty();
                            self.status = format!("Current label: {}", self.model.current_label);
                        }
                    }
                    InputMode::RenamePeriod => {
                        let label = self.input_buffer.trim().to_string();
                        let periods = self.model.periods.clone();
                        if let Some(period) = periods.get(self.periods_index) {
                            let id = period.id;
                            let mut updated = self.model.periods.clone();
                            if let Some(p) = updated.iter_mut().find(|p| p.id == id) {
                                p.label = label.clone();
                            }
                            self.apply_periods_update(updated);
                            self.status = format!("Renamed period to '{}'", label);
                        }
                    }
                    InputMode::EventFilter(field) => {
                        let text = self.input_buffer.trim().to_string();
                        match field {
                            FilterField::Key => self.event_filters.key_filter = text,
                            FilterField::Type => self.event_filters.type_filter = text,
                            FilterField::Fuzzy => self.event_filters.fuzzy_filter = text,
                            FilterField::Exact => self.event_filters.exact_filter = text,
                            FilterField::Substring => self.event_filters.substring_filter = text,
                        }
                        self.commit_filter_change(FilterOrigin::TypedInput);
                    }
                    InputMode::TypesFilter => {
                        self.types_filter = self.input_buffer.trim().to_string();
                        self.type_index = 0;
                        self.path_index = 0;
                        self.types_path_focus = false;
                        self.mark_dirty();
                    }
                    InputMode::RenameType => {
                        let visible = self.visible_types();
                        if let Some(type_id) = visible.get(self.type_index) {
                            let type_id = type_id.clone();
                            let old_name = self.model.canonical_type_name(&type_id);
                            self.model.rename_type(&type_id, self.input_buffer.clone());
                            let new_name = self.model.canonical_type_name(&type_id);
                            let renames = vec![(old_name, new_name)];
                            rewrite_filter_terms_for_renames(
                                &renames,
                                &mut self.event_filters.type_filter,
                                self.stashed_event_filters
                                    .as_mut()
                                    .map(|f| &mut f.type_filter),
                                &mut self.types_filter,
                            );
                            self.user_renamed_types.insert(type_id);
                            self.mark_live_cache_dirty();
                            // The rename itself is shared; the side-effect on
                            // event_filters/stashed_event_filters touches local fields.
                            self.mark_dirty();
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
                    InputMode::MergeTypes => {
                        let label = self.input_buffer.clone();
                        self.finalize_merge_with_label(label);
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

    pub fn filters_working(&self) -> bool {
        self.live_cache_dirty && self.event_filters.has_active() && !self.loading_locked()
    }

    pub fn displayed_event_filters(&self) -> &DataFilters {
        self.stashed_event_filters
            .as_ref()
            .unwrap_or(&self.event_filters)
    }

    fn toggle_event_filters_enabled(&mut self) {
        let anchor = if self.mode == UiMode::Live {
            self.live_anchor_at(self.live_event_index)
        } else {
            None
        };
        if let Some(saved) = self.stashed_event_filters.take() {
            self.event_filters = saved;
            self.mark_dirty();
            let live_count = self.model.events.len();
            let baseline_count = self.baseline_events.len();
            let restored = self
                .stashed_live_visible_indices
                .as_ref()
                .map(|(n, _)| *n == live_count)
                .unwrap_or(false)
                && self
                    .stashed_baseline_visible_indices
                    .as_ref()
                    .map(|(n, _)| *n == baseline_count)
                    .unwrap_or(false);
            if restored {
                self.live_visible_indices = self.stashed_live_visible_indices.take().unwrap().1;
                self.baseline_visible_indices =
                    self.stashed_baseline_visible_indices.take().unwrap().1;
                self.live_cache_dirty = false;
                self.baseline_cache_dirty = false;
            } else {
                self.stashed_live_visible_indices = None;
                self.stashed_baseline_visible_indices = None;
                self.mark_live_cache_dirty();
            }
            self.after_filter_change(anchor);
            self.status = "Event filters restored".to_string();
            return;
        }

        if !self.event_filters.has_active() {
            self.status = "No active event filters to suspend".to_string();
            return;
        }

        self.stashed_live_visible_indices = Some((
            self.model.events.len(),
            std::mem::take(&mut self.live_visible_indices),
        ));
        self.stashed_baseline_visible_indices = Some((
            self.baseline_events.len(),
            std::mem::take(&mut self.baseline_visible_indices),
        ));
        self.stashed_event_filters = Some(self.event_filters.clone());
        self.event_filters = DataFilters::default();
        self.mark_live_cache_dirty();
        self.mark_dirty();
        self.after_filter_change(anchor);
        self.status = "Event filters suspended (press y to restore)".to_string();
    }

    fn apply_profile_filters(&mut self, filters: DataFilters) {
        if filters.has_active() {
            self.stashed_event_filters = None;
            self.stashed_live_visible_indices = None;
            self.stashed_baseline_visible_indices = None;
            self.event_filters = filters;
            self.mark_live_cache_dirty();
            self.mark_dirty();
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
                merge_groups: saved.merge_groups.clone(),
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
            merge_groups: self.model.merge_groups.values().cloned().collect(),
        };
        profile_fingerprint(&profile)
    }

    fn has_nonempty_profile_state(&self) -> bool {
        self.event_filters.has_active()
            || !self.model.renamed_types().is_empty()
            || self.model.types.values().any(|tp| tp.known_unrelated)
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
            UiMode::Values => self.navigate_values(intent),
        }
    }

    fn navigate_live(&mut self, intent: NavIntent) {
        if self.live_key_focus {
            let key_count = self.live_selected_key_paths().len();
            let mut state = self.live_json_focus_state();
            match state.handle_nav(intent, key_count) {
                JsonFocusNav::Consumed => {
                    self.set_live_json_focus_state(state);
                    return;
                }
                JsonFocusNav::ExitFocus => {
                    self.exit_live_key_focus();
                    return;
                }
                JsonFocusNav::EnterValueFocus => {
                    if self.selected_live_value_token().is_some() {
                        state.value_focus = true;
                        self.set_live_json_focus_state(state);
                    } else {
                        self.status = "Selected path has no value".to_string();
                    }
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
                        let key_count = self.period_selected_key_paths().len();
                        let mut state = self.period_json_focus_state();
                        match state.handle_nav(intent, key_count) {
                            JsonFocusNav::Consumed => {
                                self.set_period_json_focus_state(state);
                                PeriodsFocus::Json
                            }
                            JsonFocusNav::ExitFocus => PeriodsFocus::Events,
                            JsonFocusNav::EnterValueFocus => PeriodsFocus::Json,
                        }
                    }
                };
            }
            NavIntent::Right => {
                if self.periods_focus == PeriodsFocus::Json {
                    let key_count = self.period_selected_key_paths().len();
                    let mut state = self.period_json_focus_state();
                    match state.handle_nav(intent, key_count) {
                        JsonFocusNav::EnterValueFocus => {
                            if self.selected_period_value_token().is_some() {
                                state.value_focus = true;
                                self.set_period_json_focus_state(state);
                            } else {
                                self.status = "Selected path has no value".to_string();
                            }
                        }
                        JsonFocusNav::Consumed | JsonFocusNav::ExitFocus => {
                            self.set_period_json_focus_state(state);
                        }
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
                    let key_count = self.period_selected_key_paths().len();
                    let mut state = self.period_json_focus_state();
                    let _ = state.handle_nav(intent, key_count);
                    self.set_period_json_focus_state(state);
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
                    let key_count = self.period_selected_key_paths().len();
                    let mut state = self.period_json_focus_state();
                    let _ = state.handle_nav(intent, key_count);
                    self.set_period_json_focus_state(state);
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
                    let key_count = self.period_selected_key_paths().len();
                    let mut state = self.period_json_focus_state();
                    let _ = state.handle_nav(intent, key_count);
                    self.set_period_json_focus_state(state);
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
                    let key_count = self.period_selected_key_paths().len();
                    let mut state = self.period_json_focus_state();
                    let _ = state.handle_nav(intent, key_count);
                    self.set_period_json_focus_state(state);
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
                    let key_count = self.period_selected_key_paths().len();
                    let mut state = self.period_json_focus_state();
                    let _ = state.handle_nav(intent, key_count);
                    self.set_period_json_focus_state(state);
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
                    let key_count = self.period_selected_key_paths().len();
                    let mut state = self.period_json_focus_state();
                    let _ = state.handle_nav(intent, key_count);
                    self.set_period_json_focus_state(state);
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
        if self.data_key_focus {
            let key_count = self.data_selected_key_paths().len();
            let mut state = self.data_json_focus_state();
            match state.handle_nav(intent, key_count) {
                JsonFocusNav::Consumed => {
                    self.set_data_json_focus_state(state);
                    return;
                }
                JsonFocusNav::ExitFocus => {
                    self.exit_data_key_focus();
                    return;
                }
                JsonFocusNav::EnterValueFocus => {
                    if self.selected_data_value_token().is_some() {
                        state.value_focus = true;
                        self.set_data_json_focus_state(state);
                    } else {
                        self.status = "Selected path has no value".to_string();
                    }
                    return;
                }
            }
        }
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
            NavIntent::Left => {
                self.exit_data_key_focus();
                return;
            }
            NavIntent::Right => {
                if self.data_selected_key_paths().is_empty() {
                    self.status = "Selected event has no keys".to_string();
                    self.data_key_focus = false;
                } else {
                    self.enter_data_key_focus();
                }
                return;
            }
        };
        self.data_value_focus = false;
        self.clamp_data_key_selection();
    }

    fn live_selected_event(&self) -> Option<&EventRecord> {
        self.live_event_at_visible_index(self.live_event_index)
    }

    pub fn live_selected_key_paths(&self) -> Vec<String> {
        let Some(event) = self.live_selected_event() else {
            return Vec::new();
        };
        let paths = collect_indexed_paths(&event.obj);
        filter_paths_by_collapsed(paths, self.collapsed_paths.get(&event.type_id))
    }

    /// Collect unique values for `values_key` across all currently filtered events.
    /// Returns (display_str, filter_token, count) sorted by count descending.
    /// When opened from Baseline view, aggregates over baseline events instead of live.
    fn ensure_values_cache(&mut self) {
        use std::collections::HashMap;
        if self.values_cache.is_none() {
            let mut counts: HashMap<String, (String, usize)> = HashMap::new();
            if self.values_return_mode == UiMode::Data {
                self.ensure_baseline_cache();
                let events: Vec<&EventRecord> = self.visible_baseline_events();
                for e in events {
                    for v in values_at_path(&e.obj, &self.values_key) {
                        let token = value_token(v);
                        let display = v.to_string();
                        let entry = counts.entry(token.clone()).or_insert((display, 0));
                        entry.1 += 1;
                    }
                }
            } else {
                for e in self.model.filtered_events(&self.event_filters) {
                    for v in values_at_path(&e.obj, &self.values_key) {
                        let token = value_token(v);
                        let display = v.to_string();
                        let entry = counts.entry(token.clone()).or_insert((display, 0));
                        entry.1 += 1;
                    }
                }
            }
            let mut result: Vec<(String, String, usize)> = counts
                .into_iter()
                .map(|(token, (display, count))| (display, token, count))
                .collect();
            result.sort_by(|a, b| b.2.cmp(&a.2).then_with(|| a.0.cmp(&b.0)));
            self.values_cache = Some(result);
        }
    }

    pub fn collect_key_values(&mut self) -> &[(String, String, usize)] {
        self.ensure_values_cache();
        self.values_cache.as_deref().unwrap_or(&[])
    }

    pub fn cached_key_values(&self) -> &[(String, String, usize)] {
        self.values_cache.as_deref().unwrap_or(&[])
    }

    fn enter_values_mode_for_key(&mut self, key: String, return_mode: UiMode) {
        self.values_key = normalize_path(&key);
        self.values_index = 0;
        self.values_return_mode = return_mode;
        self.values_cache = None;
        self.mode = UiMode::Values;
        let count = self.collect_key_values().len();
        self.status = format!("{} unique values for '{}'", count, self.values_key);
    }

    fn return_from_values_browser(&mut self) {
        self.mode = self.values_return_mode;
        match self.values_return_mode {
            UiMode::Live => {
                self.exit_live_key_focus();
                self.live_key_focus = true;
                self.clamp_live_key_selection();
            }
            UiMode::Periods => {
                self.period_value_focus = false;
                self.periods_focus = PeriodsFocus::Json;
            }
            UiMode::Data => {
                self.exit_data_key_focus();
                self.data_key_focus = true;
                self.clamp_data_key_selection();
            }
            UiMode::Types | UiMode::Values => {}
        }
    }

    fn enter_values_from_live(&mut self) {
        let keys = self.live_selected_key_paths();
        let Some(key) = keys.get(self.live_key_index).cloned() else {
            self.status = "No key selected".to_string();
            return;
        };
        self.enter_values_mode_for_key(key, UiMode::Live);
    }

    fn enter_values_from_periods(&mut self) {
        let keys = self.period_selected_key_paths();
        let Some(key) = keys.get(self.period_json_key_index).cloned() else {
            self.status = "No key selected".to_string();
            return;
        };
        self.enter_values_mode_for_key(key, UiMode::Periods);
    }

    fn enter_values_from_data(&mut self) {
        let keys = self.data_selected_key_paths();
        let Some(key) = keys.get(self.data_key_index).cloned() else {
            self.status = "No key selected".to_string();
            return;
        };
        self.enter_values_mode_for_key(key, UiMode::Data);
    }

    fn apply_values_selection(&mut self) {
        self.ensure_values_cache();
        let entries = self.cached_key_values();
        let Some((_, token, _)) = entries.get(self.values_index) else {
            return;
        };
        let token = token.clone();
        let key = self.values_key.clone();
        self.apply_exact_filter_toggle(&key, &token);
    }

    fn navigate_values(&mut self, intent: NavIntent) {
        self.ensure_values_cache();
        let count = self.cached_key_values().len();
        match intent {
            NavIntent::LineUp => self.values_index = self.values_index.saturating_sub(1),
            NavIntent::LineDown => {
                if self.values_index + 1 < count {
                    self.values_index += 1;
                }
            }
            NavIntent::Home => self.values_index = 0,
            NavIntent::End => self.values_index = count.saturating_sub(1),
            NavIntent::PageUp => {
                self.values_index = self.values_index.saturating_sub(MENU_PAGE_STEP)
            }
            NavIntent::PageDown => {
                self.values_index =
                    (self.values_index + MENU_PAGE_STEP).min(count.saturating_sub(1))
            }
            NavIntent::Left | NavIntent::Right => {}
        }
    }

    fn live_json_focus_state(&self) -> JsonFocusState {
        JsonFocusState {
            key_index: self.live_key_index,
            value_focus: self.live_value_focus,
        }
    }

    fn set_live_json_focus_state(&mut self, state: JsonFocusState) {
        self.live_key_index = state.key_index;
        self.live_value_focus = state.value_focus;
    }

    fn data_json_focus_state(&self) -> JsonFocusState {
        JsonFocusState {
            key_index: self.data_key_index,
            value_focus: self.data_value_focus,
        }
    }

    fn set_data_json_focus_state(&mut self, state: JsonFocusState) {
        self.data_key_index = state.key_index;
        self.data_value_focus = state.value_focus;
    }

    fn period_json_focus_state(&self) -> JsonFocusState {
        JsonFocusState {
            key_index: self.period_json_key_index,
            value_focus: self.period_value_focus,
        }
    }

    fn set_period_json_focus_state(&mut self, state: JsonFocusState) {
        self.period_json_key_index = state.key_index;
        self.period_value_focus = state.value_focus;
    }

    fn clamp_live_key_selection(&mut self) {
        let key_count = self.live_selected_key_paths().len();
        let mut state = self.live_json_focus_state();
        state.clamp(key_count);
        self.set_live_json_focus_state(state);
        if key_count == 0 {
            self.live_key_index = 0;
            self.live_key_focus = false;
            return;
        }
    }

    fn set_ui_mode(&mut self, mode: UiMode) {
        if self.mode == UiMode::Types && mode != UiMode::Types {
            // Drop pending merge selection when leaving the Types view.
            self.selected_type_ids.clear();
        }
        self.mode = mode;
        self.return_to_live_object_on_types_esc = false;
        self.return_to_types_on_live_esc = false;
        self.types_path_focus = false;
        self.type_preview_open = false;
        self.periods_focus = PeriodsFocus::Periods;
        self.period_value_focus = false;
        self.data_key_focus = false;
        self.data_value_focus = false;
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
        let mut state = self.live_json_focus_state();
        state.enter();
        self.set_live_json_focus_state(state);
        self.live_key_focus = true;
    }

    fn apply_live_selected_key_filter(&mut self) {
        let keys = self.live_selected_key_paths();
        if let Some(path) = keys.get(self.live_key_index) {
            if is_scalar_array_item_path(path) {
                if let Some(token) = self.selected_live_value_token() {
                    self.apply_exact_filter_toggle(path, &token);
                } else {
                    self.status = "Selected path has no value".to_string();
                }
            } else {
                self.apply_key_filter_in_place(path);
            }
        }
    }

    fn apply_period_selected_key_filter(&mut self) {
        let keys = self.period_selected_key_paths();
        if let Some(path) = keys.get(self.period_json_key_index) {
            if is_scalar_array_item_path(path) {
                if let Some(token) = self.selected_period_value_token() {
                    self.apply_exact_filter_toggle(path, &token);
                } else {
                    self.status = "Selected path has no value".to_string();
                }
            } else {
                self.apply_key_filter_in_place(path);
            }
        } else {
            self.status = "Selected event has no keys".to_string();
        }
    }

    fn apply_data_selected_key_filter(&mut self) {
        let keys = self.data_selected_key_paths();
        if let Some(path) = keys.get(self.data_key_index) {
            if is_scalar_array_item_path(path) {
                if let Some(token) = self.selected_data_value_token() {
                    self.apply_exact_filter_toggle(path, &token);
                } else {
                    self.status = "Selected path has no value".to_string();
                }
            } else {
                self.apply_key_filter_in_place(path);
            }
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
        self.apply_exact_filter_toggle(path, &token);
    }

    fn apply_data_selected_value_filter(&mut self) {
        let keys = self.data_selected_key_paths();
        let Some(path) = keys.get(self.data_key_index) else {
            return;
        };
        let Some(token) = self.selected_data_value_token() else {
            self.status = "Selected path has no value".to_string();
            return;
        };
        self.apply_exact_filter_toggle(path, &token);
    }

    fn exit_live_key_focus(&mut self) {
        let was_focus = self.live_key_focus;
        let mut state = self.live_json_focus_state();
        state.exit();
        self.set_live_json_focus_state(state);
        self.live_key_focus = false;
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
        match self.mode {
            UiMode::Live => {
                self.refresh_live_position();
                if !self.live_follow {
                    self.pending_live_anchor = selected_anchor;
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
                    self.data_key_index = 0;
                    self.data_key_focus = false;
                    self.data_value_focus = false;
                } else {
                    self.data_index = self.data_index.min(n.saturating_sub(1));
                    self.clamp_data_key_selection();
                }
            }
            UiMode::Types => {}
            UiMode::Values => {
                self.ensure_values_cache();
                let n = self.cached_key_values().len();
                self.values_index = if n == 0 {
                    0
                } else {
                    self.values_index.min(n - 1)
                };
            }
        }
    }

    fn apply_key_filter_in_place(&mut self, path: &str) {
        let logical_path = normalize_path(path);
        let selected_anchor = if self.mode == UiMode::Live {
            self.live_anchor_at(self.live_event_index)
        } else {
            None
        };
        if self.event_filters.key_filter == logical_path {
            self.event_filters.key_filter.clear();
            self.status = format!("Removed key filter: {}", logical_path);
        } else {
            self.event_filters.key_filter = logical_path.clone();
            self.status = format!("Applied key filter: {}", logical_path);
        }
        self.commit_filter_change(FilterOrigin::KeyShortcut {
            anchor: selected_anchor,
        });
    }

    fn apply_live_selected_value_filter(&mut self) {
        let keys = self.live_selected_key_paths();
        let Some(path) = keys.get(self.live_key_index) else {
            return;
        };
        let Some(token) = self.selected_live_value_token() else {
            self.status = "Selected path has no value".to_string();
            return;
        };
        self.apply_exact_filter_toggle(path, &token);
    }

    fn live_selected_path_prefers_exact_filter(&self) -> bool {
        self.live_selected_key_paths()
            .get(self.live_key_index)
            .map(|path| is_scalar_array_item_path(path))
            .unwrap_or(false)
    }

    fn period_selected_path_prefers_exact_filter(&self) -> bool {
        self.period_selected_key_paths()
            .get(self.period_json_key_index)
            .map(|path| is_scalar_array_item_path(path))
            .unwrap_or(false)
    }

    fn data_selected_path_prefers_exact_filter(&self) -> bool {
        self.data_selected_key_paths()
            .get(self.data_key_index)
            .map(|path| is_scalar_array_item_path(path))
            .unwrap_or(false)
    }

    fn apply_exact_filter_toggle(&mut self, path: &str, token: &str) {
        let exact = format!("{}={}", normalize_path(path), token);
        let anchor = if self.mode == UiMode::Live {
            self.live_anchor_at(self.live_event_index)
        } else {
            None
        };
        if self.event_filters.exact_filter == exact {
            self.event_filters.exact_filter.clear();
            self.status = format!("Removed exact filter: {}", exact);
        } else {
            self.event_filters.exact_filter = exact.clone();
            self.status = format!("Applied exact filter: {}", exact);
        }
        self.commit_filter_change(FilterOrigin::KeyShortcut { anchor });
    }

    fn commit_filter_change(&mut self, origin: FilterOrigin) {
        self.stashed_event_filters = None;
        self.stashed_live_visible_indices = None;
        self.stashed_baseline_visible_indices = None;
        // Filters live in local state.
        self.mark_dirty();
        match origin {
            FilterOrigin::KeyShortcut { anchor } => {
                self.mark_live_cache_dirty();
                self.after_filter_change(anchor);
            }
            FilterOrigin::TypedInput | FilterOrigin::TypeView => {
                self.mark_live_cache_dirty();
                self.data_index = 0;
                self.live_event_index = 0;
                self.period_event_index = 0;
                self.refresh_live_position();
            }
        }
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

    fn selected_data_value_token(&self) -> Option<String> {
        let event = self.selected_data_event()?;
        let key = self
            .data_selected_key_paths()
            .get(self.data_key_index)?
            .clone();
        value_at_path(&event.obj, &key).map(value_token)
    }

    fn jump_to_type_id(&mut self, type_id: String, set_return_to_live_esc: bool) {
        // If the type is filtered out by types_filter, clear the filter so it becomes visible.
        if !self.visible_types().contains(&type_id) && self.model.types.contains_key(&type_id) {
            self.types_filter.clear();
            self.type_index = 0;
        }
        if let Some(idx) = self
            .visible_types()
            .iter()
            .position(|candidate| candidate == &type_id)
        {
            let type_name = self.model.type_display_name(&type_id);
            self.mode = UiMode::Types;
            self.return_to_types_on_live_esc = false;
            self.type_index = idx;
            self.path_index = 0;
            self.types_path_focus = false;
            self.live_key_focus = false;
            self.live_value_focus = false;
            self.data_key_focus = false;
            self.data_value_focus = false;
            self.return_to_live_object_on_types_esc = set_return_to_live_esc;
            self.status = format!("Jumped to type {}", type_name);
        } else {
            self.status = format!("Type not found: {}", self.model.type_display_name(&type_id));
        }
    }

    fn jump_to_live_selected_event_type(&mut self) {
        let Some(event) = self.live_selected_event() else {
            return;
        };
        self.jump_to_type_id(event.type_id.clone(), true);
    }

    fn jump_to_period_selected_event_type(&mut self) {
        let Some(event) = self.selected_period_event() else {
            return;
        };
        self.jump_to_type_id(event.type_id.clone(), false);
    }

    fn jump_to_data_selected_event_type(&mut self) {
        let Some(event) = self.selected_data_event() else {
            return;
        };
        self.jump_to_type_id(event.type_id.clone(), false);
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
        self.mark_dirty();
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

    fn mark_live_cache_dirty(&mut self) {
        self.live_cache_dirty = true;
        self.baseline_cache_dirty = true;
        self.values_cache = None;
        // Intentionally does not mark *any* persisted state dirty. This helper
        // is called both on pure ingest (no state mutation) and from sites
        // that also mutate state — those sites now mark the right flag
        // explicitly. Leaving this transitive call would force a write on
        // every batch ingested.
    }

    /// Marks fields persisted in the shared file (periods, renames, normalized
    /// field overrides, triage set) as needing a write to `<sha>.shared.json`.
    ///
    /// Shared writes are flushed eagerly — they're tiny (sub-10 KB) and other
    /// Single dirty mark. State is small, single-writer, and written
    /// atomically, so we persist eagerly: instant durability without any
    /// reload-from-disk feedback loop.
    fn mark_dirty(&mut self) {
        self.state_dirty = true;
        if !self.reader.source_exists() {
            return;
        }
        match self.persist_state() {
            Ok(()) => self.state_dirty = false,
            Err(err) => {
                // Keep the dirty flag set so the next autosave retries.
                eprintln!("{WARNING_PREFIX_ORANGE} state write failed: {err}");
            }
        }
    }

    fn rebuild_live_cache_if_needed(&mut self) {
        if self.loading_locked() {
            return;
        }
        if !self.live_cache_dirty {
            return;
        }
        let base = self.model.filtered_event_indices(&self.event_filters, None);
        self.live_visible_indices =
            self.apply_whitelist_to_indices(self.model.events.len(), base, None, |idx| {
                self.model.events.get(idx)
            });
        self.live_cache_dirty = false;
    }

    pub fn ensure_live_cache(&mut self) {
        self.rebuild_live_cache_if_needed();
    }

    fn apply_pending_live_anchor(&mut self) {
        let Some(anchor) = self.pending_live_anchor.take() else {
            return;
        };
        if let Some(idx) = self.find_live_index(&anchor) {
            self.live_event_index = idx;
            let total = self.live_visible_total();
            self.ensure_live_selection_visible_n(total);
        } else {
            self.live_event_index = 0;
            self.live_view_start = 0;
        }
        self.clamp_live_key_selection();
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
        self.baseline_visible_indices =
            self.apply_whitelist_to_indices(self.baseline_events.len(), base, None, |idx| {
                self.baseline_events.get(idx)
            });
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

    fn live_anchor_at(&self, index: usize) -> Option<LiveAnchor> {
        let event_idx = *self.live_visible_indices.get(index)?;
        Some(LiveAnchor { event_idx })
    }

    fn find_live_index(&self, anchor: &LiveAnchor) -> Option<usize> {
        // Match by absolute event_idx — unique even when many events
        // share the same millisecond timestamp and type id.
        self.live_visible_indices
            .iter()
            .position(|&event_idx| event_idx == anchor.event_idx)
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
            let indices = self.apply_whitelist_to_indices(
                self.model.events.len(),
                base,
                Some((start, end)),
                |idx| self.model.events.get(idx),
            );
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

    pub fn is_event_triaged(&self, idx: usize) -> bool {
        self.triaged_event_indices.contains(&idx)
    }

    fn toggle_triage_period_event(&mut self) {
        let event_idx = self
            .visible_period_event_rows()
            .get(self.period_event_index)
            .map(|(idx, _)| *idx);
        if let Some(idx) = event_idx {
            if self.triaged_event_indices.contains(&idx) {
                self.triaged_event_indices.remove(&idx);
            } else {
                self.triaged_event_indices.insert(idx);
            }
            self.mark_dirty();
        }
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
                let default = default_type_label(type_id).to_lowercase();
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
        self.show_status_debug || self.loading_locked() || !self.initial_load_complete
    }

    pub fn has_modal_confirmation(&self) -> bool {
        self.pending_delete_period_id.is_some()
            || self.pending_profile_override.is_some()
            || self.pending_unmerge_group_id.is_some()
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
        if let Some(group_id) = self.pending_unmerge_group_id.as_ref() {
            let label = self.model.canonical_type_name(group_id);
            let n = self
                .model
                .merge_groups
                .get(group_id)
                .map(|g| g.members.len())
                .unwrap_or(0);
            return Some(ModalConfirmation {
                title: "Unmerge Type".to_string(),
                lines: vec![
                    "`y` to unmerge, `n`/`Esc` to disregard".to_string(),
                    "".to_string(),
                    format!("Unmerge '{label}' back into its {n} member types?"),
                    "Member-level type filters will be restored from the saved label.".to_string(),
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
        let paths = collect_indexed_paths(&event.obj);
        filter_paths_by_collapsed(paths, self.collapsed_paths.get(&event.type_id))
    }

    pub fn selected_data_event(&self) -> Option<&EventRecord> {
        let event_idx = *self.baseline_visible_indices.get(self.data_index)?;
        self.baseline_events.get(event_idx)
    }

    pub fn data_selected_key_paths(&self) -> Vec<String> {
        let Some(event) = self.selected_data_event() else {
            return Vec::new();
        };
        let paths = collect_indexed_paths(&event.obj);
        filter_paths_by_collapsed(paths, self.collapsed_paths.get(&event.type_id))
    }

    pub fn baseline_tab_enabled(&self) -> bool {
        self.baseline_tab_enabled
    }

    pub fn collapsed_paths_for_type(&self, type_id: &str) -> Option<&HashSet<String>> {
        self.collapsed_paths.get(type_id)
    }

    fn toggle_collapse(&mut self, type_id: &str, path: &str, container: bool) {
        if path.is_empty() || !container {
            return;
        }
        let set = self.collapsed_paths.entry(type_id.to_string()).or_default();
        if !set.insert(path.to_string()) {
            set.remove(path);
            if set.is_empty() {
                self.collapsed_paths.remove(type_id);
            }
        }
    }

    fn toggle_collapse_live(&mut self) {
        let Some(event) = self.live_selected_event() else {
            return;
        };
        let type_id = event.type_id.clone();
        let paths = collect_indexed_paths(&event.obj);
        let visible = filter_paths_by_collapsed(paths.clone(), self.collapsed_paths.get(&type_id));
        let Some(path) = visible.get(self.live_key_index).cloned() else {
            return;
        };
        let container = path_value_is_container(&event.obj, &path);
        self.toggle_collapse(&type_id, &path, container);
        if container {
            self.reanchor_live_key_index(&path);
        }
    }

    fn toggle_collapse_period(&mut self) {
        let Some(event) = self.selected_period_event() else {
            return;
        };
        let type_id = event.type_id.clone();
        let paths = collect_indexed_paths(&event.obj);
        let visible = filter_paths_by_collapsed(paths.clone(), self.collapsed_paths.get(&type_id));
        let Some(path) = visible.get(self.period_json_key_index).cloned() else {
            return;
        };
        let container = path_value_is_container(&event.obj, &path);
        self.toggle_collapse(&type_id, &path, container);
        if container {
            self.reanchor_period_key_index(&path);
        }
    }

    fn toggle_collapse_data(&mut self) {
        let Some(event) = self.selected_data_event() else {
            return;
        };
        let type_id = event.type_id.clone();
        let paths = collect_indexed_paths(&event.obj);
        let visible = filter_paths_by_collapsed(paths.clone(), self.collapsed_paths.get(&type_id));
        let Some(path) = visible.get(self.data_key_index).cloned() else {
            return;
        };
        let container = path_value_is_container(&event.obj, &path);
        self.toggle_collapse(&type_id, &path, container);
        if container {
            self.reanchor_data_key_index(&path);
        }
    }

    fn reanchor_live_key_index(&mut self, path: &str) {
        let paths = self.live_selected_key_paths();
        if let Some(idx) = paths.iter().position(|p| p == path) {
            self.live_key_index = idx;
        }
    }

    fn reanchor_period_key_index(&mut self, path: &str) {
        let paths = self.period_selected_key_paths();
        if let Some(idx) = paths.iter().position(|p| p == path) {
            self.period_json_key_index = idx;
        }
    }

    fn reanchor_data_key_index(&mut self, path: &str) {
        let paths = self.data_selected_key_paths();
        if let Some(idx) = paths.iter().position(|p| p == path) {
            self.data_key_index = idx;
        }
    }

    pub fn type_excluded_by_type_filter(&self, type_id: &str) -> bool {
        type_is_negated_in_filter(
            &self.event_filters.type_filter,
            &self.model.canonical_type_name(type_id),
        )
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
        let obj = serde_json::to_string(&event.obj)
            .unwrap_or_default()
            .to_lowercase();
        self.whitelist_terms
            .iter()
            .any(|needle| obj.contains(needle))
    }

    fn apply_whitelist_to_indices<'a, F>(
        &self,
        source_len: usize,
        indices: Vec<usize>,
        range: Option<(f64, f64)>,
        mut event_at: F,
    ) -> Vec<usize>
    where
        F: FnMut(usize) -> Option<&'a EventRecord>,
    {
        let descending = indices
            .windows(2)
            .find_map(|window| (window[0] != window[1]).then_some(window[0] > window[1]))
            .unwrap_or(false);
        let source_indices: Box<dyn Iterator<Item = usize>> = if descending {
            Box::new((0..source_len).rev())
        } else {
            Box::new(0..source_len)
        };
        match self.whitelist_mode {
            WhitelistMode::Off => indices,
            WhitelistMode::AlwaysShow => {
                let mut seen: StdHashSet<usize> = StdHashSet::with_capacity(indices.len());
                let mut out = Vec::with_capacity(indices.len());
                for idx in indices {
                    if seen.insert(idx) {
                        out.push(idx);
                    }
                }
                for idx in source_indices {
                    let Some(event) = event_at(idx) else {
                        continue;
                    };
                    if let Some((start, end)) = range {
                        if event.ts < start || event.ts > end {
                            continue;
                        }
                    }
                    if self.event_matches_whitelist(event) && seen.insert(idx) {
                        out.push(idx);
                    }
                }
                out
            }
            WhitelistMode::OnlyWhitelist => source_indices
                .filter(|idx| {
                    let Some(e) = event_at(*idx) else {
                        return false;
                    };
                    if let Some((start, end)) = range {
                        if e.ts < start || e.ts > end {
                            return false;
                        }
                    }
                    self.event_matches_whitelist(e)
                })
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
                    self.mark_dirty();
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
            self.event_filters.type_filter = replace_positive_type_filters(
                &self.event_filters.type_filter,
                &self.model.canonical_type_name(type_id),
            );
            self.mode = UiMode::Live;
            self.return_to_types_on_live_esc = true;
            self.types_path_focus = false;
            self.commit_filter_change(FilterOrigin::TypeView);
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
        let mut state = self.period_json_focus_state();
        state.clamp(key_count);
        self.set_period_json_focus_state(state);
    }

    fn clamp_data_key_selection(&mut self) {
        let key_count = self.data_selected_key_paths().len();
        let mut state = self.data_json_focus_state();
        state.clamp(key_count);
        self.set_data_json_focus_state(state);
        if key_count == 0 {
            self.data_key_index = 0;
            self.data_key_focus = false;
            return;
        }
    }

    fn enter_data_key_focus(&mut self) {
        let mut state = self.data_json_focus_state();
        state.enter();
        self.set_data_json_focus_state(state);
        self.data_key_focus = true;
        self.clamp_data_key_selection();
    }

    fn exit_data_key_focus(&mut self) {
        let mut state = self.data_json_focus_state();
        state.exit();
        self.set_data_json_focus_state(state);
        self.data_key_focus = false;
    }

    fn toggle_data_key_focus(&mut self) {
        self.ensure_baseline_cache();
        self.clamp_data_key_selection();
        if self.data_selected_key_paths().is_empty() {
            self.status = "Selected event has no keys".to_string();
            self.data_key_focus = false;
            return;
        }
        if self.data_key_focus {
            self.exit_data_key_focus();
            self.status = "Baseline JSON keys focus: OFF".to_string();
        } else {
            self.enter_data_key_focus();
            self.status = "Baseline JSON keys focus: ON".to_string();
        }
    }

    fn toggle_known_unrelated(&mut self) {
        if self.mode != UiMode::Types {
            return;
        }
        let visible = self.visible_types();
        if let Some(type_id) = visible.get(self.type_index) {
            let name = self.model.canonical_type_name(type_id);
            let was_excluded = type_is_negated_in_filter(&self.event_filters.type_filter, &name);
            self.event_filters.type_filter =
                toggle_negated_type_in_filter(&self.event_filters.type_filter, &name);
            if was_excluded {
                self.status = format!("Removed negative type filter: {}", name);
            } else {
                self.status = format!("Added negative type filter: {}", name);
            }
            self.commit_filter_change(FilterOrigin::TypeView);
        }
    }

    pub fn is_type_selected_for_merge(&self, type_id: &str) -> bool {
        self.selected_type_ids.contains(type_id)
    }

    fn toggle_type_merge_selection(&mut self) {
        if self.mode != UiMode::Types {
            return;
        }
        let visible = self.visible_types();
        let Some(type_id) = visible.get(self.type_index).cloned() else {
            return;
        };
        if self.selected_type_ids.contains(&type_id) {
            self.selected_type_ids.remove(&type_id);
        } else {
            self.selected_type_ids.insert(type_id);
        }
        let n = self.selected_type_ids.len();
        self.status = if n == 0 {
            "Merge selection cleared".to_string()
        } else if n == 1 {
            "1 type selected for merge (need >=2, press 's' on more rows)".to_string()
        } else {
            format!("{n} types selected for merge (press 'g' to merge)")
        };
    }

    fn begin_merge_or_unmerge(&mut self) {
        if self.mode != UiMode::Types {
            return;
        }
        if self.selected_type_ids.len() >= 2 {
            self.input_mode = crate::tui::InputMode::MergeTypes;
            self.input_buffer.clear();
            self.status = format!(
                "Enter a label for the merged type ({} members). Esc to cancel",
                self.selected_type_ids.len()
            );
            return;
        }
        let visible = self.visible_types();
        if let Some(type_id) = visible.get(self.type_index).cloned() {
            if self.model.merge_groups.contains_key(&type_id) {
                self.pending_unmerge_group_id = Some(type_id.clone());
                let label = self.model.canonical_type_name(&type_id);
                self.status = format!("Unmerge '{label}'? `y` to confirm, `n`/`Esc` to disregard");
                return;
            }
        }
        self.status = "Select >=2 types with 's' to merge, or highlight a merged group to unmerge"
            .to_string();
    }

    fn confirm_unmerge_pending(&mut self) {
        let Some(group_id) = self.pending_unmerge_group_id.take() else {
            return;
        };
        let label = self.model.canonical_type_name(&group_id);
        let Some(group) = self.model.unmerge_group(&group_id) else {
            self.status = format!("Group {group_id} not found");
            return;
        };
        // Rebuild App.baseline_events: restore each event's type_id from
        // original_type_id so member-keyed downstream views are correct.
        for ev in self.baseline_events.iter_mut() {
            if ev.type_id == group_id {
                let restored = ev
                    .original_type_id
                    .take()
                    .unwrap_or_else(|| group_id.clone());
                ev.type_id = restored;
            }
        }
        // Expand filter terms.
        let member_names: Vec<String> = group
            .members
            .iter()
            .zip(group.members_prior_name.iter())
            .map(|(member_id, prior)| {
                prior
                    .clone()
                    .unwrap_or_else(|| default_type_label(member_id))
            })
            .collect();
        self.event_filters.type_filter =
            expand_merged_label_in_filter(&self.event_filters.type_filter, &label, &member_names);
        if let Some(stashed) = self.stashed_event_filters.as_mut() {
            stashed.type_filter =
                expand_merged_label_in_filter(&stashed.type_filter, &label, &member_names);
        }
        self.commit_filter_change(FilterOrigin::TypeView);
        self.pending_live_recompute = true;
        self.mark_dirty();
        self.status = format!("Unmerged '{label}' ({} members)", group.members.len());
    }

    fn cancel_pending_unmerge(&mut self) {
        if self.pending_unmerge_group_id.take().is_some() {
            self.status = "Unmerge cancelled".to_string();
        }
    }

    fn finalize_merge_with_label(&mut self, label: String) {
        let members: Vec<String> = self.selected_type_ids.iter().cloned().collect();
        if members.len() < 2 {
            self.status = "Need at least 2 types selected to merge".to_string();
            self.selected_type_ids.clear();
            return;
        }
        // Capture each member's prior canonical name BEFORE the merge mutates
        // self.model.types — we need them to rewrite the filter.
        let prior_names: Vec<String> = members
            .iter()
            .map(|m| self.model.canonical_type_name(m))
            .collect();
        let cleaned_label = label.trim().to_string();
        let Some(group_id) = self.model.merge_types(&members, cleaned_label.clone()) else {
            self.status = "Merge failed (insufficient valid members)".to_string();
            return;
        };
        // Rewrite the type filter: replace each member's prior name with the
        // merged label, then dedupe.
        for prior in &prior_names {
            self.event_filters.type_filter =
                rename_type_terms_in_filter(&self.event_filters.type_filter, prior, &cleaned_label);
            if let Some(stashed) = self.stashed_event_filters.as_mut() {
                stashed.type_filter =
                    rename_type_terms_in_filter(&stashed.type_filter, prior, &cleaned_label);
            }
        }
        self.event_filters.type_filter = dedupe_filter_terms(&self.event_filters.type_filter);
        if let Some(stashed) = self.stashed_event_filters.as_mut() {
            stashed.type_filter = dedupe_filter_terms(&stashed.type_filter);
        }
        // Rebuild App.baseline_events: any event whose type_id was a merged
        // member now points to the group id.
        let member_set: HashSet<String> = members.iter().cloned().collect();
        for ev in self.baseline_events.iter_mut() {
            if member_set.contains(&ev.type_id) {
                let prev = std::mem::replace(&mut ev.type_id, group_id.clone());
                if ev.original_type_id.is_none() {
                    ev.original_type_id = Some(prev);
                }
            }
        }
        self.commit_filter_change(FilterOrigin::TypeView);
        self.selected_type_ids.clear();
        self.pending_live_recompute = true;
        self.mark_dirty();
        // Point the cursor at the newly created merged group so the user sees
        // the row they just acted on. Path-focus was over a now-gone member,
        // so reset it to the list pane.
        let visible_after = self.visible_types();
        if let Some(idx) = visible_after.iter().position(|id| id == &group_id) {
            self.type_index = idx;
            self.types_path_focus = false;
            self.path_index = 0;
        }
        self.status = format!(
            "Merged {} types into '{}'",
            members.len(),
            if cleaned_label.is_empty() {
                "merged group"
            } else {
                cleaned_label.as_str()
            }
        );
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
            if self.reader.is_http() {
                self.status = "Loading HTTP stream metadata...".to_string();
                return;
            }
            self.initial_load_complete = true;
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

fn validate_type_field(obj: &Value) -> Result<()> {
    let Some(raw) = obj.get("_type") else {
        return Ok(());
    };
    match raw {
        Value::String(s) if !s.trim().is_empty() => Ok(()),
        Value::String(_) => {
            bail!("Unsupported input: `_type` must be a non-empty string when present.")
        }
        _ => bail!("Unsupported input: `_type` must be a string when present."),
    }
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
    profile
        .renames
        .sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    profile.renames.dedup();
    profile.known_unrelated_types.sort();
    profile.known_unrelated_types.dedup();
    profile.normalized_field_overrides.sort_by(|a, b| {
        a.type_id
            .cmp(&b.type_id)
            .then(a.path.cmp(&b.path))
            .then((a.mode as u8).cmp(&(b.mode as u8)))
    });
    profile
        .normalized_field_overrides
        .dedup_by(|a, b| a.type_id == b.type_id && a.path == b.path && a.mode == b.mode);
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
        .merge_groups
        .sort_by(|a, b| a.group_id.cmp(&b.group_id));
    profile
}

fn is_scalar_array_item_path(path: &str) -> bool {
    path.ends_with(']')
}

/// Rewrites filter strings in-place so that terms referencing renamed type
/// display names continue to match. Pure helper (no `App` dependency) so it
/// can be unit-tested directly. `renames` are `(old_name, new_name)` pairs.
fn rewrite_filter_terms_for_renames(
    renames: &[(String, String)],
    event_type_filter: &mut String,
    stashed_type_filter: Option<&mut String>,
    types_filter: &mut String,
) {
    if renames.is_empty() {
        return;
    }
    *event_type_filter = apply_rename_batch_to_filter(event_type_filter, renames);
    if let Some(stashed) = stashed_type_filter {
        *stashed = apply_rename_batch_to_filter(stashed, renames);
    }
    *types_filter = apply_rename_batch_to_filter(types_filter, renames);
}

fn filter_paths_by_collapsed(
    paths: Vec<String>,
    collapsed: Option<&HashSet<String>>,
) -> Vec<String> {
    let Some(collapsed) = collapsed else {
        return paths;
    };
    if collapsed.is_empty() {
        return paths;
    }
    paths
        .into_iter()
        .filter(|p| !has_collapsed_ancestor(p, collapsed))
        .collect()
}

fn has_collapsed_ancestor(path: &str, collapsed: &HashSet<String>) -> bool {
    let bytes = path.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        if c == b'.' || c == b'[' {
            if collapsed.contains(&path[..i]) {
                return true;
            }
        }
        i += 1;
    }
    false
}

fn path_value_is_container(root: &Value, path: &str) -> bool {
    use crate::domain::value_at_path;
    matches!(
        value_at_path(root, path),
        Some(Value::Object(_)) | Some(Value::Array(_))
    )
}

#[cfg(test)]
mod tests {
    use super::{
        normalize_navigation_code, parse_event_timestamp_millis, validate_type_field, App,
        NavIntent, PeriodsFocus, UiMode, MENU_PAGE_STEP,
    };
    use crate::domain::prepare_event;
    use crate::persistence::{SessionEvent, SessionExport, SourceProfile};
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use serde_json::{json, Value};

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

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
    fn validate_type_field_accepts_missing_and_string() {
        assert!(validate_type_field(&json!({"x": 1})).is_ok());
        assert!(validate_type_field(&json!({"_type": "login", "x": 1})).is_ok());
    }

    #[test]
    fn validate_type_field_rejects_non_string_and_empty() {
        for bad in [
            json!({"_type": 42}),
            json!({"_type": true}),
            json!({"_type": null}),
            json!({"_type": ["a"]}),
            json!({"_type": {}}),
            json!({"_type": ""}),
            json!({"_type": "   "}),
        ] {
            assert!(
                validate_type_field(&bad).is_err(),
                "expected error for {bad}"
            );
        }
    }

    #[test]
    fn resolve_event_ts_requires_timestamp_in_live_mode_but_not_offline() {
        let mut app = App::new(
            std::path::PathBuf::from("/tmp/json_demo/stream.jsonl"),
            None,
            false,
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
        app.model
            .ingest(json!({"_timestamp": 1_700_000_000_000u64, "x": 1}), 1.0);
        app.model
            .ingest(json!({"_timestamp": 1_700_000_001_000u64, "x": 2}), 2.0);
        app.model
            .ingest(json!({"_timestamp": 1_700_000_002_000u64, "x": 3}), 3.0);
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
            merge_groups: vec![],
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
            merge_groups: vec![],
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
            merge_groups: vec![],
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

    #[test]
    fn toggle_known_unrelated_marks_selected_type_as_excluded() {
        let mut app = test_app();
        app.model.ingest(json!({"kind": "alpha"}), 1.0);
        app.mode = UiMode::Types;
        app.type_index = 0;

        let visible = app.visible_types();
        let selected = visible[0].clone();
        assert!(!app.type_excluded_by_type_filter(&selected));

        app.toggle_known_unrelated();

        assert!(app.type_excluded_by_type_filter(&selected));
    }

    #[test]
    fn jump_to_live_selected_event_type_clears_filter_if_needed() {
        let mut app = test_app();
        app.model.ingest(json!({"kind": "alpha"}), 1.0);
        app.ensure_live_cache();
        app.mode = UiMode::Live;
        app.live_event_index = 0;
        app.live_key_focus = true;

        let selected_type = app
            .live_selected_event()
            .expect("live event")
            .type_id
            .clone();

        // Even when the types_filter doesn't match, jump clears it and switches to Types.
        app.types_filter = "does-not-match".to_string();
        app.jump_to_live_selected_event_type();
        assert_eq!(app.mode, UiMode::Types);
        assert!(app.types_filter.is_empty());
        assert_eq!(app.visible_types()[app.type_index], selected_type);
    }

    #[test]
    fn positive_type_filter_does_not_mark_other_types_as_u_excluded() {
        let mut app = test_app();
        app.model.ingest(json!({"kind": "alpha", "a": 1}), 1.0);
        app.model.ingest(json!({"kind": "beta", "b": 2}), 2.0);
        app.mode = UiMode::Types;

        let visible = app.visible_types();
        let first = visible[0].clone();
        let second = visible[1].clone();
        app.event_filters.type_filter = app.model.canonical_type_name(&first);

        assert!(!app.type_excluded_by_type_filter(&first));
        assert!(!app.type_excluded_by_type_filter(&second));
    }

    #[test]
    fn type_filter_t_preserves_u_negations_and_overwrites_positive_terms() {
        let mut app = test_app();
        app.model.ingest(json!({"kind": "alpha", "a": 1}), 1.0);
        app.model.ingest(json!({"kind": "beta", "b": 2}), 2.0);
        app.mode = UiMode::Types;
        let visible = app.visible_types();
        let first = visible[0].clone();
        let second = visible[1].clone();

        app.event_filters.type_filter =
            format!("!{} && OldType", app.model.canonical_type_name(&second));
        app.type_index = 0;
        app.apply_selected_type_filter();

        assert!(app
            .event_filters
            .type_filter
            .contains(&app.model.canonical_type_name(&first)));
        assert!(app
            .event_filters
            .type_filter
            .contains(&format!("!{}", app.model.canonical_type_name(&second))));
        assert!(!app.event_filters.type_filter.contains("OldType"));

        app.handle_key(key(KeyCode::Esc));
        assert_eq!(app.mode, UiMode::Types);
        assert_eq!(
            app.event_filters.type_filter,
            format!("!{}", app.model.canonical_type_name(&second))
        );
    }

    #[test]
    fn renaming_filtered_type_updates_active_type_filter() {
        let mut app = test_app();
        app.model.ingest(json!({"kind": "alpha", "a": 1}), 1.0);
        app.mode = UiMode::Types;
        app.type_index = 0;
        let type_id = app.visible_types()[0].clone();
        let old_name = app.model.canonical_type_name(&type_id);
        app.event_filters.type_filter = old_name.clone();
        app.input_mode = crate::tui::InputMode::RenameType;
        app.input_buffer = "Renamed Type".to_string();

        app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

        assert_eq!(app.model.canonical_type_name(&type_id), "Renamed Type");
        assert!(app.event_filters.type_filter.contains("Renamed Type"));
        let filtered = app.model.filtered_events(&app.event_filters);
        assert_eq!(filtered.len(), 1);
    }

    #[test]
    fn live_exact_filter_toggle_round_trips() {
        let mut app = test_app();
        app.model.ingest(json!({"status": "ok"}), 1.0);
        app.ensure_live_cache();
        app.mode = UiMode::Live;
        app.live_event_index = 0;
        app.enter_live_key_focus();

        app.apply_live_selected_value_filter();
        assert_eq!(app.event_filters.exact_filter, "status=s:ok");

        app.apply_live_selected_value_filter();
        assert!(app.event_filters.exact_filter.is_empty());
    }

    #[test]
    fn periods_exact_filter_toggle_round_trips() {
        let mut app = test_app();
        app.model.ingest(json!({"status": "ok"}), 1.0);
        app.model.set_periods(vec![crate::domain::ActionPeriod {
            id: 1,
            label: "p".to_string(),
            start: 1.0,
            end: Some(1.0),
        }]);
        app.mode = UiMode::Periods;
        app.periods_focus = PeriodsFocus::Json;

        app.apply_period_selected_value_filter();
        assert_eq!(app.event_filters.exact_filter, "status=s:ok");

        app.apply_period_selected_value_filter();
        assert!(app.event_filters.exact_filter.is_empty());
    }

    #[test]
    fn values_selection_toggle_round_trips() {
        let mut app = test_app();
        app.model.ingest(json!({"status": "ok"}), 1.0);
        app.mode = UiMode::Live;
        app.ensure_live_cache();
        app.live_event_index = 0;
        app.enter_live_key_focus();
        app.enter_values_from_live();

        app.apply_values_selection();
        assert_eq!(app.event_filters.exact_filter, "status=s:ok");

        app.mode = UiMode::Values;
        app.values_return_mode = UiMode::Live;
        app.values_key = "status".to_string();
        app.values_cache = None;
        app.apply_values_selection();
        assert!(app.event_filters.exact_filter.is_empty());
    }

    #[test]
    fn values_browser_collects_all_array_item_values_for_logical_path() {
        let mut app = test_app();
        app.model.ingest(
            json!({"items":[{"name":"first"},{"name":"second"},{"name":"second"}]}),
            1.0,
        );
        app.values_return_mode = UiMode::Live;
        app.values_key = "items[].name".to_string();
        app.values_cache = None;

        let values = app.collect_key_values();
        assert_eq!(values.len(), 2);
        assert_eq!(values[0].0, "\"second\"");
        assert_eq!(values[0].2, 2);
        assert_eq!(values[1].0, "\"first\"");
        assert_eq!(values[1].2, 1);
    }

    #[test]
    fn enter_on_scalar_array_item_applies_exact_contains_filter() {
        let mut app = test_app();
        app.model.ingest(json!({"items":[1,2,3]}), 1.0);
        app.ensure_live_cache();
        app.mode = UiMode::Live;
        app.live_event_index = 0;
        app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        let keys = app.live_selected_key_paths();
        let idx = keys
            .iter()
            .position(|path| path == "items[0]")
            .expect("items[0] path should exist");
        app.live_key_index = idx;

        app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        assert_eq!(app.event_filters.exact_filter, "items[]=n:1");
        assert!(app.event_filters.key_filter.is_empty());
    }

    #[test]
    fn values_browser_collects_scalar_array_values_of_multiple_types() {
        let mut app = test_app();
        app.model
            .ingest(json!({"items":["alpha", true, null, "alpha"]}), 1.0);
        app.values_return_mode = UiMode::Live;
        app.values_key = "items[]".to_string();
        app.values_cache = None;

        let values = app.collect_key_values();
        assert_eq!(values.len(), 3);
        assert_eq!(values[0].0, "\"alpha\"");
        assert_eq!(values[0].2, 2);
        assert_eq!(values[1].0, "null");
        assert_eq!(values[1].2, 1);
        assert_eq!(values[2].0, "true");
        assert_eq!(values[2].2, 1);
    }

    #[test]
    fn typed_filter_commit_resets_indices_to_top() {
        let mut app = test_app();
        app.model.ingest(json!({"kind": "a"}), 1.0);
        app.model.ingest(json!({"kind": "b"}), 2.0);
        app.live_follow = false;
        app.live_event_index = 1;
        app.period_event_index = 1;
        app.data_index = 1;
        app.input_mode = crate::tui::InputMode::EventFilter(crate::domain::FilterField::Type);
        app.input_buffer = "type".to_string();

        app.handle_input(KeyCode::Enter);

        assert_eq!(app.live_event_index, 0);
        assert_eq!(app.period_event_index, 0);
        assert_eq!(app.data_index, 0);
    }

    #[test]
    fn live_key_filter_shortcut_preserves_selected_anchor() {
        let mut app = test_app();
        app.model.ingest(json!({"a": 1, "keep": true}), 1.0);
        app.model.ingest(json!({"a": 2, "keep": true}), 2.0);
        app.ensure_live_cache();
        app.mode = UiMode::Live;
        app.live_follow = false;
        app.live_event_index = 1;
        app.enter_live_key_focus();

        app.apply_live_selected_key_filter();

        let selected = app.live_selected_event().expect("selected event");
        assert_eq!(selected.ts, 2.0);
    }

    #[test]
    fn clear_filters_clears_event_filters() {
        let mut app = test_app();
        app.event_filters = crate::domain::DataFilters {
            key_filter: "a".to_string(),
            ..crate::domain::DataFilters::default()
        };
        app.live_follow = false;
        app.live_event_index = 3;
        app.period_event_index = 4;
        app.data_index = 5;

        app.handle_key(key(KeyCode::Char('c')));

        // Filters are cleared; period and data indices are left where they were
        // (KeyShortcut origin preserves position rather than resetting to 0).
        assert_eq!(app.event_filters, crate::domain::DataFilters::default());
        assert_eq!(app.period_event_index, 4);
        assert_eq!(app.data_index, 5);
    }

    #[test]
    fn pressing_m_opens_period_and_persist_does_not_drop_it() {
        // Regression for the post-delete `m` flash: in the old shared-state
        // build, deleting a period kept its id in `user_deleted_period_ids`
        // forever; `set_periods` then reset `next_period_id` to max+1 (i.e.
        // the just-deleted id); pressing `m` opened a new period with that
        // same id, and the persist merge dropped it because the id was on
        // the deleted list. The eager-write + watcher-reload loop then
        // observed the open period vanishing and flipped the indicator OFF.
        // Walk through the same sequence and assert the new period
        // survives the persist.
        let mut app = test_app();
        for i in 1..=5u64 {
            app.model.ingest(
                json!({"_timestamp": 1_700_000_000_000u64 + i * 1000}),
                i as f64,
            );
        }
        // Seed two closed periods so we have a non-trivial periods list.
        app.handle_key(key(KeyCode::Char('m')));
        app.model
            .ingest(json!({"_timestamp": 1_700_000_010_000u64}), 10.0);
        app.handle_key(key(KeyCode::Char('m')));
        app.handle_key(key(KeyCode::Char('m')));
        app.model
            .ingest(json!({"_timestamp": 1_700_000_020_000u64}), 20.0);
        app.handle_key(key(KeyCode::Char('m')));
        assert!(
            app.model.active_period().is_none(),
            "setup: both periods closed"
        );
        let last_id = app.model.periods.iter().map(|p| p.id).max().unwrap();

        // Delete the most recent period — the exact scenario that used to
        // poison `user_deleted_period_ids` with the id that the next
        // toggle would reuse.
        app.delete_period_by_id(last_id).expect("delete succeeds");

        // Now press `m`. The new period gets `next_period_id = last_id`,
        // matching the just-deleted id.
        app.handle_key(key(KeyCode::Char('m')));
        assert!(
            app.model.active_period().is_some(),
            "after pressing m post-delete the period should be active"
        );
        let opened_id = app.model.active_period().unwrap().id;
        // Trigger several extra persists to be sure none of them drop it.
        for _ in 0..5 {
            app.mark_dirty();
        }
        assert!(
            app.model.active_period().is_some(),
            "open period must survive repeated persists after a prior delete"
        );
        assert_eq!(app.model.active_period().unwrap().id, opened_id);
    }

    #[test]
    fn esc_from_periods_json_moves_back_to_events() {
        let mut app = test_app();
        app.model.ingest(json!({"status": "ok"}), 1.0);
        app.model.set_periods(vec![crate::domain::ActionPeriod {
            id: 1,
            label: "p".to_string(),
            start: 1.0,
            end: Some(1.0),
        }]);
        app.mode = UiMode::Periods;
        app.periods_focus = PeriodsFocus::Json;

        app.handle_key(key(KeyCode::Esc));
        assert_eq!(app.periods_focus, PeriodsFocus::Events);
    }

    #[test]
    fn esc_from_values_restores_live_key_focus() {
        let mut app = test_app();
        app.model.ingest(json!({"status": "ok"}), 1.0);
        app.ensure_live_cache();
        app.mode = UiMode::Live;
        app.live_event_index = 0;
        app.enter_live_key_focus();
        app.live_key_index = 0;
        app.enter_values_from_live();

        app.handle_key(key(KeyCode::Esc));

        assert_eq!(app.mode, UiMode::Live);
        assert!(app.live_key_focus);
        assert_eq!(app.live_key_index, 0);
    }

    #[test]
    fn baseline_key_focus_navigation_does_not_move_event_row() {
        let mut app = test_app();
        let prepared = prepare_event(json!({"a": 1, "b": 2}));
        app.baseline_events.push(crate::domain::EventRecord {
            ts: 1.0,
            type_id: prepared.type_id.clone(),
            obj: prepared.obj,
            keys: prepared.keys,
            size_bytes: 0,
            action_period_id: None,
            in_action_period: false,
            live_rate_score: 0.0,
            live_uniq_score: 0.0,
            original_type_id: None,
        });
        app.baseline_visible_indices = vec![0];
        app.baseline_cache_dirty = false;
        app.mode = UiMode::Data;
        app.data_index = 0;
        app.toggle_data_key_focus();

        app.navigate_data(NavIntent::LineDown);
        assert_eq!(app.data_index, 0);
        assert_eq!(app.data_key_index, 1);
    }

    #[test]
    fn baseline_value_focus_applies_exact_filter() {
        let mut app = test_app();
        let prepared = prepare_event(json!({"status": "ok"}));
        app.baseline_events.push(crate::domain::EventRecord {
            ts: 1.0,
            type_id: prepared.type_id.clone(),
            obj: prepared.obj,
            keys: prepared.keys,
            size_bytes: 0,
            action_period_id: None,
            in_action_period: false,
            live_rate_score: 0.0,
            live_uniq_score: 0.0,
            original_type_id: None,
        });
        app.baseline_visible_indices = vec![0];
        app.baseline_cache_dirty = false;
        app.mode = UiMode::Data;
        app.data_index = 0;
        app.toggle_data_key_focus();
        app.navigate_data(NavIntent::Right);
        assert!(app.data_value_focus);

        app.apply_data_selected_value_filter();
        assert_eq!(app.event_filters.exact_filter, "status=s:ok");
    }

    #[test]
    fn control_start_stop_are_idempotent() {
        let mut app = test_app();
        app.model
            .ingest(json!({"_timestamp": 1_700_000_000_000u64}), 1.0);

        let started = app.control_start_action(Some("api".to_string()));
        assert_eq!(started.status, 200);
        assert_eq!(started.body["changed"], json!(true));
        assert_eq!(started.body["active"], json!(true));

        let started_again = app.control_start_action(None);
        assert_eq!(started_again.status, 200);
        assert_eq!(started_again.body["changed"], json!(false));
        assert_eq!(started_again.body["active"], json!(true));

        let stopped = app.control_stop_action();
        assert_eq!(stopped.status, 200);
        assert_eq!(stopped.body["changed"], json!(true));
        assert_eq!(stopped.body["active"], json!(false));

        let stopped_again = app.control_stop_action();
        assert_eq!(stopped_again.status, 200);
        assert_eq!(stopped_again.body["changed"], json!(false));
        assert_eq!(stopped_again.body["active"], json!(false));
    }

    #[test]
    fn control_start_stop_rejected_in_offline_mode() {
        let mut app = App::new(
            std::path::PathBuf::from("/tmp/test_app.jsonl"),
            None,
            true,
            false,
            true,
            false,
        );
        let started = app.control_start_action(None);
        assert_eq!(started.status, 409);
        assert_eq!(started.body["ok"], json!(false));

        let stopped = app.control_stop_action();
        assert_eq!(stopped.status, 409);
        assert_eq!(stopped.body["ok"], json!(false));
    }

    #[test]
    fn rewrite_filter_terms_rewrites_event_stashed_and_types_filters() {
        use super::rewrite_filter_terms_for_renames;
        let renames = vec![("OldA".to_string(), "NewA".to_string())];
        let mut event_filter = "OldA && other".to_string();
        let mut stashed = "OldA".to_string();
        let mut types_filter = "OldA".to_string();
        rewrite_filter_terms_for_renames(
            &renames,
            &mut event_filter,
            Some(&mut stashed),
            &mut types_filter,
        );
        assert!(event_filter.contains("NewA"));
        assert!(!event_filter.contains("OldA"));
        assert_eq!(stashed, "NewA");
        assert_eq!(types_filter, "NewA");
    }

    #[test]
    fn rewrite_filter_terms_noop_for_empty_renames() {
        use super::rewrite_filter_terms_for_renames;
        let mut event_filter = "Alpha".to_string();
        let mut types_filter = "Alpha".to_string();
        rewrite_filter_terms_for_renames(&[], &mut event_filter, None, &mut types_filter);
        assert_eq!(event_filter, "Alpha");
        assert_eq!(types_filter, "Alpha");
    }

    #[test]
    fn remote_rename_rewrites_event_and_types_filters() {
        // Simulate the watcher reload path applying a rename pushed by another
        // operator. The model already has the type ingested under its default
        // label; our local event/types filters reference that label.
        let mut app = test_app();
        app.model.ingest(json!({"kind": "alpha", "a": 1}), 1.0);
        let type_id = app.visible_types()[0].clone();
        let old_name = app.model.canonical_type_name(&type_id);
        app.event_filters.type_filter = old_name.clone();
        app.types_filter = old_name.clone();
        app.stashed_event_filters = Some(crate::domain::DataFilters {
            type_filter: old_name.clone(),
            ..crate::domain::DataFilters::default()
        });

        // Mimic what reload_shared_state_from_disk does after pulling the new
        // shared state from disk: apply the rename to the model, then rewrite
        // local filter strings using the (old, new) display-name diff.
        let renames = vec![(type_id.clone(), "Renamed".to_string())];
        app.model.apply_renames(&renames);
        let new_name = app.model.canonical_type_name(&type_id);
        let pairs = vec![(old_name.clone(), new_name.clone())];
        super::rewrite_filter_terms_for_renames(
            &pairs,
            &mut app.event_filters.type_filter,
            app.stashed_event_filters
                .as_mut()
                .map(|f| &mut f.type_filter),
            &mut app.types_filter,
        );

        assert!(app.event_filters.type_filter.contains(&new_name));
        assert!(!app.event_filters.type_filter.contains(&old_name));
        assert_eq!(app.types_filter, new_name);
        assert_eq!(
            app.stashed_event_filters.as_ref().unwrap().type_filter,
            new_name
        );
        // The filter still matches the underlying type.
        let filtered = app.model.filtered_events(&app.event_filters);
        assert_eq!(filtered.len(), 1);
    }

    #[test]
    fn merge_via_s_s_g_enter_label_creates_visible_group() {
        let mut app = test_app();
        app.model.ingest(json!({"event": "login", "x": 1}), 1.0);
        app.model.ingest(json!({"event": "logout", "y": 2}), 2.0);
        app.mode = UiMode::Types;
        // Select first two visible rows.
        let visible = app.visible_types();
        assert!(visible.len() >= 2);
        let a = visible[0].clone();
        let b = visible[1].clone();
        app.type_index = 0;
        app.handle_key(key(KeyCode::Char('s')));
        app.type_index = 1;
        app.handle_key(key(KeyCode::Char('s')));
        assert!(app.selected_type_ids.contains(&a));
        assert!(app.selected_type_ids.contains(&b));
        // Press 'g' to begin merge.
        app.handle_key(key(KeyCode::Char('g')));
        assert_eq!(app.input_mode, crate::tui::InputMode::MergeTypes);
        // Type a label.
        for ch in "Auth".chars() {
            app.handle_key(key(KeyCode::Char(ch)));
        }
        app.handle_key(key(KeyCode::Enter));
        // The merged group should appear in visible_types().
        let visible = app.visible_types();
        let has_group = visible
            .iter()
            .any(|id| app.model.merge_groups.contains_key(id));
        assert!(has_group, "expected a merged group in visible types");
        // Selection cleared.
        assert!(app.selected_type_ids.is_empty());
    }

    #[test]
    fn merge_repositions_cursor_onto_merged_group() {
        // After a successful merge, the cursor should point at the newly
        // created group row rather than staying at the previous numeric index.
        let mut app = test_app();
        // Ingest three distinct shapes so we have stable rows around the
        // merged group.
        app.model.ingest(json!({"event": "login", "x": 1}), 1.0);
        app.model.ingest(json!({"event": "logout", "y": 2}), 2.0);
        app.model.ingest(json!({"event": "ping", "z": 3}), 3.0);
        app.mode = UiMode::Types;
        let visible_before = app.visible_types();
        assert!(visible_before.len() >= 2);
        // Select first two and merge under a recognisable label.
        app.type_index = 0;
        app.handle_key(key(KeyCode::Char('s')));
        app.type_index = 1;
        app.handle_key(key(KeyCode::Char('s')));
        // Pretend the user was in path-focus before pressing g.
        app.types_path_focus = true;
        app.handle_key(key(KeyCode::Char('g')));
        for ch in "Auth".chars() {
            app.handle_key(key(KeyCode::Char(ch)));
        }
        app.handle_key(key(KeyCode::Enter));
        // Cursor should be on the merged group row.
        let visible_after = app.visible_types();
        let cursor_id = &visible_after[app.type_index];
        assert!(
            app.model.merge_groups.contains_key(cursor_id),
            "expected cursor to land on the merged group, got id {cursor_id}"
        );
        // Path-focus is reset so the user is back on the list pane.
        assert!(!app.types_path_focus);
    }

    /// Helper: ingest two distinct shapes into a fresh app and return their
    /// (login, logout) structural type ids.
    fn seed_two_types(app: &mut App) -> (String, String) {
        for _ in 0..3 {
            app.model
                .ingest(json!({"event": "login", "user": "a"}), 1.0);
        }
        for _ in 0..2 {
            app.model
                .ingest(json!({"event": "logout", "session": "z"}), 2.0);
        }
        let login_id = app
            .model
            .events
            .iter()
            .find(|e| e.obj.get("user").is_some())
            .map(|e| e.type_id.clone())
            .expect("login event");
        let logout_id = app
            .model
            .events
            .iter()
            .find(|e| e.obj.get("session").is_some())
            .map(|e| e.type_id.clone())
            .expect("logout event");
        (login_id, logout_id)
    }

    #[test]
    fn profile_import_applies_saved_merge_groups() {
        // Build a model with two ingested types and assemble a profile whose
        // merge_groups list joins them. Importing that profile should produce
        // the merged group in `model.merge_groups` and a corresponding entry
        // in `model.types`.
        let mut app = test_app();
        let (login_id, logout_id) = seed_two_types(&mut app);
        let members = vec![login_id.clone(), logout_id.clone()];
        let mut sorted_members = members.clone();
        sorted_members.sort();
        let group_id = crate::domain::AnalyzerModel::compute_group_id(&sorted_members);
        let group = crate::domain::MergeGroup {
            group_id: group_id.clone(),
            label: "Auth".to_string(),
            members: sorted_members,
            members_prior_name: vec![None, None],
        };
        let profile = SourceProfile {
            renames: vec![],
            known_unrelated_types: vec![],
            normalized_field_overrides: vec![],
            negative_filters: crate::domain::DataFilters::default(),
            whitelist_terms: vec![],
            merge_groups: vec![group],
        };
        app.apply_profile(profile, false);
        assert!(app.model.merge_groups.contains_key(&group_id));
        assert!(app.model.types.contains_key(&group_id));
        assert!(!app.model.types.contains_key(&login_id));
        assert!(!app.model.types.contains_key(&logout_id));
        // Merged count == sum of member counts (3 + 2).
        assert_eq!(app.model.types.get(&group_id).unwrap().count, 5);
    }

    #[test]
    fn profile_import_registers_aliases_when_members_absent() {
        // Members referenced by the profile don't exist in this model's
        // ingested types yet. We can't tell "ghost / handcrafted" apart from
        // "pre-ingest: members will arrive later", so we register the alias
        // hook + seed an empty TypeProfile. This is what makes CLI
        // `--profile` work at boot.
        let mut app = test_app();
        let (login_id, _logout_id) = seed_two_types(&mut app);
        let ghost_a = "ghost-a-doesnotexist".to_string();
        let ghost_b = "ghost-b-doesnotexist".to_string();
        let mut sorted_members = vec![ghost_a.clone(), ghost_b.clone()];
        sorted_members.sort();
        let group_id = crate::domain::AnalyzerModel::compute_group_id(&sorted_members);
        let group = crate::domain::MergeGroup {
            group_id: group_id.clone(),
            label: "Phantom".to_string(),
            members: sorted_members.clone(),
            members_prior_name: vec![None, None],
        };
        let profile = SourceProfile {
            renames: vec![],
            known_unrelated_types: vec![],
            normalized_field_overrides: vec![],
            negative_filters: crate::domain::DataFilters::default(),
            whitelist_terms: vec![],
            merge_groups: vec![group],
        };
        app.apply_profile(profile, false);
        assert!(app.model.merge_groups.contains_key(&group_id));
        // Empty TypeProfile seeded.
        let tp = app
            .model
            .types
            .get(&group_id)
            .expect("group TypeProfile seeded");
        assert_eq!(tp.count, 0);
        // Aliases registered so future ingest of those member ids redirects
        // into the group.
        for m in &sorted_members {
            assert_eq!(app.model.type_aliases.get(m), Some(&group_id));
        }
        // Original sibling types untouched.
        assert!(app.model.types.contains_key(&login_id));
    }

    #[test]
    fn profile_import_at_boot_redirects_ingest_into_group() {
        // The actual CLI --profile + --reset flow: apply the profile to an
        // empty model, then ingest. The alias hook registered by
        // apply_profile_merge_groups should fold every matching event into
        // the merged group instead of leaving the merge label as a phantom.
        let mut app = test_app();
        // Compute the structural ids the two shapes will hash to, without
        // touching the live model's types map (so members are genuinely
        // absent when apply_profile runs).
        let mut probe = crate::domain::AnalyzerModel::new();
        probe.ingest(json!({"event": "login", "user": "a"}), 1.0);
        probe.ingest(json!({"event": "logout", "session": "z"}), 2.0);
        let login_id = probe
            .events
            .iter()
            .find(|e| e.obj.get("user").is_some())
            .map(|e| e.type_id.clone())
            .expect("login id");
        let logout_id = probe
            .events
            .iter()
            .find(|e| e.obj.get("session").is_some())
            .map(|e| e.type_id.clone())
            .expect("logout id");
        let mut members = vec![login_id.clone(), logout_id.clone()];
        members.sort();
        let group_id = crate::domain::AnalyzerModel::compute_group_id(&members);
        let group = crate::domain::MergeGroup {
            group_id: group_id.clone(),
            label: "Auth".to_string(),
            members: members.clone(),
            members_prior_name: vec![None, None],
        };
        let profile = SourceProfile {
            renames: vec![],
            known_unrelated_types: vec![],
            normalized_field_overrides: vec![],
            negative_filters: crate::domain::DataFilters {
                type_filter: format!("!{}", crate::domain::default_type_label(&login_id)),
                ..crate::domain::DataFilters::default()
            },
            whitelist_terms: vec![],
            merge_groups: vec![group],
        };
        // Model is empty — exactly the CLI boot precondition.
        assert!(app.model.types.is_empty());
        app.apply_profile(profile, false);
        // Filter rewrite happened even though no types are present yet.
        assert_eq!(app.event_filters.type_filter, "!Auth");
        // Now stream events through and confirm they land in the group.
        app.model
            .ingest(json!({"event": "login", "user": "a"}), 10.0);
        app.model
            .ingest(json!({"event": "login", "user": "b"}), 11.0);
        app.model
            .ingest(json!({"event": "logout", "session": "z"}), 12.0);
        assert!(!app.model.types.contains_key(&login_id));
        assert!(!app.model.types.contains_key(&logout_id));
        assert_eq!(app.model.types.get(&group_id).unwrap().count, 3);
    }

    #[test]
    fn profile_import_rewrites_filter_to_group_label() {
        // event_filters.type_filter references "login" (the prior canonical
        // name of one of the merged members). After import, the filter should
        // be rewritten to the merged label "Auth".
        let mut app = test_app();
        let (login_id, logout_id) = seed_two_types(&mut app);
        // Rename one of the members so the filter rewrite has something
        // user-recognisable to match.
        app.model.rename_type(&login_id, "LoginEvt".to_string());
        let prior_login_name = app.model.canonical_type_name(&login_id);
        assert_eq!(prior_login_name, "LoginEvt");
        let mut sorted_members = vec![login_id.clone(), logout_id.clone()];
        sorted_members.sort();
        let group_id = crate::domain::AnalyzerModel::compute_group_id(&sorted_members);
        let group = crate::domain::MergeGroup {
            group_id: group_id.clone(),
            label: "Merged".to_string(),
            members: sorted_members,
            members_prior_name: vec![None, None],
        };
        let profile = SourceProfile {
            renames: vec![],
            known_unrelated_types: vec![],
            normalized_field_overrides: vec![],
            negative_filters: crate::domain::DataFilters {
                type_filter: "LoginEvt".to_string(),
                ..crate::domain::DataFilters::default()
            },
            whitelist_terms: vec![],
            merge_groups: vec![group],
        };
        app.apply_profile(profile, false);
        assert_eq!(app.event_filters.type_filter, "Merged");
        assert!(app.model.merge_groups.contains_key(&group_id));
    }
}
