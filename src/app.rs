use crate::domain::{
    prepare_event, value_at_path, value_token, ActionPeriod, AnalyzerModel, DataFilters,
    EventRecord, FilterField, PreparedEvent,
};
use crate::io::StreamReader;
use crate::persistence::{invalidate_state, load_state, save_state, RestoredState};
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
use serde_json::Value;
use std::env;
use std::fs;
use std::io::stdout;
use std::path::PathBuf;
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
    live_cache_dirty: bool,
    initial_load_target_bytes: Option<u64>,
    initial_load_complete: bool,
    pending_live_recompute: bool,
    show_status_debug: bool,
    quit_pending_until: Option<Instant>,
    pending_delete_period_id: Option<u64>,
}

impl App {
    pub fn new(
        stream_path: PathBuf,
        baseline_path: Option<PathBuf>,
        offline: bool,
        show_status_debug: bool,
    ) -> Self {
        let initial_load_target_bytes = fs::metadata(&stream_path)
            .ok()
            .map(|m| m.len())
            .filter(|len| *len > 0);
        let initial_load_complete = initial_load_target_bytes.is_none();
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
            live_cache_dirty: true,
            initial_load_target_bytes,
            initial_load_complete,
            pending_live_recompute: false,
            show_status_debug,
            quit_pending_until: None,
            pending_delete_period_id: None,
        };
        app.restore_persisted_state();
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
        if let Err(err) = self.persist_state() {
            eprintln!("warning: failed to persist session state: {err}");
        }
        loop_result
    }

    fn ingest_new_events(&mut self) -> Result<bool> {
        self.rebuild_live_cache_if_needed();
        let use_snapshot_parallel = self.offline || self.loading_locked();
        let events_result = if use_snapshot_parallel {
            self.reader.poll_snapshot_parallel()
        } else {
            self.reader.poll()
        };
        match events_result {
            Ok(events) => {
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
                    self.model.ingest_prepared(prepared, ts);
                }
                if n > 0 {
                    self.mark_live_cache_dirty();
                    self.pending_live_recompute = true;
                }
                self.apply_persisted_overrides_if_ready();

                if n > 0 {
                    if self.offline && !self.offline_loaded {
                        self.status = self.offline_load_status();
                    } else {
                        self.status = format!("Ingested {} events", n);
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
            Err(err) => {
                self.status = format!("Stream read error: {err}");
                if let Some(prompt) = self.delete_confirmation_status() {
                    self.status = prompt;
                }
                Ok(false)
            }
        }
    }

    fn ingest_baseline_corpus(&mut self) -> Result<()> {
        let Some(reader) = self.baseline_reader.as_mut() else {
            self.baseline_loaded = true;
            return Ok(());
        };

        let events = reader.poll()?;
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
            self.baseline_events.push(EventRecord {
                ts,
                type_id,
                obj,
                keys,
                action_period_id: None,
                in_action_period: false,
                live_rate_score: 0.0,
                live_uniq_score: 0.0,
            });
        }

        self.baseline_loaded = true;
        self.pending_live_recompute = true;
        self.status = format!(
            "Baseline loaded: {} events from {}",
            self.baseline_events.len(),
            reader.path().display()
        );
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
                    "Restored session: {} periods, {} renames",
                    saved.periods.len(),
                    saved.renames.len()
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
            return;
        }
        let saved = self.pending_restore.take().unwrap();
        if !saved.renames.is_empty() {
            self.model.apply_renames(&saved.renames);
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
            &self.model.current_label,
            &self.event_filters,
            self.stashed_event_filters.as_ref(),
            &self.types_filter,
        )
    }

    fn handle_key(&mut self, key: KeyEvent) -> bool {
        self.startup_hint = None;
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
                _ => {}
            }
        }
        if key.modifiers.contains(KeyModifiers::CONTROL)
            && matches!(key.code, KeyCode::Char('c') | KeyCode::Char('C'))
        {
            return true;
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
            KeyCode::Char('q') => {
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
            KeyCode::Esc if self.mode == UiMode::Live && self.live_key_focus => {
                self.exit_live_key_focus();
            }
            KeyCode::Char('h') | KeyCode::Char('?') => {
                self.show_help_overlay = !self.show_help_overlay;
            }
            KeyCode::Char('1') => {
                self.set_ui_mode(UiMode::Live);
                self.clamp_live_key_selection();
            }
            KeyCode::Char('2') => self.set_ui_mode(UiMode::Periods),
            KeyCode::Char('3') => self.set_ui_mode(UiMode::Types),
            KeyCode::Char('4') => self.set_ui_mode(UiMode::Data),
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
                if self.model.toggle_period() {
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
                self.start_event_filter_input(FilterField::Fuzzy)
            }
            KeyCode::Char('e') if self.mode != UiMode::Types => {
                self.start_event_filter_input(FilterField::Exact)
            }
            KeyCode::Char('y') if self.mode != UiMode::Types => self.toggle_event_filters_enabled(),
            KeyCode::Char('/') if self.mode == UiMode::Types => {
                self.input_mode = InputMode::TypesFilter;
                self.input_buffer = self.types_filter.clone();
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
                self.apply_period_selected_key_filter();
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
            self.periods_focus = PeriodsFocus::Periods;
            return;
        }
        self.periods_index = self.periods_index.min(periods_len.saturating_sub(1));
        let event_count = self.visible_period_events().len();
        if event_count == 0 {
            self.period_event_index = 0;
            self.period_json_key_index = 0;
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
                    PeriodsFocus::Json => PeriodsFocus::Events,
                };
            }
            NavIntent::Right => {
                self.advance_periods_focus();
            }
            NavIntent::LineUp => match self.periods_focus {
                PeriodsFocus::Periods => {
                    if self.periods_index > 0 {
                        self.periods_index -= 1;
                        self.period_event_index = 0;
                        self.period_json_key_index = 0;
                    }
                }
                PeriodsFocus::Events => {
                    self.period_event_index = self.period_event_index.saturating_sub(1);
                    self.period_json_key_index = 0;
                }
                PeriodsFocus::Json => {
                    self.period_json_key_index = self.period_json_key_index.saturating_sub(1);
                }
            },
            NavIntent::LineDown => match self.periods_focus {
                PeriodsFocus::Periods => {
                    if self.periods_index + 1 < periods_len {
                        self.periods_index += 1;
                        self.period_event_index = 0;
                        self.period_json_key_index = 0;
                    }
                }
                PeriodsFocus::Events => {
                    if event_count > 0 && self.period_event_index + 1 < event_count {
                        self.period_event_index += 1;
                    }
                    self.period_json_key_index = 0;
                }
                PeriodsFocus::Json => {
                    let keys = self.period_selected_key_paths();
                    if self.period_json_key_index + 1 < keys.len() {
                        self.period_json_key_index += 1;
                    }
                }
            },
            NavIntent::Home => match self.periods_focus {
                PeriodsFocus::Periods => {
                    self.periods_index = 0;
                    self.period_event_index = 0;
                    self.period_json_key_index = 0;
                }
                PeriodsFocus::Events => {
                    self.period_event_index = 0;
                    self.period_json_key_index = 0;
                }
                PeriodsFocus::Json => {
                    self.period_json_key_index = 0;
                }
            },
            NavIntent::End => match self.periods_focus {
                PeriodsFocus::Periods => {
                    self.periods_index = periods_len.saturating_sub(1);
                    self.period_event_index = 0;
                    self.period_json_key_index = 0;
                }
                PeriodsFocus::Events => {
                    if event_count > 0 {
                        self.period_event_index = event_count.saturating_sub(1);
                    }
                    self.period_json_key_index = 0;
                }
                PeriodsFocus::Json => {
                    let keys = self.period_selected_key_paths();
                    if !keys.is_empty() {
                        self.period_json_key_index = keys.len().saturating_sub(1);
                    }
                }
            },
            NavIntent::PageUp => match self.periods_focus {
                PeriodsFocus::Periods => {
                    if self.periods_index > 0 {
                        self.periods_index = self.periods_index.saturating_sub(MENU_PAGE_STEP);
                        self.period_event_index = 0;
                        self.period_json_key_index = 0;
                    }
                }
                PeriodsFocus::Events => {
                    self.period_event_index =
                        self.period_event_index.saturating_sub(MENU_PAGE_STEP);
                    self.period_json_key_index = 0;
                }
                PeriodsFocus::Json => {
                    self.period_json_key_index =
                        self.period_json_key_index.saturating_sub(MENU_PAGE_STEP);
                }
            },
            NavIntent::PageDown => match self.periods_focus {
                PeriodsFocus::Periods => {
                    if self.periods_index + 1 < periods_len {
                        self.periods_index = (self.periods_index + MENU_PAGE_STEP)
                            .min(periods_len.saturating_sub(1));
                        self.period_event_index = 0;
                        self.period_json_key_index = 0;
                    }
                }
                PeriodsFocus::Events => {
                    if event_count > 0 {
                        self.period_event_index = (self.period_event_index + MENU_PAGE_STEP)
                            .min(event_count.saturating_sub(1));
                    }
                    self.period_json_key_index = 0;
                }
                PeriodsFocus::Json => {
                    let keys = self.period_selected_key_paths();
                    if !keys.is_empty() {
                        self.period_json_key_index = (self.period_json_key_index + MENU_PAGE_STEP)
                            .min(keys.len().saturating_sub(1));
                    }
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
        self.periods_focus = PeriodsFocus::Periods;
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
            UiMode::Data => self
                .visible_baseline_events()
                .get(self.data_index)
                .cloned()
                .cloned(),
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
    }

    fn rebuild_live_cache_if_needed(&mut self) {
        if self.loading_locked() {
            return;
        }
        if !self.live_cache_dirty {
            return;
        }
        self.live_visible_indices = self.model.filtered_event_indices(&self.event_filters, None);
        self.live_cache_dirty = false;
    }

    pub fn ensure_live_cache(&mut self) {
        self.rebuild_live_cache_if_needed();
    }

    fn live_visible_total(&self) -> usize {
        self.live_visible_indices.len()
    }

    fn live_event_at_visible_index(&self, index: usize) -> Option<&EventRecord> {
        let event_idx = *self.live_visible_indices.get(index)?;
        self.model.events.get(event_idx)
    }

    pub fn visible_baseline_events(&self) -> Vec<&EventRecord> {
        self.model
            .filtered_event_slice(&self.baseline_events, &self.event_filters)
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

    fn visible_period_events(&self) -> Vec<&EventRecord> {
        let periods = self.model.closed_periods();
        if let Some(p) = periods.get(self.periods_index) {
            let start = p.start;
            let end = p.end.unwrap_or(p.start);
            self.model
                .filtered_events_in_range(&self.event_filters, Some((start, end)))
        } else {
            Vec::new()
        }
    }

    pub fn visible_types(&self) -> Vec<String> {
        let query = self.types_filter.to_lowercase();
        self.model
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
            .map(|(type_id, _)| type_id.clone())
            .collect()
    }

    pub fn startup_hint(&self) -> Option<&str> {
        self.startup_hint.as_deref()
    }

    pub fn should_show_status_line(&self) -> bool {
        self.show_status_debug || self.loading_locked() || self.pending_delete_period_id.is_some()
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
            let type_id = type_id.clone();
            self.model.toggle_known_unrelated_type(&type_id);
            self.mark_live_cache_dirty();
            if self.mode == UiMode::Live {
                self.refresh_live_position();
            }
        }
    }

    fn loading_locked(&self) -> bool {
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

#[cfg(test)]
mod tests {
    use super::{
        normalize_navigation_code, parse_event_timestamp_millis, App, NavIntent, PeriodsFocus,
        MENU_PAGE_STEP,
    };
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
