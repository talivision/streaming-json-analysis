use crate::domain::{AnalyzerModel, DataFilters, EventRecord, FilterField};
use crate::io::StreamReader;
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
use serde_json::Value;
use std::io::stdout;
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};
use std::time::{SystemTime, UNIX_EPOCH};

const LIVE_WINDOW_DEFAULT: usize = 120;
const LIVE_RECOMPUTE_MIN_INTERVAL: Duration = Duration::from_secs(1);
const LIVE_FALLBACK_POLL_INTERVAL: Duration = Duration::from_millis(10);
const UI_FRAME_SLEEP: Duration = Duration::from_millis(16);
const UI_BURST_SLEEP: Duration = Duration::from_millis(1);

pub struct ObjectInspector {
    pub event: EventRecord,
    pub key_paths: Vec<String>,
    pub key_index: usize,
}

pub struct LiveRenderData<'a> {
    pub rows: Vec<&'a EventRecord>,
    pub selected_visible: Option<usize>,
    pub selected: Option<&'a EventRecord>,
    pub total: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateBoundaryViewMode {
    Point,
    Interval,
}

impl RateBoundaryViewMode {
    pub fn next(self) -> Self {
        match self {
            Self::Point => Self::Interval,
            Self::Interval => Self::Point,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Point => "point",
            Self::Interval => "interval",
        }
    }
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
    pub data_index: usize,
    pub periods_index: usize,
    pub period_event_index: usize,
    pub live_event_index: usize, // absolute index in full live rows
    pub live_view_start: usize,
    pub live_window_rows: usize,
    pub live_follow: bool,
    pub live_edge_until_center: bool,
    pub show_help_overlay: bool,
    pub rate_view: RateBoundaryViewMode,
    pub offline: bool,
    pub status: String,
    pub inspector: Option<ObjectInspector>,
    stashed_event_filters: Option<DataFilters>,
    reader: StreamReader,
    offline_loaded: bool,
    offline_fallback_ts: f64,
}

impl App {
    pub fn new() -> Self {
        let mut stream_path = PathBuf::from("/tmp/json_demo/stream.jsonl");
        let mut offline = false;
        for arg in std::env::args().skip(1) {
            if arg == "--offline" {
                offline = true;
            } else if !arg.starts_with('-') {
                stream_path = PathBuf::from(arg);
            }
        }

        Self {
            model: AnalyzerModel::new(),
            mode: UiMode::Live,
            input_mode: InputMode::None,
            input_buffer: String::new(),
            event_filters: DataFilters::default(),
            types_filter: String::new(),
            type_index: 0,
            path_index: 0,
            data_index: 0,
            periods_index: 0,
            period_event_index: 0,
            live_event_index: 0,
            live_view_start: 0,
            live_window_rows: LIVE_WINDOW_DEFAULT,
            live_follow: true,
            live_edge_until_center: false,
            show_help_overlay: false,
            rate_view: RateBoundaryViewMode::Point,
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
            offline_loaded: false,
            offline_fallback_ts: unix_ts(),
        }
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

        let mut last_poll = Instant::now() - LIVE_FALLBACK_POLL_INTERVAL;
        let mut last_live_recompute = Instant::now();

        let loop_result = (|| -> Result<()> {
            loop {
                let loop_started_at = Instant::now();

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

                let force_refresh = last_live_recompute.elapsed() >= LIVE_RECOMPUTE_MIN_INTERVAL;
                if ingested_any || force_refresh {
                    self.model.refresh_live_anomaly_scores();
                    last_live_recompute = Instant::now();
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
        loop_result
    }

    fn ingest_new_events(&mut self) -> Result<bool> {
        match self.reader.poll() {
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
                for (idx, e) in events.into_iter().enumerate() {
                    let ts = self.resolve_event_ts(&e, batch_now, idx)?;
                    self.model.ingest(e, ts);
                }

                if n > 0 {
                    self.status = format!("Ingested {} events", n);
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
                    self.status = "Offline mode: no events found".to_string();
                }

                if self.offline {
                    self.offline_loaded = true;
                }
                Ok(n > 0)
            }
            Err(err) => {
                self.status = format!("Stream read error: {err}");
                Ok(false)
            }
        }
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

    fn handle_key(&mut self, key: KeyEvent) -> bool {
        if key.modifiers.contains(KeyModifiers::CONTROL)
            && matches!(key.code, KeyCode::Char('c') | KeyCode::Char('C'))
        {
            return true;
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

        let code = normalize_navigation_code(key);

        if self.input_mode != InputMode::None {
            return self.handle_input(code);
        }

        if self.inspector.is_some() {
            return self.handle_inspector(code);
        }

        match code {
            KeyCode::Char('q') => return true,
            KeyCode::Char('h') | KeyCode::Char('?') => {
                self.show_help_overlay = !self.show_help_overlay;
            }
            KeyCode::Char('1') => self.mode = UiMode::Live,
            KeyCode::Char('2') => self.mode = UiMode::Periods,
            KeyCode::Char('3') => self.mode = UiMode::Types,
            KeyCode::Char('4') => self.mode = UiMode::Data,
            KeyCode::Char('m') => {
                if self.model.toggle_period() {
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
            KeyCode::Char('g') => {
                self.rate_view = self.rate_view.next();
                self.status = format!("Rate boundary: {}", self.rate_view.label());
            }
            KeyCode::Char('f') if self.mode == UiMode::Live => {
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
            KeyCode::Char('k') if self.mode != UiMode::Types => {
                self.start_event_filter_input(FilterField::Key)
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
            KeyCode::Enter => self.open_selected_event(),
            KeyCode::Char(' ') => self.toggle_current_path(),
            KeyCode::Char('u') => self.toggle_known_unrelated(),
            _ => {}
        }
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
            KeyCode::Char('k') => {
                if let Some(ins) = self.inspector.as_ref() {
                    if let Some(path) = ins.key_paths.get(ins.key_index) {
                        self.event_filters.key_filter = path.clone();
                        self.mode = UiMode::Data;
                        self.data_index = 0;
                        self.status = format!("Applied key filter: {}", path);
                    }
                }
            }
            KeyCode::Char('t') => {
                if let Some(ins) = self.inspector.as_ref() {
                    if let Some(idx) = self.model.find_type_index(&ins.event.type_id) {
                        self.mode = UiMode::Types;
                        self.type_index = idx;
                        self.path_index = 0;
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
                        self.data_index = 0;
                        self.live_event_index = 0;
                        self.period_event_index = 0;
                        self.refresh_live_position();
                    }
                    InputMode::TypesFilter => {
                        self.types_filter = self.input_buffer.trim().to_string();
                        self.type_index = 0;
                    }
                    InputMode::RenameType => {
                        let visible = self.visible_types();
                        if let Some(type_id) = visible.get(self.type_index) {
                            let type_id = type_id.clone();
                            self.model.rename_type(&type_id, self.input_buffer.clone());
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
        let total = self.visible_live_events().len();
        if total == 0 {
            self.live_event_index = 0;
            self.live_view_start = 0;
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
            NavIntent::Left | NavIntent::Right => return,
        };

        self.live_follow = false;
        if matches!(intent, NavIntent::Home) {
            self.live_edge_until_center = false;
            self.live_view_start = 0;
            self.clamp_live_indices();
            return;
        }
        if matches!(intent, NavIntent::End) {
            self.live_edge_until_center = false;
            let window = self.live_window_rows.max(1);
            self.live_view_start = total.saturating_sub(window);
            self.clamp_live_indices();
            return;
        }

        if was_follow {
            // When leaving follow, keep context from the stream head first, then converge to centered.
            self.live_view_start = self.live_event_index.saturating_sub(10);
            self.live_edge_until_center = true;
        }

        self.clamp_live_indices();
        self.reposition_live_selection();
    }

    fn navigate_periods(&mut self, intent: NavIntent) {
        match intent {
            NavIntent::LineUp => {
                if self.periods_index > 0 {
                    self.periods_index -= 1;
                    self.period_event_index = 0;
                } else if self.period_event_index > 0 {
                    self.period_event_index -= 1;
                }
            }
            NavIntent::LineDown => {
                let periods = self.model.closed_periods();
                if self.periods_index + 1 < periods.len() {
                    self.periods_index += 1;
                    self.period_event_index = 0;
                }
            }
            NavIntent::Home => {
                self.periods_index = 0;
                self.period_event_index = 0;
            }
            NavIntent::End => {
                let periods = self.model.closed_periods();
                self.periods_index = periods.len().saturating_sub(1);
                self.period_event_index = 0;
            }
            NavIntent::Left => {
                self.period_event_index = self.period_event_index.saturating_sub(1);
            }
            NavIntent::Right => {
                let n = self.visible_period_events().len();
                if self.period_event_index + 1 < n {
                    self.period_event_index += 1;
                }
            }
            NavIntent::PageUp | NavIntent::PageDown => {}
        }
    }

    fn navigate_types(&mut self, intent: NavIntent) {
        let n = self.visible_types().len();
        if n == 0 {
            self.type_index = 0;
            self.path_index = 0;
            return;
        }
        match intent {
            NavIntent::LineUp => {
                if self.type_index > 0 {
                    self.type_index -= 1;
                    self.path_index = 0;
                }
            }
            NavIntent::LineDown => {
                if self.type_index + 1 < n {
                    self.type_index += 1;
                    self.path_index = 0;
                }
            }
            NavIntent::Home => {
                self.type_index = 0;
                self.path_index = 0;
            }
            NavIntent::End => {
                self.type_index = n.saturating_sub(1);
                self.path_index = 0;
            }
            NavIntent::Left => {
                self.path_index = self.path_index.saturating_sub(1);
            }
            NavIntent::Right => {
                let visible = self.visible_types();
                if let Some(type_id) = visible.get(self.type_index) {
                    if let Some(tp) = self.model.types.get(type_id) {
                        if self.path_index + 1 < tp.considered_paths.len() {
                            self.path_index += 1;
                        }
                    }
                }
            }
            NavIntent::PageUp | NavIntent::PageDown => {}
        }
    }

    fn navigate_data(&mut self, intent: NavIntent) {
        let total = self.model.filtered_events(&self.event_filters).len();
        let page_step = 30usize;
        self.data_index = match intent {
            NavIntent::LineUp => self.data_index.saturating_sub(1),
            NavIntent::LineDown => self.data_index.saturating_add(1),
            NavIntent::PageUp => self.data_index.saturating_sub(page_step),
            NavIntent::PageDown => self.data_index.saturating_add(page_step),
            NavIntent::Home => 0,
            NavIntent::End => total.saturating_sub(1),
            NavIntent::Left | NavIntent::Right => self.data_index,
        };
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

    fn open_selected_event(&mut self) {
        let selected = match self.mode {
            UiMode::Live => self
                .visible_live_events()
                .get(self.live_event_index)
                .cloned()
                .cloned(),
            UiMode::Periods => self
                .visible_period_events()
                .get(self.period_event_index)
                .cloned()
                .cloned(),
            UiMode::Data => self
                .model
                .filtered_events(&self.event_filters)
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

    fn visible_live_events(&self) -> Vec<&EventRecord> {
        let mut events = self.model.filtered_events(&self.event_filters);
        events.reverse();
        events
    }

    fn live_anchor_at(&self, index: usize) -> Option<LiveAnchor> {
        self.visible_live_events().get(index).map(|e| LiveAnchor {
            ts: e.ts,
            type_id: e.type_id.clone(),
        })
    }

    fn find_live_index(&self, anchor: &LiveAnchor) -> Option<usize> {
        self.visible_live_events()
            .iter()
            .position(|e| e.ts == anchor.ts && e.type_id == anchor.type_id)
    }

    pub fn set_live_window_rows(&mut self, rows: usize) {
        self.live_window_rows = rows.max(1);
    }

    pub fn live_render_data_for_window(&self, max_rows: usize) -> LiveRenderData<'_> {
        let all = self.visible_live_events();
        let total = all.len();
        if total == 0 {
            return LiveRenderData {
                rows: Vec::new(),
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
        let rows: Vec<&EventRecord> = all[start..end].to_vec();
        let selected = all.get(self.live_event_index).copied();
        let selected_visible = if self.live_event_index >= start && self.live_event_index < end {
            Some(self.live_event_index - start)
        } else {
            None
        };
        LiveRenderData {
            rows,
            selected_visible,
            selected,
            total,
        }
    }

    fn clamp_live_indices(&mut self) {
        let total = self.visible_live_events().len();
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

    fn pin_live_to_latest(&mut self) {
        let total = self.visible_live_events().len();
        if total == 0 {
            self.live_event_index = 0;
            self.live_view_start = 0;
            return;
        }
        let window = self.live_window_rows.max(1);
        self.live_event_index = total - 1;
        self.live_view_start = total.saturating_sub(window);
    }

    fn refresh_live_position(&mut self) {
        if self.live_follow {
            self.live_edge_until_center = false;
            self.pin_live_to_latest();
        } else {
            self.clamp_live_indices();
            self.reposition_live_selection();
        }
    }

    fn ensure_live_selection_visible(&mut self) {
        let total = self.visible_live_events().len();
        if total == 0 {
            self.live_event_index = 0;
            self.live_view_start = 0;
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

    fn center_live_selection_in_view(&mut self) {
        let total = self.visible_live_events().len();
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

    fn reposition_live_selection(&mut self) {
        if self.live_edge_until_center {
            self.ensure_live_selection_visible();

            let total = self.visible_live_events().len();
            if total == 0 {
                self.live_event_index = 0;
                self.live_view_start = 0;
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
            self.center_live_selection_in_view();
        }
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

    fn visible_types(&self) -> Vec<String> {
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

    fn toggle_current_path(&mut self) {
        if self.mode != UiMode::Types {
            return;
        }
        let visible = self.visible_types();
        if let Some(type_id) = visible.get(self.type_index) {
            let type_id = type_id.clone();
            if let Some(tp) = self.model.types.get(&type_id) {
                let keys: Vec<String> = tp.considered_paths.keys().cloned().collect();
                if let Some(path) = keys.get(self.path_index) {
                    self.model.toggle_type_path(&type_id, path);
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
            self.mode = UiMode::Data;
            self.data_index = 0;
            self.period_event_index = 0;
            self.live_event_index = 0;
            self.refresh_live_position();
            self.status = format!(
                "Applied type filter: {}",
                self.model.type_display_name(type_id)
            );
        }
    }

    fn toggle_known_unrelated(&mut self) {
        if self.mode != UiMode::Types {
            return;
        }
        let visible = self.visible_types();
        if let Some(type_id) = visible.get(self.type_index) {
            let type_id = type_id.clone();
            self.model.toggle_known_unrelated_type(&type_id);
        }
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

#[cfg(test)]
mod tests {
    use super::normalize_navigation_code;
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

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
}
