use crate::app::{App, ModalConfirmation, ObjectInspector, PeriodsFocus};
use crate::domain::{EventRecord, FilterField, PathOverride};
use indexmap::IndexMap;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, Paragraph, Tabs, Wrap};
use ratatui::Frame;

struct JsonRender {
    lines: Vec<Line<'static>>,
    selected_line: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UiMode {
    Live,
    Periods,
    Types,
    Data,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InputMode {
    None,
    Label,
    EventFilter(FilterField),
    TypesFilter,
    RenameType,
    InsertPeriodRange,
    EditPeriodRange,
    ExportSessionPath,
    ExportProfilePath,
}

pub fn draw_ui(frame: &mut Frame<'_>, app: &mut App) {
    let root = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(10),
            Constraint::Length(5),
        ])
        .split(frame.area());

    // Compute once per frame; passed to every row renderer to avoid O(types) per row.
    let max_type_count = app.model.types.values().map(|t| t.count).max().unwrap_or(1) as f64;

    draw_tabs(frame, root[0], app.mode, app.baseline_tab_enabled());
    match app.mode {
        UiMode::Live => draw_live(frame, root[1], app, max_type_count),
        UiMode::Periods => draw_periods(frame, root[1], app, max_type_count),
        UiMode::Types => draw_types(frame, root[1], app),
        UiMode::Data => draw_data(frame, root[1], app, max_type_count),
    }
    let modal = app.modal_confirmation();
    draw_controls(frame, root[2], app);

    if let Some(inspector) = app.inspector.as_ref() {
        draw_inspector(frame, inspector, app);
    }
    if app.show_help_overlay {
        draw_full_help(frame, app);
    }
    if app.type_preview_open() && app.mode == UiMode::Types {
        draw_type_preview_modal(frame, app);
    }
    if let Some(confirm) = modal {
        draw_confirmation_modal(frame, &confirm);
    }
}

fn draw_tabs(frame: &mut Frame<'_>, area: Rect, mode: UiMode, baseline_enabled: bool) {
    let mut titles = vec![
        tab_title("1", "Live"),
        tab_title("2", "Periods"),
        tab_title("3", "Types"),
    ];
    if baseline_enabled {
        titles.push(tab_title("4", "Baseline"));
    }
    let selected = match mode {
        UiMode::Live => 0,
        UiMode::Periods => 1,
        UiMode::Types => 2,
        UiMode::Data => {
            if baseline_enabled {
                3
            } else {
                2
            }
        }
    };
    let tabs = Tabs::new(titles)
        .block(
            Block::default()
                .title("JSON Analyzer")
                .borders(Borders::ALL),
        )
        .highlight_style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )
        .select(selected);
    frame.render_widget(tabs, area);
}

fn draw_live(frame: &mut Frame<'_>, area: Rect, app: &mut App, max_type_count: f64) {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(40), Constraint::Percentage(60)])
        .split(area);

    let list_rows = cols[0].height.saturating_sub(2) as usize;
    app.set_live_window_rows(list_rows);
    app.ensure_live_cache();
    let live = app.live_render_data_for_window(list_rows);
    let selected_visible = if live.rows.is_empty() {
        None
    } else {
        live.selected_visible.or(Some(0))
    };
    let selected_event_abs_index = selected_visible
        .and_then(|vis_idx| live.row_indices.get(vis_idx).copied())
        .and_then(|row_1based| row_1based.checked_sub(1));
    let mut items = Vec::new();
    let stream_inner_width = cols[0].width.saturating_sub(2) as usize;
    let index_width = app.model.total_objects().max(1).to_string().len().max(3);
    let type_col_width = live
        .rows
        .iter()
        .map(|e| app.model.canonical_type_name(&e.type_id).chars().count() + 2)
        .max()
        .unwrap_or(16)
        .clamp(12, 36);
    let first_live_ts = app.model.events.front().map(|e| e.ts).unwrap_or(0.0);
    for (idx, e) in live.rows.iter().enumerate() {
        let selected = Some(idx) == selected_visible;
        let row_index = live.row_indices.get(idx).copied();
        let diff_ms = Some((((e.ts - first_live_ts) * 1000.0).round() as i64).max(0));
        items.push(ListItem::new(render_event_line(
            app,
            e,
            row_index,
            index_width,
            type_col_width,
            diff_ms,
            selected,
            stream_inner_width,
            max_type_count,
        )));
    }

    let live_title = format!(
        "Events  row {}/{}  objects {}  types {}",
        app.live_event_index.saturating_add(1),
        live.total,
        app.model.total_objects(),
        app.model.types.len()
    );
    let stream = List::new(items).block(Block::default().title(live_title).borders(Borders::ALL));
    frame.render_widget(stream, cols[0]);

    let (preview_text, preview_scroll) = if let Some(sel) = live.selected {
        let mut lines = vec![Line::from(Span::styled(
            app.model.type_display_name(&sel.type_id),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ))];
        let show_uniq = sel.live_uniq_score;
        let show_rate = sel.live_rate_score;
        let value_color = value_anomaly_color(anomaly_norm(show_uniq));
        let rate_color = rate_anomaly_color(anomaly_norm(show_rate));
        if sel.in_action_period {
            lines.push(Line::from(vec![
                Span::styled("value anomaly ", Style::default().fg(Color::Gray)),
                Span::styled(
                    format_score(show_uniq),
                    Style::default()
                        .fg(value_color)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw("  "),
                Span::styled("rate anomaly ", Style::default().fg(Color::Gray)),
                Span::styled(
                    format_score(show_rate),
                    Style::default().fg(rate_color).add_modifier(Modifier::BOLD),
                ),
            ]));
            if let Some(event_idx) = selected_event_abs_index {
                if let Some((actual, expected)) = app.model.rate_debug_info_for_event_index(event_idx)
                {
                    lines.push(Line::from(vec![
                        Span::styled("rate  expected ", Style::default().fg(Color::DarkGray)),
                        Span::styled(
                            format!("{:.4}/s", expected),
                            Style::default().fg(Color::DarkGray),
                        ),
                        Span::styled("  actual ", Style::default().fg(Color::DarkGray)),
                        Span::styled(
                            format!("{:.4}/s", actual),
                            Style::default().fg(Color::DarkGray),
                        ),
                    ]));
                }
            }
        }
        lines.push(Line::from(""));
        let key_paths = app.live_selected_key_paths();
        let selected_path = if app.live_key_focus {
            key_paths.get(app.live_key_index)
        } else {
            None
        };
        let considered_paths = app
            .model
            .types
            .get(&sel.type_id)
            .map(|tp| &tp.considered_paths);
        let sub_lc = app.event_filters.substring_filter.to_lowercase();
        let whitelist_terms = if app.whitelist_highlight_enabled() {
            app.whitelist_terms()
        } else {
            &[]
        };
        let rendered = render_json_keypicker(
            &sel.obj,
            selected_path,
            app.live_key_focus,
            app.live_value_focus,
            &app.event_filters.key_filter,
            &sub_lc,
            whitelist_terms,
            considered_paths,
        );
        let scroll = selected_json_scroll(rendered.selected_line, cols[1].height);
        lines.extend(rendered.lines);
        (Text::from(lines), scroll)
    } else {
        (Text::from("No event selected"), 0)
    };
    let title = selected_json_title(app.live_key_focus, app.live_value_focus, cols[1].width);
    let preview = Paragraph::new(preview_text)
        .scroll((preview_scroll, 0))
        .wrap(Wrap { trim: false })
        .block(Block::default().title(title).borders(Borders::ALL));
    frame.render_widget(preview, cols[1]);
}

fn draw_periods(frame: &mut Frame<'_>, area: Rect, app: &App, max_type_count: f64) {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(22),
            Constraint::Percentage(28),
            Constraint::Percentage(50),
        ])
        .split(area);

    let periods = app.model.closed_periods();
    let mut p_items = Vec::new();
    for (idx, p) in periods.iter().enumerate() {
        let mut style = Style::default();
        if idx == app.periods_index {
            style = if app.periods_focus == PeriodsFocus::Periods {
                style.fg(Color::Yellow).add_modifier(Modifier::BOLD)
            } else {
                style.fg(Color::Gray)
            };
        }
        let dur = p.end.unwrap_or(p.start) - p.start;
        let row_range = app
            .period_row_range_for(p)
            .map(|(a, b)| format!("{a}-{b}"))
            .unwrap_or_else(|| "-".to_string());
        p_items.push(ListItem::new(Line::from(vec![Span::styled(
            format!(
                "[{}] #{} {} ({:.2}s) rows {}",
                idx + 1,
                p.id,
                p.label,
                dur,
                row_range
            ),
            style,
        )])));
    }
    frame.render_widget(
        List::new(p_items).block(
            Block::default()
                .title(action_periods_title(cols[0].width))
                .borders(Borders::ALL),
        ),
        cols[0],
    );

    let mut rows = Vec::new();
    let events_inner_width = cols[1].width.saturating_sub(2) as usize;
    let index_width = app.model.total_objects().max(1).to_string().len().max(3);
    let max_period_rows = (cols[1].height as usize).saturating_sub(2);
    let mut selected_event: Option<&EventRecord> = None;
    if periods.get(app.periods_index).is_some() {
        let events = app.visible_period_event_rows();
        let type_col_width = events
            .iter()
            .map(|(_, e)| app.model.canonical_type_name(&e.type_id).chars().count() + 2)
            .max()
            .unwrap_or(16)
            .clamp(12, 36);
        let first_period_ts = events.first().map(|(_, e)| e.ts).unwrap_or(0.0);
        let total = events.len();
        let window = max_period_rows.max(1);
        let start_idx = if total <= window {
            0
        } else {
            let half = window / 2;
            app.period_event_index
                .saturating_sub(half)
                .min(total.saturating_sub(window))
        };
        for (vis_idx, (event_idx, e)) in events.iter().skip(start_idx).take(window).enumerate() {
            let idx = start_idx + vis_idx;
            let selected = idx == app.period_event_index;
            let diff_ms = Some((((e.ts - first_period_ts) * 1000.0).round() as i64).max(0));
            rows.push(ListItem::new(render_event_line(
                app,
                e,
                Some(*event_idx + 1),
                index_width,
                type_col_width,
                diff_ms,
                selected,
                events_inner_width,
                max_type_count,
            )));
        }
        selected_event = events.get(app.period_event_index).map(|(_, e)| *e);
    }
    frame.render_widget(
        List::new(rows).block(Block::default().title("Events").borders(Borders::ALL)),
        cols[1],
    );

    let (preview_text, preview_scroll) = if let Some(sel) = selected_event {
        let mut lines = vec![Line::from(Span::styled(
            app.model.type_display_name(&sel.type_id),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ))];
        lines.push(Line::from(""));
        let considered_paths = app
            .model
            .types
            .get(&sel.type_id)
            .map(|tp| &tp.considered_paths);
        let key_paths = app.period_selected_key_paths();
        let selected_path = if app.periods_focus == PeriodsFocus::Json {
            key_paths.get(app.period_json_key_index)
        } else {
            None
        };
        let sub_lc = app.event_filters.substring_filter.to_lowercase();
        let whitelist_terms = if app.whitelist_highlight_enabled() {
            app.whitelist_terms()
        } else {
            &[]
        };
        let rendered = render_json_keypicker(
            &sel.obj,
            selected_path,
            app.periods_focus == PeriodsFocus::Json,
            app.period_value_focus,
            &app.event_filters.key_filter,
            &sub_lc,
            whitelist_terms,
            considered_paths,
        );
        let scroll = selected_json_scroll(rendered.selected_line, cols[2].height);
        lines.extend(rendered.lines);
        (Text::from(lines), scroll)
    } else {
        (Text::from("No event selected"), 0)
    };
    frame.render_widget(
        Paragraph::new(preview_text)
            .scroll((preview_scroll, 0))
            .wrap(Wrap { trim: false })
            .block(
                Block::default()
                    .title(selected_json_title(
                        app.periods_focus == PeriodsFocus::Json,
                        app.period_value_focus,
                        cols[2].width,
                    ))
                    .borders(Borders::ALL),
            ),
        cols[2],
    );
}

fn styled_hotkey(label: &str) -> Span<'static> {
    Span::styled(
        label.to_string(),
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD),
    )
}

fn types_list_title(row: usize, total: usize, unfiltered: usize, path_focus: bool, search: &str, pane_width: u16) -> Line<'static> {
    let focus = if path_focus { "details" } else { "list" };
    let (counter_len, counter_spans) = if search.is_empty() {
        let counter = format!("Types {}/{} ({focus}) ", row, total);
        (counter.len(), vec![Span::raw(counter)])
    } else {
        let omitted = unfiltered.saturating_sub(total);
        let search = truncate_text(search, 16);
        (
            format!("Types {}/{} ({omitted} omitted, filter: {}) ", row, total, search).len(),
            vec![
                Span::raw(format!("Types {}/{} ({omitted} omitted, filter: ", row, total)),
                Span::styled(search, Style::default().fg(Color::LightGreen)),
                Span::raw(") "),
            ],
        )
    };
    if pane_width < 20 {
        return Line::from("Types");
    }
    if (pane_width as usize) < counter_len + 4 {
        return Line::from(counter_spans);
    }
    // Narrow: just show keys without descriptions
    if pane_width < 56 {
        let mut spans = counter_spans.clone();
        spans.extend([
            styled_hotkey("↵"),
            Span::raw("/"),
            styled_hotkey("t"),
            Span::raw("/"),
            styled_hotkey("u"),
            Span::raw("/"),
            styled_hotkey("/"),
        ]);
        return Line::from(spans);
    }
    // Medium: short descriptions
    if pane_width < 80 {
        let mut spans = counter_spans.clone();
        spans.extend([
            styled_hotkey("↵"),
            Span::raw(" details, "),
            styled_hotkey("t"),
            Span::raw(" filter, "),
            styled_hotkey("u"),
            Span::raw(" exclude, "),
            styled_hotkey("/"),
            Span::raw(" search"),
        ]);
        return Line::from(spans);
    }
    // Wide: full descriptions
    let mut spans = counter_spans;
    spans.extend([
        styled_hotkey("↵"),
        Span::raw(" details, "),
        styled_hotkey("t"),
        Span::raw(" filter, "),
        styled_hotkey("u"),
        Span::raw(" toggle !type, "),
        styled_hotkey("/"),
        Span::raw(" search, by count"),
    ]);
    Line::from(spans)
}

fn action_periods_title(pane_width: u16) -> Line<'static> {
    if pane_width < 24 {
        return Line::from("Periods");
    }
    if pane_width < 36 {
        return Line::from(vec![
            Span::raw("Periods ("),
            styled_hotkey("i"),
            Span::raw("/"),
            styled_hotkey("e"),
            Span::raw("/"),
            styled_hotkey("d"),
            Span::raw(")"),
        ]);
    }
    if pane_width < 56 {
        return Line::from(vec![
            Span::raw("Periods ("),
            styled_hotkey("i"),
            Span::raw(" add, "),
            styled_hotkey("e"),
            Span::raw(" edit, "),
            styled_hotkey("d"),
            Span::raw(" del?)"),
        ]);
    }
    Line::from(vec![
        Span::raw("Action Periods ("),
        styled_hotkey("i"),
        Span::raw(" insert start-end, "),
        styled_hotkey("e"),
        Span::raw(" edit selected, "),
        styled_hotkey("d"),
        Span::raw(" delete selected)"),
    ])
}

fn selected_json_title(is_key_focus: bool, value_focus: bool, pane_width: u16) -> Line<'static> {
    if !is_key_focus {
        return Line::from("selected JSON");
    }
    if value_focus {
        if pane_width < 64 {
            return Line::from(vec![
                Span::raw("selected JSON ("),
                styled_hotkey("↵"),
                Span::raw("/"),
                styled_hotkey("e"),
                Span::raw(", "),
                styled_hotkey("t"),
                Span::raw(")"),
            ]);
        }
        return Line::from(vec![
            Span::raw("selected JSON ("),
            styled_hotkey("↵"),
            Span::raw("/"),
            styled_hotkey("e"),
            Span::raw(" apply value filter, "),
            styled_hotkey("t"),
            Span::raw(" jump type)"),
        ]);
    }
    let narrow = pane_width < 56;
    if narrow {
        Line::from(vec![
            Span::raw("selected JSON ("),
            styled_hotkey("↵"),
            Span::raw(", "),
            styled_hotkey("t"),
            Span::raw(")"),
        ])
    } else {
        Line::from(vec![
            Span::raw("selected JSON ("),
            styled_hotkey("↵"),
            Span::raw(" apply filter, "),
            styled_hotkey("→"),
            Span::raw(" value, "),
            styled_hotkey("t"),
            Span::raw(" jump type)"),
        ])
    }
}

fn type_details_title(app: &App, pane_width: u16) -> Line<'static> {
    if !app.types_path_focus {
        if pane_width < 64 {
            return Line::from(vec![
                Span::raw("Type Details / Paths ("),
                styled_hotkey("t"),
                Span::raw(", "),
                styled_hotkey("u"),
                Span::raw(", "),
                styled_hotkey("j"),
                Span::raw(")"),
            ]);
        }
        return Line::from(vec![
            Span::raw("Type Details / Paths ("),
            styled_hotkey("t"),
            Span::raw(" filter to live, "),
            styled_hotkey("u"),
            Span::raw(" toggle !type filter, "),
            styled_hotkey("j"),
            Span::raw(" preview sample)"),
        ]);
    }
    let narrow = pane_width < 64;
    if narrow {
        Line::from(vec![
            Span::raw("Type Details / Paths ("),
            styled_hotkey("space"),
            Span::raw(", "),
            styled_hotkey("t"),
            Span::raw(", "),
            styled_hotkey("u"),
            Span::raw(", "),
            styled_hotkey("j"),
            Span::raw(")"),
        ])
    } else {
        Line::from(vec![
            Span::raw("Type Details / Paths ("),
            styled_hotkey("space"),
            Span::raw(" toggle path, "),
            styled_hotkey("t"),
            Span::raw(" filter to live, "),
            styled_hotkey("u"),
            Span::raw(" toggle !type filter, "),
            styled_hotkey("j"),
            Span::raw(" preview sample)"),
        ])
    }
}

fn draw_types(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(45), Constraint::Percentage(55)])
        .split(area);

    let visible_ids = app.visible_types();
    let visible: Vec<(&str, &_)> = visible_ids
        .iter()
        .filter_map(|id| app.model.types.get(id.as_str()).map(|tp| (id.as_str(), tp)))
        .collect();
    let total_types = visible.len();
    let selected_type = if total_types == 0 {
        0
    } else {
        app.type_index.min(total_types.saturating_sub(1))
    };
    let type_window = (cols[0].height as usize).saturating_sub(2).max(1);
    let type_start = if total_types <= type_window {
        0
    } else {
        let half = type_window / 2;
        selected_type
            .saturating_sub(half)
            .min(total_types.saturating_sub(type_window))
    };

    let mut type_items = Vec::new();
    for (vis_idx, (type_id, tp)) in visible
        .iter()
        .skip(type_start)
        .take(type_window)
        .enumerate()
    {
        let idx = type_start + vis_idx;
        let excluded = app.type_excluded_by_type_filter(type_id);
        let mut style = Style::default();
        if idx == app.type_index {
            style = if app.types_path_focus {
                style.fg(Color::Gray)
            } else if excluded {
                style
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD | Modifier::DIM)
            } else {
                style.fg(Color::Yellow).add_modifier(Modifier::BOLD)
            };
        } else if excluded {
            style = style.fg(Color::Gray).add_modifier(Modifier::DIM);
        }
        let name = app.model.canonical_type_name(type_id);
        let marker = if excluded { "[-] " } else { "    " };
        type_items.push(ListItem::new(Line::from(vec![Span::styled(
            format!("{}{}  count={}", marker, name, tp.count),
            style,
        )])));
    }
    let type_title = types_list_title(
        if total_types == 0 { 0 } else { selected_type + 1 },
        total_types,
        app.model.types.len(),
        app.types_path_focus,
        &app.types_filter,
        cols[0].width,
    );
    frame.render_widget(
        List::new(type_items).block(Block::default().title(type_title).borders(Borders::ALL)),
        cols[0],
    );

    let mut lines = Vec::new();
    if let Some((type_id, tp)) = visible.get(selected_type) {
        lines.push(Line::from(Span::styled(
            app.model.canonical_type_name(type_id),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )));
        lines.push(Line::from(vec![
            Span::styled("id: ", Style::default().fg(Color::Gray)),
            Span::styled((*type_id).to_string(), Style::default().fg(Color::DarkGray)),
        ]));
        lines.push(Line::from(""));

        let total_paths = tp.considered_paths.len();
        let selected_path = if total_paths == 0 {
            0
        } else {
            app.path_index.min(total_paths.saturating_sub(1))
        };
        let path_window = (cols[1].height as usize).saturating_sub(6).max(1);
        let path_start = if total_paths <= path_window {
            0
        } else {
            let half = path_window / 2;
            selected_path
                .saturating_sub(half)
                .min(total_paths.saturating_sub(path_window))
        };

        for (vis_idx, (path, on)) in tp
            .considered_paths
            .iter()
            .skip(path_start)
            .take(path_window)
            .enumerate()
        {
            let idx = path_start + vis_idx;
            let selected = idx == app.path_index;
            let sel = if selected { ">" } else { " " };
            let override_mode = tp.path_overrides.get(path.as_str()).copied();
            let (marker, color) = match (override_mode, *on) {
                (Some(PathOverride::ForcedOn), _) => ("[MANUAL INCLUDE]", Color::LightGreen),
                (Some(PathOverride::ForcedOff), _) => ("[MANUAL EXCLUDE]", Color::LightRed),
                (None, true) => ("[AUTO INCLUDE]", Color::Green),
                (None, false) => ("[AUTO EXCLUDE]", Color::DarkGray),
            };
            let mode = if override_mode.is_some() {
                "manual"
            } else {
                "auto"
            };
            let path_style = if selected && app.types_path_focus {
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };
            lines.push(Line::from(vec![
                Span::raw(format!("{} ", sel)),
                Span::styled(marker, Style::default().fg(color)),
                Span::raw(" "),
                Span::styled(format!("{} ({})", path, mode), path_style),
            ]));
        }
        if total_paths > path_window {
            lines.push(Line::from(Span::styled(
                format!("rows {}/{}", selected_path.saturating_add(1), total_paths),
                Style::default().fg(Color::Gray),
            )));
        }

        lines.push(Line::from(""));
    }

    frame.render_widget(
        Paragraph::new(Text::from(lines))
            .wrap(Wrap { trim: true })
            .block(
                Block::default()
                    .title(type_details_title(app, cols[1].width))
                    .borders(Borders::ALL),
            ),
        cols[1],
    );
}

fn draw_data(frame: &mut Frame<'_>, area: Rect, app: &mut App, max_type_count: f64) {
    app.ensure_baseline_cache();
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(40), Constraint::Percentage(60)])
        .split(area);

    let rows = app.visible_baseline_events();
    let total = rows.len();
    let selected = if total == 0 {
        0
    } else {
        app.data_index.min(total.saturating_sub(1))
    };
    let window = cols[0].height.saturating_sub(2) as usize;
    let start = if total <= window {
        0
    } else {
        let half = window / 2;
        selected
            .saturating_sub(half)
            .min(total.saturating_sub(window))
    };
    let index_width = total.max(1).to_string().len().max(3);
    let type_col_width = rows
        .iter()
        .map(|e| app.model.canonical_type_name(&e.type_id).chars().count() + 2)
        .max()
        .unwrap_or(16)
        .clamp(12, 36);
    let first_baseline_ts = rows.last().map(|e| e.ts).unwrap_or(0.0);
    let mut items = Vec::new();
    let list_inner_width = cols[0].width.saturating_sub(2) as usize;
    for (vis_idx, e) in rows.iter().skip(start).take(window).enumerate() {
        let row = start + vis_idx + 1;
        let is_selected = row - 1 == selected;
        let diff_ms = Some((((e.ts - first_baseline_ts) * 1000.0).round() as i64).max(0));
        items.push(ListItem::new(render_event_line(
            app,
            e,
            Some(row),
            index_width,
            type_col_width,
            diff_ms,
            is_selected,
            list_inner_width,
            max_type_count,
        )));
    }
    frame.render_widget(
        List::new(items).block(
            Block::default()
                .title(format!(
                    "Baseline  row {}/{}",
                    selected.saturating_add(1),
                    total
                ))
                .borders(Borders::ALL),
        ),
        cols[0],
    );

    let (preview_text, preview_scroll) = if let Some(sel) = rows.get(selected) {
        let mut lines = vec![Line::from(Span::styled(
            app.model.type_display_name(&sel.type_id),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ))];
        lines.push(Line::from(""));
        let considered_paths = app
            .model
            .types
            .get(&sel.type_id)
            .map(|tp| &tp.considered_paths);
        let sub_lc = app.event_filters.substring_filter.to_lowercase();
        let whitelist_terms = if app.whitelist_highlight_enabled() {
            app.whitelist_terms()
        } else {
            &[]
        };
        let rendered = render_json_keypicker(
            &sel.obj,
            None,
            false,
            false,
            &app.event_filters.key_filter,
            &sub_lc,
            whitelist_terms,
            considered_paths,
        );
        let scroll = selected_json_scroll(rendered.selected_line, cols[1].height);
        lines.extend(rendered.lines);
        (Text::from(lines), scroll)
    } else {
        (Text::from("No baseline event selected"), 0)
    };
    frame.render_widget(
        Paragraph::new(preview_text)
            .scroll((preview_scroll, 0))
            .wrap(Wrap { trim: false })
            .block(
                Block::default()
                    .title("selected JSON")
                    .borders(Borders::ALL),
            ),
        cols[1],
    );
}

fn draw_controls(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let mut text = Vec::new();
    let inner_width = area.width.saturating_sub(2) as usize;
    if app.input_mode != InputMode::None {
        let title = match app.input_mode {
            InputMode::None => "",
            InputMode::Label => "set label",
            InputMode::EventFilter(field) => match field {
                FilterField::Key => "keys (expr: a && !b, a || b)",
                FilterField::Type => "type (expr: login && !debug)",
                FilterField::Fuzzy => "fuzzy (expr: abc && !xyz)",
                FilterField::Exact => "exact key=value (expr supported)",
                FilterField::Substring => "substring (expr: foo && !bar)",
            },
            InputMode::TypesFilter => "type list filter",
            InputMode::RenameType => "rename type",
            InputMode::InsertPeriodRange => {
                "insert period in format <row_start>-<row_end> (e.g. 234-268)"
            }
            InputMode::EditPeriodRange => {
                "edit period in format <row_start>-<row_end> (e.g. 234-268)"
            }
            InputMode::ExportSessionPath => "export session path (Enter to write)",
            InputMode::ExportProfilePath => "export profile path (Enter to write)",
        };
        text.push(Line::from(vec![
            Span::styled(format!("{}: ", title), Style::default().fg(Color::Yellow)),
            Span::raw(app.input_buffer.clone()),
        ]));
    } else if app.should_show_status_line() && !app.status.is_empty() {
        let max_status = inner_width.saturating_sub(10).max(16);
        text.push(Line::from(vec![
            Span::styled("status: ", Style::default().fg(Color::Yellow)),
            Span::styled(
                truncate_text(&app.status, max_status),
                Style::default().fg(Color::LightGreen),
            ),
        ]));
        text.push(Line::from(""));
    } else if let Some(hint) = app.startup_hint() {
        let max_hint = inner_width.saturating_sub(8).max(16);
        text.push(Line::from(vec![
            Span::styled("hint: ", Style::default().fg(Color::Yellow)),
            Span::styled(
                truncate_text(hint, max_hint),
                Style::default().fg(Color::LightGreen),
            ),
        ]));
        text.push(Line::from(""));
    } else {
        let mut row = Vec::new();
        let action_on = app.model.active_period().is_some();
        let filters_active = app.event_filters.active_count();
        let wide = inner_width >= 130;
        let medium = inner_width >= 105;
        let export_state = if medium { "ready" } else { "ok" };

        row.push(Span::styled(
            if wide { "action (m)" } else { "m" },
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ));
        row.push(Span::raw(":"));
        row.push(Span::styled(
            if action_on { "ON" } else { "OFF" },
            Style::default().fg(if action_on {
                Color::LightGreen
            } else {
                Color::DarkGray
            }),
        ));
        row.push(Span::raw("  "));

        row.push(Span::styled(
            if wide { "follow (f)" } else { "f" },
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ));
        row.push(Span::raw(":"));
        row.push(Span::styled(
            if app.live_follow { "ON" } else { "OFF" },
            Style::default().fg(if app.live_follow {
                Color::LightGreen
            } else {
                Color::DarkGray
            }),
        ));
        row.push(Span::raw("  "));

        row.push(Span::styled(
            if wide { "help (h)" } else { "h" },
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ));
        row.push(Span::raw(":"));
        row.push(Span::styled("show", Style::default().fg(Color::Gray)));
        row.push(Span::raw("  "));

        row.push(Span::styled(
            if wide { "clear (c)" } else { "c" },
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ));
        row.push(Span::raw(":"));
        row.push(Span::styled("ready", Style::default().fg(Color::Gray)));
        row.push(Span::raw("  "));

        row.push(Span::styled(
            if wide { "filters (y)" } else { "y" },
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ));
        row.push(Span::raw(":"));
        let filters_on = !app.filters_suspended() && filters_active > 0;
        row.push(Span::styled(
            if filters_on { "ON" } else { "OFF" },
            Style::default().fg(if filters_on {
                Color::LightGreen
            } else {
                Color::DarkGray
            }),
        ));
        row.push(Span::raw("  "));
        row.push(Span::styled(
            if wide { "whitelist (w)" } else { "w" },
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ));
        row.push(Span::raw(":"));
        let w_color = if !app.whitelist_loaded() || app.whitelist_mode_label() == "off" {
            Color::DarkGray
        } else {
            Color::LightGreen
        };
        row.push(Span::styled(app.whitelist_mode_label(), Style::default().fg(w_color)));
        row.push(Span::raw("  "));
        row.push(Span::styled(
            if wide { "export session (x)" } else { "x" },
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ));
        row.push(Span::raw(":"));
        row.push(Span::styled(export_state, Style::default().fg(Color::Gray)));
        row.push(Span::raw("  "));
        row.push(Span::styled(
            if wide { "export profile (p)" } else { "p" },
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ));
        row.push(Span::raw(":"));
        row.push(Span::styled(export_state, Style::default().fg(Color::Gray)));

        text.push(Line::from(row));
    }
    let width = area.width.saturating_sub(2) as usize;
    let filters_active = app.event_filters.active_count();
    let show_long_names = width >= 100;
    let key = display_filter(&app.event_filters.key_filter);
    let typ = if app.event_filters.type_filter.is_empty() {
        "off".to_string()
    } else {
        truncate_text(
            &app.model
                .display_type_filter_value(&app.event_filters.type_filter),
            20,
        )
    };
    let substring = display_filter(&app.event_filters.substring_filter);
    let fuzzy = display_filter(&app.event_filters.fuzzy_filter);
    let exact = display_filter(&app.event_filters.exact_filter);
    let mut row = vec![Span::raw(" ")];
    let mut push_value = |label: &str, value: String, active: bool, idx: usize| {
        if idx > 0 {
            row.push(Span::raw("  "));
        }
        row.push(Span::styled(
            format!("{}=", label),
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ));
        row.push(Span::styled(
            value,
            Style::default().fg(if active {
                Color::LightGreen
            } else {
                Color::DarkGray
            }),
        ));
    };

    let labels = if show_long_names {
        ["k/key", "t/type", "//sub", "z/fuzzy", "e/exact"]
    } else {
        ["k", "t", "/", "z", "e"]
    };
    push_value(labels[0], key, !app.event_filters.key_filter.is_empty(), 0);
    push_value(labels[1], typ, !app.event_filters.type_filter.is_empty(), 1);
    push_value(
        labels[2],
        substring,
        !app.event_filters.substring_filter.is_empty(),
        2,
    );
    push_value(
        labels[3],
        fuzzy,
        !app.event_filters.fuzzy_filter.is_empty(),
        3,
    );
    push_value(
        labels[4],
        exact,
        !app.event_filters.exact_filter.is_empty(),
        4,
    );
    row.push(Span::raw("  "));
    row.push(Span::styled("state:", Style::default().fg(Color::Gray)));
    if app.filters_suspended() {
        row.push(Span::styled(
            "suspended",
            Style::default().fg(Color::Yellow),
        ));
    } else {
        row.push(Span::styled(
            format!("active:{}/5", filters_active),
            Style::default().fg(if filters_active > 0 {
                Color::LightGreen
            } else {
                Color::DarkGray
            }),
        ));
    }
    let sep = "─".repeat(width.max(1));
    text.push(Line::from(sep));
    text.push(Line::from(row));

    frame.render_widget(
        Paragraph::new(Text::from(text)).block(Block::default().title("Controls").borders(Borders::ALL)),
        area,
    );
}

fn display_filter(value: &str) -> String {
    if value.is_empty() {
        "off".to_string()
    } else {
        truncate_text(value, 20)
    }
}

fn draw_full_help(frame: &mut Frame<'_>, app: &App) {
    let popup = centered_rect(88, 88, frame.area());
    frame.render_widget(Clear, popup);
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(4), Constraint::Min(8)])
        .split(popup);

    frame.render_widget(
        Paragraph::new(Text::from(vec![
            Line::from(Span::styled(
                "Quick Help",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            )),
            Line::from("Press h or ? to close this help."),
        ]))
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Discoverability"),
        ),
        rows[0],
    );

    let body = vec![
        Line::from("Global"),
        Line::from(if app.baseline_tab_enabled() {
            "  q quit (press twice) | h/? help | 1 Live | 2 Periods | 3 Types | 4 Baseline"
        } else {
            "  q quit (press twice) | h/? help | 1 Live | 2 Periods | 3 Types"
        }),
        Line::from("  x export session now | p export profile now"),
        Line::from(""),
        Line::from("Live"),
        Line::from("  m toggle action period"),
        Line::from("  f toggle follow"),
        Line::from("  up/down move cursor (disables follow)"),
        Line::from("  right or enter focus key selection in selected-object pane"),
        Line::from("  left returns from key selection to event list"),
        Line::from("  Home/End jump to top/bottom"),
        Line::from("  PageUp/PageDown move viewport and cursor (Ctrl+U / Ctrl+D also)"),
        Line::from("  with key focus: up/down select key, k apply key filter, t jump to type"),
        Line::from("  k/t///z/e set filters (/ substring, z fuzzy), c clear filters, y suspend/restore filters, w cycle whitelist"),
        Line::from("  filter expression syntax: AND with &&, OR with ||, negate with !"),
        Line::from("  examples: type 'login && !debug' | exact 'user=alice && !event=logout'"),
        Line::from(""),
        Line::from("Periods"),
        Line::from("  3 panes: periods | events | selected JSON"),
        Line::from("  enter/right move focus right, left move focus left"),
        Line::from("  up/down choose row in active pane"),
        Line::from("  with periods focus: i insert start-end, e edit selected, d delete selected (asks confirm)"),
        Line::from(""),
        Line::from("Types"),
        Line::from("  / filter types by id or name"),
        Line::from("  t apply selected type as event filter and jump to Live"),
        Line::from("  after t: esc returns to Types"),
        Line::from("  r rename selected type"),
        Line::from("  j popup preview of first-seen sample object"),
        Line::from("  enter/right focus paths, left return to type list"),
        Line::from("  with path focus: up/down choose path, space toggle include/exclude"),
        Line::from("  u toggle selected type in negative type filter (!\"type name\")"),
        Line::from(""),
        Line::from("Baseline"),
        Line::from("  up/down scroll | enter inspect"),
        Line::from("  k keys filter, t type filter, / substring filter, z fuzzy filter, e exact path=value, w whitelist"),
    ];
    frame.render_widget(
        Paragraph::new(Text::from(body))
            .wrap(Wrap { trim: true })
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Commands by Mode"),
            ),
        rows[1],
    );
}

fn draw_inspector(frame: &mut Frame<'_>, inspector: &ObjectInspector, app: &App) {
    let popup = centered_rect(80, 80, frame.area());
    frame.render_widget(Clear, popup);

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(4), Constraint::Min(8)])
        .split(popup);

    let name = app.model.type_display_name(&inspector.event.type_id);
    frame.render_widget(
        Paragraph::new(Text::from(vec![
            Line::from(Span::styled(
                name,
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            )),
            Line::from(
                "Select key and press k to filter events by key, t to jump to type, esc to close",
            ),
        ]))
        .block(
            Block::default()
                .title("Object Inspector")
                .borders(Borders::ALL),
        ),
        rows[0],
    );

    let selected_path = inspector.key_paths.get(inspector.key_index);
    let sub_lc = app.event_filters.substring_filter.to_lowercase();
    let whitelist_terms = if app.whitelist_highlight_enabled() {
        app.whitelist_terms()
    } else {
        &[]
    };
    let rendered = render_json_keypicker(
        &inspector.event.obj,
        selected_path,
        true,
        false,
        &app.event_filters.key_filter,
        &sub_lc,
        whitelist_terms,
        app.model
            .types
            .get(&inspector.event.type_id)
            .map(|tp| &tp.considered_paths),
    );
    let scroll = selected_json_scroll(rendered.selected_line, rows[1].height);
    frame.render_widget(
        Paragraph::new(Text::from(rendered.lines))
            .scroll((scroll, 0))
            .wrap(Wrap { trim: false })
            .block(Block::default().title("Object").borders(Borders::ALL)),
        rows[1],
    );
}

fn draw_type_preview_modal(frame: &mut Frame<'_>, app: &App) {
    let popup = centered_rect(78, 82, frame.area());
    frame.render_widget(Clear, popup);
    let visible = app.visible_types();
    let selected_idx = app.type_index.min(visible.len().saturating_sub(1));
    let selected = visible.get(selected_idx).and_then(|type_id| {
        app.model
            .types
            .get(type_id)
            .map(|tp| (type_id.as_str(), tp.example.clone()))
    });
    let mut lines: Vec<Line<'static>> = Vec::new();
    if let Some((type_id, sample)) = selected {
        lines.push(Line::from(Span::styled(
            app.model.canonical_type_name(type_id),
            Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
        )));
        lines.push(Line::from(""));
        let rendered = render_json_keypicker(&sample, None, false, false, "", "", &[], None);
        lines.extend(rendered.lines);
    } else {
        lines.push(Line::from("No type selected"));
    }
    frame.render_widget(
        Paragraph::new(Text::from(lines))
            .wrap(Wrap { trim: false })
            .block(
                Block::default()
                    .title("Type Sample Preview (j/Esc to close)")
                    .borders(Borders::ALL),
            ),
        popup,
    );
}

fn render_json_keypicker(
    value: &serde_json::Value,
    selected_path: Option<&String>,
    _focused: bool,
    value_focus: bool,
    active_key_filter: &str,
    substring_filter: &str,
    whitelist_terms: &[String],
    considered_paths: Option<&IndexMap<String, bool>>,
) -> JsonRender {
    let mut lines = Vec::new();
    let mut selected_line = None;
    render_json_value_lines(
        value,
        "",
        0,
        true,
        selected_path.map(|s| s.as_str()),
        value_focus,
        active_key_filter,
        substring_filter,
        whitelist_terms,
        considered_paths,
        &mut lines,
        &mut selected_line,
    );
    JsonRender {
        lines,
        selected_line,
    }
}

fn render_json_value_lines(
    value: &serde_json::Value,
    path: &str,
    indent: usize,
    is_last: bool,
    selected_path: Option<&str>,
    value_focus: bool,
    active_key_filter: &str,
    substring_filter: &str,
    whitelist_terms: &[String],
    considered_paths: Option<&IndexMap<String, bool>>,
    out: &mut Vec<Line<'static>>,
    selected_line: &mut Option<usize>,
) {
    match value {
        serde_json::Value::Object(map) => {
            out.push(Line::from(format!("{}{{", "  ".repeat(indent))));
            let len = map.len();
            for (idx, (k, child)) in map.iter().enumerate() {
                let key_path = if path.is_empty() {
                    k.clone()
                } else {
                    format!("{}.{}", path, k)
                };
                let child_is_last = idx + 1 == len;
                render_json_keyed_value_line(
                    Some(k),
                    child,
                    &key_path,
                    indent + 1,
                    child_is_last,
                    selected_path,
                    value_focus,
                    active_key_filter,
                    substring_filter,
                    whitelist_terms,
                    considered_paths,
                    out,
                    selected_line,
                );
            }
            let tail = if is_last { "}" } else { "}," };
            out.push(Line::from(format!("{}{}", "  ".repeat(indent), tail)));
        }
        serde_json::Value::Array(arr) => {
            out.push(Line::from(format!("{}[", "  ".repeat(indent))));
            for (idx, child) in arr.iter().enumerate() {
                let child_is_last = idx + 1 == arr.len();
                let child_path = if path.is_empty() {
                    "[]".to_string()
                } else {
                    format!("{}[]", path)
                };
                render_json_keyed_value_line(
                    None,
                    child,
                    &child_path,
                    indent + 1,
                    child_is_last,
                    selected_path,
                    value_focus,
                    active_key_filter,
                    substring_filter,
                    whitelist_terms,
                    considered_paths,
                    out,
                    selected_line,
                );
            }
            let tail = if is_last { "]" } else { "]," };
            out.push(Line::from(format!("{}{}", "  ".repeat(indent), tail)));
        }
        _ => {
            let value_text = serde_json::to_string(value).unwrap_or_else(|_| "null".to_string());
            let tail = if is_last { "" } else { "," };
            let highlight = !substring_filter.is_empty()
                && value_text.to_lowercase().contains(substring_filter);
            let wl_highlight = matches_any_term(&value_text.to_lowercase(), whitelist_terms);
            let base = json_value_style(value);
            let mut line = vec![Span::raw("  ".repeat(indent))];
            if highlight || wl_highlight {
                line.extend(highlight_text_spans(
                    &value_text,
                    substring_filter,
                    whitelist_terms,
                    base,
                ));
            } else {
                line.push(Span::styled(value_text, base));
            }
            line.push(Span::styled(tail, json_punctuation_style()));
            out.push(Line::from(line));
        }
    }
}

fn push_open_bracket(
    mut prefix: Vec<Span<'static>>,
    bracket: &'static str,
    sel_or_filt: bool,
    punctuation_override: Style,
    out: &mut Vec<Line<'static>>,
) {
    prefix.push(Span::styled(
        bracket,
        if sel_or_filt {
            punctuation_override
        } else {
            json_punctuation_style()
        },
    ));
    out.push(Line::from(prefix));
}

fn push_close_bracket(
    indent: usize,
    open: char,
    is_last: bool,
    sel_or_filt: bool,
    punctuation_override: Style,
    out: &mut Vec<Line<'static>>,
) {
    let tail = if is_last {
        format!("{}", open)
    } else {
        format!("{},", open)
    };
    out.push(Line::from(vec![
        Span::raw("  ".repeat(indent)),
        Span::styled(
            tail,
            if sel_or_filt {
                punctuation_override
            } else {
                json_punctuation_style()
            },
        ),
    ]));
}

fn render_json_keyed_value_line(
    key: Option<&str>,
    value: &serde_json::Value,
    path: &str,
    indent: usize,
    is_last: bool,
    selected_path: Option<&str>,
    value_focus: bool,
    active_key_filter: &str,
    substring_filter: &str,
    whitelist_terms: &[String],
    considered_paths: Option<&IndexMap<String, bool>>,
    out: &mut Vec<Line<'static>>,
    selected_line: &mut Option<usize>,
) {
    let selected = selected_path == Some(path);
    let filtered = !active_key_filter.is_empty() && active_key_filter == path;
    let normalized_out = is_path_normalized_out(considered_paths, path);
    let key_highlight = !substring_filter.is_empty()
        && key.map(|k| k.to_lowercase().contains(substring_filter)).unwrap_or(false);
    let key_whitelist_highlight = key
        .map(|k| matches_any_term(&k.to_lowercase(), whitelist_terms))
        .unwrap_or(false);
    let key_override = if selected {
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD | Modifier::UNDERLINED)
    } else if filtered {
        Style::default()
            .fg(Color::LightGreen)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default()
    };
    let key_override = apply_normalized_out_style(key_override, normalized_out);
    let punctuation_override = if selected {
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD)
    } else if filtered {
        Style::default().fg(Color::LightGreen)
    } else {
        Style::default()
    };
    let punctuation_override = apply_normalized_out_style(punctuation_override, normalized_out);

    let mut prefix = vec![Span::raw("  ".repeat(indent))];
    if let Some(k) = key {
        let key_base = apply_normalized_out_style(json_key_base_style(), normalized_out);
        if selected || filtered {
            prefix.push(Span::styled(format!("\"{k}\""), key_override));
        } else if key_highlight || key_whitelist_highlight {
            prefix.push(Span::raw("\""));
            prefix.extend(highlight_text_spans(
                k,
                substring_filter,
                whitelist_terms,
                key_base,
            ));
            prefix.push(Span::raw("\""));
        } else {
            prefix.push(Span::styled(format!("\"{k}\""), key_base));
        }
        prefix.push(Span::styled(
            ": ",
            apply_normalized_out_style(json_punctuation_style(), normalized_out),
        ));
    }

    let sel_or_filt = selected || filtered;
    match value {
        serde_json::Value::Object(map) => {
            if selected && selected_line.is_none() {
                *selected_line = Some(out.len());
            }
            push_open_bracket(prefix, "{", sel_or_filt, punctuation_override, out);
            let len = map.len();
            for (idx, (k, child)) in map.iter().enumerate() {
                let key_path = format!("{}.{}", path, k);
                render_json_keyed_value_line(
                    Some(k),
                    child,
                    &key_path,
                    indent + 1,
                    idx + 1 == len,
                    selected_path,
                    value_focus,
                    active_key_filter,
                    substring_filter,
                    whitelist_terms,
                    considered_paths,
                    out,
                    selected_line,
                );
            }
            push_close_bracket(indent, '}', is_last, sel_or_filt, punctuation_override, out);
        }
        serde_json::Value::Array(arr) => {
            if selected && selected_line.is_none() {
                *selected_line = Some(out.len());
            }
            push_open_bracket(prefix, "[", sel_or_filt, punctuation_override, out);
            for (idx, child) in arr.iter().enumerate() {
                let child_path = format!("{}[]", path);
                render_json_keyed_value_line(
                    None,
                    child,
                    &child_path,
                    indent + 1,
                    idx + 1 == arr.len(),
                    selected_path,
                    value_focus,
                    active_key_filter,
                    substring_filter,
                    whitelist_terms,
                    considered_paths,
                    out,
                    selected_line,
                );
            }
            push_close_bracket(indent, ']', is_last, sel_or_filt, punctuation_override, out);
        }
        _ => {
            if selected && selected_line.is_none() {
                *selected_line = Some(out.len());
            }
            let mut line = prefix;
            let value_text = serde_json::to_string(value).unwrap_or_else(|_| "null".to_string());
            let value_highlight = !substring_filter.is_empty()
                && value_text.to_lowercase().contains(substring_filter);
            let value_whitelist_highlight =
                matches_any_term(&value_text.to_lowercase(), whitelist_terms);
            let base_value_style = apply_normalized_out_style(
                if filtered {
                    Style::default().fg(Color::LightGreen)
                } else {
                    json_value_style(value)
                },
                normalized_out,
            );
            let value_override = if selected && value_focus {
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD | Modifier::UNDERLINED)
            } else if selected {
                Style::default().fg(Color::Yellow)
            } else {
                base_value_style
            };
            let value_override = apply_normalized_out_style(value_override, normalized_out);
            if !selected && !filtered && (value_highlight || value_whitelist_highlight) {
                line.extend(highlight_text_spans(
                    &value_text,
                    substring_filter,
                    whitelist_terms,
                    base_value_style,
                ));
            } else {
                line.push(Span::styled(value_text, value_override));
            }
            if !is_last {
                line.push(Span::styled(
                    ",",
                    apply_normalized_out_style(json_punctuation_style(), normalized_out),
                ));
            }
            out.push(Line::from(line));
        }
    }
}

fn matches_any_term(text_lc: &str, terms: &[String]) -> bool {
    terms.iter().any(|needle| text_lc.contains(needle))
}

fn highlight_text_spans(
    text: &str,
    substring_filter: &str,
    whitelist_terms: &[String],
    base_style: Style,
) -> Vec<Span<'static>> {
    if !text.is_ascii() {
        return vec![Span::styled(text.to_string(), base_style)];
    }
    let lower = text.to_ascii_lowercase();
    let mut spans = Vec::new();
    let mut i = 0usize;
    while i < text.len() {
        let mut best: Option<(usize, usize, Style)> = None;
        if !substring_filter.is_empty() {
            if let Some(pos) = lower[i..].find(substring_filter) {
                let start = i + pos;
                let end = start + substring_filter.len();
                best = Some((start, end, Style::default().fg(Color::Black).bg(Color::Yellow)));
            }
        }
        for needle in whitelist_terms {
            if needle.is_empty() {
                continue;
            }
            if let Some(pos) = lower[i..].find(needle) {
                let start = i + pos;
                let end = start + needle.len();
                let candidate = (start, end, Style::default().fg(Color::Black).bg(Color::Rgb(255, 140, 0)));
                best = match best {
                    None => Some(candidate),
                    Some(cur) => {
                        if candidate.0 < cur.0 {
                            Some(candidate)
                        } else {
                            Some(cur)
                        }
                    }
                };
            }
        }
        let Some((start, end, hl_style)) = best else {
            spans.push(Span::styled(text[i..].to_string(), base_style));
            break;
        };
        if start > i {
            spans.push(Span::styled(text[i..start].to_string(), base_style));
        }
        spans.push(Span::styled(text[start..end].to_string(), hl_style));
        i = end;
    }
    spans
}

fn selected_json_scroll(selected_line: Option<usize>, pane_height: u16) -> u16 {
    let view_rows = pane_height.saturating_sub(2) as usize;
    if view_rows == 0 {
        return 0;
    }
    let Some(line) = selected_line else {
        return 0;
    };
    let half = view_rows / 2;
    line.saturating_sub(half).min(u16::MAX as usize) as u16
}

fn is_path_normalized_out(considered_paths: Option<&IndexMap<String, bool>>, path: &str) -> bool {
    considered_paths
        .and_then(|paths| paths.get(path).copied())
        .map(|is_considered| !is_considered)
        .unwrap_or(false)
}

fn apply_normalized_out_style(style: Style, normalized_out: bool) -> Style {
    if normalized_out {
        style.fg(Color::Gray).add_modifier(Modifier::DIM)
    } else {
        style
    }
}

fn json_key_base_style() -> Style {
    Style::default().fg(Color::Cyan)
}

fn json_value_style(value: &serde_json::Value) -> Style {
    match value {
        serde_json::Value::String(_) => Style::default().fg(Color::Green),
        serde_json::Value::Number(_) => Style::default().fg(Color::Rgb(255, 165, 0)),
        serde_json::Value::Bool(_) => Style::default().fg(Color::LightRed),
        serde_json::Value::Null => Style::default()
            .fg(Color::DarkGray)
            .add_modifier(Modifier::ITALIC),
        _ => Style::default(),
    }
}

fn json_punctuation_style() -> Style {
    Style::default().fg(Color::Gray)
}

fn render_event_line(
    app: &App,
    e: &EventRecord,
    row_index: Option<usize>,
    index_width: usize,
    type_col_width: usize,
    diff_ms: Option<i64>,
    selected: bool,
    row_width: usize,
    max_type_count: f64,
) -> Line<'static> {
    let name = app.model.canonical_type_name(&e.type_id);
    let type_count = app
        .model
        .types
        .get(&e.type_id)
        .map(|t| t.count)
        .unwrap_or(1) as f64;
    let mut style = event_style(e, type_count, max_type_count);
    if selected {
        style = style.add_modifier(Modifier::UNDERLINED | Modifier::BOLD);
    }
    let whitelist_hit = app.whitelist_highlight_enabled() && app.whitelist_matches_event(e);
    let action_marker = if e.in_action_period {
        Span::styled("  ", Style::default().bg(Color::Red))
    } else {
        Span::raw("  ")
    };
    let sel = if selected { "->" } else { "  " };
    let name_style = if whitelist_hit {
        style
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD | Modifier::UNDERLINED)
    } else {
        style
    };
    let row_label = row_index
        .map(|idx| format!("{:>width$}", idx, width = index_width))
        .unwrap_or_else(|| " ".repeat(index_width));
    let diff_label = diff_ms
        .map(|ms| format!("+{}ms", ms.max(0)))
        .unwrap_or_else(|| "+0ms".to_string());
    let type_block = format!("[{}]", name);
    // Build the metrics block text first so tail_len comes from the actual rendered string,
    // not from manually summed constants that can drift out of sync with the spans below.
    let metrics = if e.in_action_period {
        let rate_str = format!("{:>5}", format_score(e.live_rate_score));
        let value_str = format!("{:>5}", format_score(e.live_uniq_score));
        let rendered = format!("  R:{}  V:{}", rate_str, value_str);
        Some((rate_str, value_str, rendered))
    } else {
        None
    };
    let tail_len = metrics
        .as_ref()
        .map(|(_, _, rendered)| rendered.chars().count())
        .unwrap_or(0);

    let size_str = format_size_bytes(e.size_bytes);
    let fixed_prefix = 2
        + 1
        + 3
        + row_label.chars().count()
        + 1
        + type_col_width
        + 1
        + size_str.chars().count()
        + 1
        + diff_label.chars().count()
        + 1;
    let short_name = truncate_text(&type_block, type_col_width.max(4));
    let type_cell = if short_name.chars().count() < type_col_width {
        format!("{:<width$}", short_name, width = type_col_width)
    } else {
        short_name
    };
    let line_len = fixed_prefix + tail_len;
    let spacer_len = row_width.saturating_sub(line_len);
    let spacer = " ".repeat(spacer_len);

    let mut spans = vec![
        action_marker,
        Span::raw(" "),
        Span::raw(format!("{} ", sel)),
        Span::styled(row_label, Style::default().fg(Color::Gray)),
        Span::raw(" "),
        Span::styled(type_cell, name_style),
        Span::raw(" "),
        Span::styled(size_str, style),
        Span::raw(" "),
        Span::styled(diff_label, Style::default().fg(Color::Gray)),
    ];
    if let Some((rate_str, value_str, _)) = metrics {
        let rate_color = rate_anomaly_color(anomaly_norm(e.live_rate_score));
        let value_color = value_anomaly_color(anomaly_norm(e.live_uniq_score));
        spans.extend([
            Span::raw(spacer),
            Span::raw("  "),
            Span::styled("R:", Style::default().fg(Color::Gray)),
            Span::styled(rate_str, Style::default().fg(rate_color)),
            Span::raw("  "),
            Span::styled("V:", Style::default().fg(Color::Gray)),
            Span::styled(value_str, Style::default().fg(value_color)),
        ]);
    }
    Line::from(spans)
}

fn event_style(e: &EventRecord, type_count: f64, max_type_count: f64) -> Style {
    let commonness = (type_count / max_type_count.max(1.0))
        .sqrt()
        .clamp(0.0, 1.0);
    let rarity = 1.0 - commonness;
    let base = lerp_rgb((112, 112, 112), (0, 220, 70), rarity);
    let value_anomaly = e.live_uniq_score.clamp(0.0, 1.0);
    let mixed = lerp_rgb(base, (255, 140, 0), value_anomaly * 0.9);

    let mut style = Style::default().fg(Color::Rgb(mixed.0, mixed.1, mixed.2));
    if type_count <= 2.0 {
        style = style.add_modifier(Modifier::BOLD);
    }
    style
}

fn lerp_rgb(a: (u8, u8, u8), b: (u8, u8, u8), t: f64) -> (u8, u8, u8) {
    let tt = t.clamp(0.0, 1.0);
    let lerp = |x: u8, y: u8| -> u8 { (x as f64 + (y as f64 - x as f64) * tt).round() as u8 };
    (lerp(a.0, b.0), lerp(a.1, b.1), lerp(a.2, b.2))
}

fn format_size_bytes(n: u32) -> String {
    if n < 10_000 {
        format!("{:>4}B", n)
    } else if n < 1_000_000 {
        format!("{:>4}k", n / 1000)
    } else {
        format!("{:>4}M", n / 1_000_000)
    }
}

fn truncate_text(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    let keep = max_chars.saturating_sub(3);
    let mut out: String = text.chars().take(keep).collect();
    out.push_str("...");
    out
}

fn anomaly_norm(score: f64) -> f64 {
    score.clamp(0.0, 1.0).sqrt()
}

fn format_score(score: f64) -> String {
    format!("{:.2}", score)
}

fn value_anomaly_color(norm: f64) -> Color {
    let c = lerp_rgb((145, 145, 145), (255, 110, 0), norm);
    Color::Rgb(c.0, c.1, c.2)
}

fn rate_anomaly_color(norm: f64) -> Color {
    let c = lerp_rgb((145, 145, 145), (0, 160, 255), norm);
    Color::Rgb(c.0, c.1, c.2)
}

fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}

fn draw_confirmation_modal(frame: &mut Frame<'_>, confirm: &ModalConfirmation) {
    let mut content_width = modal_rendered_width(&confirm.title);
    for line in &confirm.lines {
        content_width = content_width.max(modal_rendered_width(line));
    }
    let max_w = frame.area().width.saturating_sub(2) as usize;
    let target_w = (content_width + 6).clamp(44, max_w.max(44)) as u16;
    let text_rows = confirm.lines.len() + 2; // title + spacer + body
    let max_h = frame.area().height.saturating_sub(2) as usize;
    let target_h = (text_rows + 3).clamp(8, max_h.max(8)) as u16;
    let popup = centered_rect_abs(target_w, target_h, frame.area());
    frame.render_widget(Clear, popup);
    let mut lines = vec![Line::from(Span::styled(
        confirm.title.clone(),
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD),
    ))];
    lines.push(Line::from(""));
    lines.extend(confirm.lines.iter().map(|s| stylize_modal_line(s)));
    frame.render_widget(
        Paragraph::new(Text::from(lines))
            .wrap(Wrap { trim: true })
            .block(Block::default().borders(Borders::ALL).title("Confirmation")),
        popup,
    );
}

fn modal_rendered_width(s: &str) -> usize {
    s.chars().filter(|c| *c != '`').count()
}

fn centered_rect_abs(width: u16, height: u16, area: Rect) -> Rect {
    let w = width.min(area.width.saturating_sub(2)).max(1);
    let h = height.min(area.height.saturating_sub(2)).max(1);
    let x = area.x + area.width.saturating_sub(w) / 2;
    let y = area.y + area.height.saturating_sub(h) / 2;
    Rect {
        x,
        y,
        width: w,
        height: h,
    }
}

fn tab_title(hotkey: &'static str, label: &'static str) -> Line<'static> {
    Line::from(vec![styled_hotkey(hotkey), Span::raw(format!(" {}", label))])
}

fn stylize_modal_line(s: &str) -> Line<'static> {
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < s.len() {
        if s.as_bytes()[i] == b'`' {
            if let Some(end_rel) = s[i + 1..].find('`') {
                let end = i + 1 + end_rel;
                let token = &s[i + 1..end];
                out.push(Span::styled(
                    token.to_string(),
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ));
                i = end + 1;
                continue;
            }
        }
        let next_tick = s[i..].find('`').map(|p| i + p).unwrap_or(s.len());
        out.push(Span::raw(s[i..next_tick].to_string()));
        i = next_tick;
    }
    Line::from(out)
}
