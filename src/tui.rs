use crate::app::{App, ObjectInspector, PeriodsFocus};
use crate::domain::{EventRecord, FilterField, PathOverride};
use indexmap::IndexMap;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, Paragraph, Tabs, Wrap};
use ratatui::Frame;

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
}

pub fn draw_ui(frame: &mut Frame<'_>, app: &mut App) {
    let root = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(10),
            Constraint::Length(3),
            Constraint::Length(3),
        ])
        .split(frame.area());

    // Compute once per frame; passed to every row renderer to avoid O(types) per row.
    let max_type_count = app.model.types.values().map(|t| t.count).max().unwrap_or(1) as f64;

    draw_tabs(frame, root[0], app.mode);
    match app.mode {
        UiMode::Live => draw_live(frame, root[1], app, max_type_count),
        UiMode::Periods => draw_periods(frame, root[1], app, max_type_count),
        UiMode::Types => draw_types(frame, root[1], app),
        UiMode::Data => draw_data(frame, root[1], app, max_type_count),
    }
    draw_status(frame, root[2], app);
    draw_help(frame, root[3], app);

    if let Some(inspector) = app.inspector.as_ref() {
        draw_inspector(frame, inspector, app);
    }
    if app.show_help_overlay {
        draw_full_help(frame);
    }
}

fn draw_tabs(frame: &mut Frame<'_>, area: Rect, mode: UiMode) {
    let titles = ["1 Live", "2 Periods", "3 Types", "4 Baseline"];
    let selected = match mode {
        UiMode::Live => 0,
        UiMode::Periods => 1,
        UiMode::Types => 2,
        UiMode::Data => 3,
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
    let mut items = Vec::new();
    let stream_inner_width = cols[0].width.saturating_sub(2) as usize;
    for (idx, e) in live.rows.iter().enumerate() {
        let selected = Some(idx) == selected_visible;
        items.push(ListItem::new(render_event_line(
            app,
            e,
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

    let preview_text = if let Some(sel) = live.selected {
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
        lines.extend(render_json_keypicker(
            &sel.obj,
            selected_path,
            app.live_key_focus,
            app.live_value_focus,
            &app.event_filters.key_filter,
            considered_paths,
        ));
        Text::from(lines)
    } else {
        Text::from("No event selected")
    };
    let title = selected_json_title(app.live_key_focus, cols[1].width);
    let preview = Paragraph::new(preview_text)
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
        p_items.push(ListItem::new(Line::from(vec![Span::styled(
            format!("#{} {} ({:.2}s)", p.id, p.label, dur),
            style,
        )])));
    }
    frame.render_widget(
        List::new(p_items).block(
            Block::default()
                .title("Action Periods")
                .borders(Borders::ALL),
        ),
        cols[0],
    );

    let mut rows = Vec::new();
    let events_inner_width = cols[1].width.saturating_sub(2) as usize;
    let max_period_rows = (cols[1].height as usize).saturating_sub(2);
    let mut selected_event: Option<&EventRecord> = None;
    if let Some(period) = periods.get(app.periods_index) {
        let start = period.start;
        let end = period.end.unwrap_or(period.start);
        let events = app
            .model
            .filtered_events_in_range(&app.event_filters, Some((start, end)));
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
        for (vis_idx, e) in events.iter().skip(start_idx).take(window).enumerate() {
            let idx = start_idx + vis_idx;
            let selected = idx == app.period_event_index;
            rows.push(ListItem::new(render_event_line(
                app,
                e,
                selected,
                events_inner_width,
                max_type_count,
            )));
        }
        selected_event = events.get(app.period_event_index).copied();
    }
    frame.render_widget(
        List::new(rows).block(Block::default().title("Events").borders(Borders::ALL)),
        cols[1],
    );

    let preview_text = if let Some(sel) = selected_event {
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
        lines.extend(render_json_keypicker(
            &sel.obj,
            selected_path,
            app.periods_focus == PeriodsFocus::Json,
            false,
            &app.event_filters.key_filter,
            considered_paths,
        ));
        Text::from(lines)
    } else {
        Text::from("No event selected")
    };
    frame.render_widget(
        Paragraph::new(preview_text)
            .wrap(Wrap { trim: false })
            .block(
                Block::default()
                    .title(selected_json_title(
                        app.periods_focus == PeriodsFocus::Json,
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

fn selected_json_title(is_key_focus: bool, pane_width: u16) -> Line<'static> {
    if !is_key_focus {
        return Line::from("selected JSON");
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
                Span::raw(")"),
            ]);
        }
        return Line::from(vec![
            Span::raw("Type Details / Paths ("),
            styled_hotkey("t"),
            Span::raw(" filter to live, "),
            styled_hotkey("u"),
            Span::raw(" toggle unrelated)"),
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
            Span::raw(" toggle unrelated)"),
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
        let mut style = Style::default();
        if idx == app.type_index {
            style = if app.types_path_focus {
                style.fg(Color::Gray)
            } else {
                style.fg(Color::Yellow).add_modifier(Modifier::BOLD)
            };
        }
        if tp.known_unrelated {
            style = style.fg(Color::DarkGray);
        }
        let name = app.model.type_display_name(type_id);
        type_items.push(ListItem::new(Line::from(vec![Span::styled(
            format!("{}  count={}", name, tp.count),
            style,
        )])));
    }
    let type_title = format!(
        "Types row {}/{} ({})  [enter/right details, t filter, / search]",
        if total_types == 0 {
            0
        } else {
            selected_type + 1
        },
        total_types,
        if app.types_path_focus {
            "details focus"
        } else {
            "list focus"
        }
    );
    frame.render_widget(
        List::new(type_items).block(Block::default().title(type_title).borders(Borders::ALL)),
        cols[0],
    );

    let mut lines = Vec::new();
    if let Some((type_id, tp)) = visible.get(selected_type) {
        lines.push(Line::from(Span::styled(
            app.model.type_display_name(type_id),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )));
        lines.push(Line::from(""));

        let total_paths = tp.considered_paths.len();
        let selected_path = if total_paths == 0 {
            0
        } else {
            app.path_index.min(total_paths.saturating_sub(1))
        };
        let path_window = (cols[1].height as usize).saturating_sub(15).max(1);
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
        let ex = serde_json::to_string_pretty(&tp.example).unwrap_or_else(|_| "{}".to_string());
        for l in ex.lines().take(12) {
            lines.push(Line::from(l.to_string()));
        }
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

fn draw_data(frame: &mut Frame<'_>, area: Rect, app: &App, max_type_count: f64) {
    let rows = app.visible_baseline_events();
    let start = app.data_index.min(rows.len().saturating_sub(1));
    // 2 border rows + 2 header lines; remaining rows are for events
    let max_event_rows = (area.height as usize).saturating_sub(4);
    let slice = rows.into_iter().skip(start).take(max_event_rows);

    let mut lines = Vec::new();
    let type_filter_display = app
        .model
        .display_type_filter_value(&app.event_filters.type_filter);
    lines.push(Line::from(vec![
        Span::styled("k:", Style::default().fg(Color::Yellow)),
        Span::raw(format!("{}  ", app.event_filters.key_filter)),
        Span::styled("t:", Style::default().fg(Color::Yellow)),
        Span::raw(format!("{}  ", type_filter_display)),
        Span::styled("/:", Style::default().fg(Color::Yellow)),
        Span::raw(format!("{}  ", app.event_filters.fuzzy_filter)),
        Span::styled("e:", Style::default().fg(Color::Yellow)),
        Span::raw(app.event_filters.exact_filter.clone()),
    ]));
    lines.push(Line::from(""));

    let data_inner_width = area.width.saturating_sub(2) as usize;
    for (idx, e) in slice.enumerate() {
        // start = data_index, so the first visible row (idx == 0) is the selected event
        let selected = idx == 0;
        lines.push(render_event_line(
            app,
            e,
            selected,
            data_inner_width,
            max_type_count,
        ));
    }

    frame.render_widget(
        Paragraph::new(Text::from(lines))
            .wrap(Wrap { trim: true })
            .block(
                Block::default()
                    .title("Baseline Explorer")
                    .borders(Borders::ALL),
            ),
        area,
    );
}

fn draw_status(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let mut text = Vec::new();
    let inner_width = area.width.saturating_sub(2) as usize;
    if app.input_mode != InputMode::None {
        let title = match app.input_mode {
            InputMode::None => "",
            InputMode::Label => "set label",
            InputMode::EventFilter(field) => field.title(),
            InputMode::TypesFilter => "type list filter",
            InputMode::RenameType => "rename type",
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
    } else if let Some(hint) = app.startup_hint() {
        let max_hint = inner_width.saturating_sub(8).max(16);
        text.push(Line::from(vec![
            Span::styled("hint: ", Style::default().fg(Color::Yellow)),
            Span::styled(
                truncate_text(hint, max_hint),
                Style::default().fg(Color::LightGreen),
            ),
        ]));
    } else {
        let mut row = Vec::new();
        let action_on = app.model.active_period().is_some();
        let filters_active = app.event_filters.active_count();
        let medium = inner_width >= 95;

        row.push(Span::styled(
            if medium { "action (m)" } else { "m" },
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
            if medium { "follow (f)" } else { "f" },
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
            if medium { "help (h)" } else { "h" },
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ));
        row.push(Span::raw(":"));
        row.push(Span::styled("show", Style::default().fg(Color::Gray)));
        row.push(Span::raw("  "));

        row.push(Span::styled(
            if medium { "filters (y)" } else { "y" },
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

        text.push(Line::from(row));
    }

    frame.render_widget(
        Paragraph::new(Text::from(text))
            .block(Block::default().title("Toggles").borders(Borders::ALL)),
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

fn draw_help(frame: &mut Frame<'_>, area: Rect, app: &App) {
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
    let fuzzy = display_filter(&app.event_filters.fuzzy_filter);
    let exact = display_filter(&app.event_filters.exact_filter);
    let unrelated_count = app
        .model
        .types
        .values()
        .filter(|tp| tp.known_unrelated)
        .count();
    let unrelated = if unrelated_count == 0 {
        "off".to_string()
    } else {
        format!("{}", unrelated_count)
    };

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
        ["k/key", "t/type", "/fuzzy", "e/exact", "u/unrelated"]
    } else {
        ["k", "t", "/", "e", "u"]
    };
    push_value(labels[0], key, !app.event_filters.key_filter.is_empty(), 0);
    push_value(labels[1], typ, !app.event_filters.type_filter.is_empty(), 1);
    push_value(
        labels[2],
        fuzzy,
        !app.event_filters.fuzzy_filter.is_empty(),
        2,
    );
    push_value(
        labels[3],
        exact,
        !app.event_filters.exact_filter.is_empty(),
        3,
    );
    push_value(labels[4], unrelated, unrelated_count > 0, 4);
    row.push(Span::raw("  "));
    row.push(Span::styled("state:", Style::default().fg(Color::Gray)));
    if app.filters_suspended() {
        row.push(Span::styled(
            "suspended",
            Style::default().fg(Color::Yellow),
        ));
    } else {
        row.push(Span::styled(
            format!("active:{}/4", filters_active),
            Style::default().fg(if filters_active > 0 {
                Color::LightGreen
            } else {
                Color::DarkGray
            }),
        ));
    }
    row.push(Span::raw("  "));

    frame.render_widget(
        Paragraph::new(Text::from(vec![Line::from(row)]))
            .block(Block::default().title("Filters").borders(Borders::ALL)),
        area,
    );
}

fn draw_full_help(frame: &mut Frame<'_>) {
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
        Line::from("  q quit | h/? help | 1 Live | 2 Periods | 3 Types | 4 Baseline"),
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
        Line::from("  k/t//e set filters, c clear filters, y toggle filters on/off"),
        Line::from(""),
        Line::from("Periods"),
        Line::from("  3 panes: periods | events | selected JSON"),
        Line::from("  enter/right move focus right, left move focus left"),
        Line::from("  up/down choose row in active pane"),
        Line::from(""),
        Line::from("Types"),
        Line::from("  / filter types by id or name"),
        Line::from("  t apply selected type as event filter and jump to Live"),
        Line::from("  after t: esc returns to Types"),
        Line::from("  r rename selected type"),
        Line::from("  enter/right focus paths, left return to type list"),
        Line::from("  with path focus: up/down choose path, space toggle include/exclude"),
        Line::from("  u mark type known unrelated"),
        Line::from(""),
        Line::from("Baseline"),
        Line::from("  up/down scroll | enter inspect"),
        Line::from("  k keys filter, t type filter, / fuzzy filter, e exact path=value"),
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
    let lines = render_json_keypicker(
        &inspector.event.obj,
        selected_path,
        true,
        false,
        &app.event_filters.key_filter,
        app.model
            .types
            .get(&inspector.event.type_id)
            .map(|tp| &tp.considered_paths),
    );
    frame.render_widget(
        Paragraph::new(Text::from(lines))
            .wrap(Wrap { trim: false })
            .block(Block::default().title("Object").borders(Borders::ALL)),
        rows[1],
    );
}

fn render_json_keypicker(
    value: &serde_json::Value,
    selected_path: Option<&String>,
    _focused: bool,
    value_focus: bool,
    active_key_filter: &str,
    considered_paths: Option<&IndexMap<String, bool>>,
) -> Vec<Line<'static>> {
    let mut lines = Vec::new();
    render_json_value_lines(
        value,
        "",
        0,
        true,
        selected_path.map(|s| s.as_str()),
        value_focus,
        active_key_filter,
        considered_paths,
        &mut lines,
    );
    lines
}

fn render_json_value_lines(
    value: &serde_json::Value,
    path: &str,
    indent: usize,
    is_last: bool,
    selected_path: Option<&str>,
    value_focus: bool,
    active_key_filter: &str,
    considered_paths: Option<&IndexMap<String, bool>>,
    out: &mut Vec<Line<'static>>,
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
                    considered_paths,
                    out,
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
                    considered_paths,
                    out,
                );
            }
            let tail = if is_last { "]" } else { "]," };
            out.push(Line::from(format!("{}{}", "  ".repeat(indent), tail)));
        }
        _ => {
            let value_text = serde_json::to_string(value).unwrap_or_else(|_| "null".to_string());
            let tail = if is_last { "" } else { "," };
            out.push(Line::from(vec![
                Span::raw("  ".repeat(indent)),
                Span::styled(value_text, json_value_style(value)),
                Span::styled(tail, json_punctuation_style()),
            ]));
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
    considered_paths: Option<&IndexMap<String, bool>>,
    out: &mut Vec<Line<'static>>,
) {
    let selected = selected_path == Some(path);
    let filtered = !active_key_filter.is_empty() && active_key_filter == path;
    let normalized_out = is_path_normalized_out(considered_paths, path);
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
        let key_style = if selected || filtered {
            key_override
        } else {
            apply_normalized_out_style(json_key_base_style(), normalized_out)
        };
        prefix.push(Span::styled(format!("\"{k}\""), key_style));
        prefix.push(Span::styled(
            ": ",
            apply_normalized_out_style(json_punctuation_style(), normalized_out),
        ));
    }

    let sel_or_filt = selected || filtered;
    match value {
        serde_json::Value::Object(map) => {
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
                    considered_paths,
                    out,
                );
            }
            push_close_bracket(indent, '}', is_last, sel_or_filt, punctuation_override, out);
        }
        serde_json::Value::Array(arr) => {
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
                    considered_paths,
                    out,
                );
            }
            push_close_bracket(indent, ']', is_last, sel_or_filt, punctuation_override, out);
        }
        _ => {
            let mut line = prefix;
            let value_text = serde_json::to_string(value).unwrap_or_else(|_| "null".to_string());
            let value_override = if selected && value_focus {
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD | Modifier::UNDERLINED)
            } else if selected {
                Style::default().fg(Color::Yellow)
            } else if filtered {
                Style::default().fg(Color::LightGreen)
            } else {
                json_value_style(value)
            };
            let value_override = apply_normalized_out_style(value_override, normalized_out);
            line.push(Span::styled(value_text, value_override));
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
    selected: bool,
    row_width: usize,
    max_type_count: f64,
) -> Line<'static> {
    let name = app.model.type_display_name(&e.type_id);
    let ts = format!("{:.3}", e.ts);
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
    let action_marker = if e.in_action_period {
        Span::styled("  ", Style::default().bg(Color::Red))
    } else {
        Span::raw("  ")
    };
    let sel = if selected { "->" } else { "  " };
    let name_style = style;
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

    let fixed_prefix = 2 + 1 + 3 + ts.chars().count() + 1;
    let name_budget = row_width.saturating_sub(fixed_prefix + tail_len).max(4);
    let short_name = truncate_text(&name, name_budget);
    let line_len = fixed_prefix + short_name.chars().count() + tail_len;
    let spacer_len = row_width.saturating_sub(line_len);
    let spacer = " ".repeat(spacer_len);

    let mut spans = vec![
        action_marker,
        Span::raw(" "),
        Span::raw(format!("{} ", sel)),
        Span::styled(ts, Style::default().fg(Color::Gray)),
        Span::raw(" "),
        Span::styled(short_name, name_style),
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
    score.clamp(0.0, 1.0)
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
