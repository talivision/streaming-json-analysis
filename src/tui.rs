use crate::app::{AnomalyViewMode, App, ObjectInspector, RateBoundaryViewMode};
use crate::domain::{EventRecord, FilterField, PathOverride};
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

    draw_tabs(frame, root[0], app.mode);
    match app.mode {
        UiMode::Live => draw_live(frame, root[1], app),
        UiMode::Periods => draw_periods(frame, root[1], app),
        UiMode::Types => draw_types(frame, root[1], app),
        UiMode::Data => draw_data(frame, root[1], app),
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
    let titles = ["1 Live", "2 Periods", "3 Types", "4 Data"];
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

fn draw_live(frame: &mut Frame<'_>, area: Rect, app: &mut App) {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(58), Constraint::Percentage(42)])
        .split(area);

    let list_rows = cols[0].height.saturating_sub(2) as usize;
    app.set_live_window_rows(list_rows);
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
        let pretty = serde_json::to_string_pretty(&sel.obj).unwrap_or_else(|_| "{}".to_string());
        let mut lines = vec![Line::from(Span::styled(
            app.model.type_display_name(&sel.type_id),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ))];
        let (show_uniq, show_rate) = displayed_anomaly_scores(app, sel);
        let value_norm = value_anomaly_norm(show_uniq);
        let rate_norm = rate_anomaly_norm(show_rate);
        let value_color = value_anomaly_color(value_norm);
        let rate_color = rate_anomaly_color(rate_norm);
        if sel.in_action_period {
            if app.anomaly_view == AnomalyViewMode::Both {
                lines.push(Line::from(vec![
                    Span::styled("value anomaly snap/live ", Style::default().fg(Color::Gray)),
                    Span::styled(
                        format!("{:.2}/{:.2}", sel.uniq_score, sel.live_uniq_score),
                        Style::default()
                            .fg(value_color)
                            .add_modifier(Modifier::BOLD),
                    ),
                ]));
                lines.push(Line::from(vec![
                    Span::styled("rate anomaly snap/live ", Style::default().fg(Color::Gray)),
                    Span::styled(
                        match app.rate_view {
                            RateBoundaryViewMode::Point => {
                                format!("{:.2}/{:.2}", sel.rate_score, sel.live_rate_score)
                            }
                            RateBoundaryViewMode::Interval => format!(
                                "{:.2}/[{:.2}..{:.2}]",
                                sel.rate_score, sel.live_rate_low, sel.live_rate_high
                            ),
                        },
                        Style::default().fg(rate_color).add_modifier(Modifier::BOLD),
                    ),
                ]));
            } else {
                lines.push(Line::from(vec![
                    Span::styled("value anomaly ", Style::default().fg(Color::Gray)),
                    Span::styled(
                        format!("{:.2}", show_uniq),
                        Style::default()
                            .fg(value_color)
                            .add_modifier(Modifier::BOLD),
                    ),
                    Span::raw("  "),
                    Span::styled("rate anomaly ", Style::default().fg(Color::Gray)),
                    Span::styled(
                        format!("{:.2}", show_rate),
                        Style::default().fg(rate_color).add_modifier(Modifier::BOLD),
                    ),
                ]));
            }
        }
        lines.push(Line::from(""));
        lines.extend(pretty.lines().map(|l| Line::from(l.to_string())));
        Text::from(lines)
    } else {
        Text::from("No event selected")
    };
    let preview = Paragraph::new(preview_text)
        .wrap(Wrap { trim: false })
        .block(
            Block::default()
                .title("Selected JSON")
                .borders(Borders::ALL),
        );
    frame.render_widget(preview, cols[1]);
}

fn draw_periods(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(35), Constraint::Percentage(65)])
        .split(area);

    let periods = app.model.closed_periods();
    let mut p_items = Vec::new();
    for (idx, p) in periods.iter().enumerate() {
        let sel = if idx == app.periods_index { ">" } else { " " };
        let dur = p.end.unwrap_or(p.start) - p.start;
        p_items.push(ListItem::new(format!(
            "{} #{} {} ({:.2}s)",
            sel, p.id, p.label, dur
        )));
    }
    frame.render_widget(
        List::new(p_items).block(
            Block::default()
                .title("All Action Periods")
                .borders(Borders::ALL),
        ),
        cols[0],
    );

    let mut rows = Vec::new();
    let events_inner_width = cols[1].width.saturating_sub(2) as usize;
    if let Some(period) = periods.get(app.periods_index) {
        let start = period.start;
        let end = period.end.unwrap_or(period.start);
        for (idx, e) in app
            .model
            .filtered_events_in_range(&app.event_filters, Some((start, end)))
            .iter()
            .take(120)
            .enumerate()
        {
            let selected = idx == app.period_event_index;
            rows.push(ListItem::new(render_event_line(
                app,
                e,
                selected,
                events_inner_width,
            )));
        }
    }
    frame.render_widget(
        List::new(rows).block(
            Block::default()
                .title("Events in Selected Period")
                .borders(Borders::ALL),
        ),
        cols[1],
    );
}

fn draw_types(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(45), Constraint::Percentage(55)])
        .split(area);

    let visible = app
        .model
        .types
        .iter()
        .filter_map(|(type_id, tp)| {
            if app.types_filter.is_empty() {
                return Some((type_id.clone(), tp));
            }
            let q = app.types_filter.to_lowercase();
            let default = format!("type-{}", &type_id[..8]).to_lowercase();
            let custom = tp.name.clone().unwrap_or_default().to_lowercase();
            if type_id.to_lowercase().contains(&q) || default.contains(&q) || custom.contains(&q) {
                Some((type_id.clone(), tp))
            } else {
                None
            }
        })
        .collect::<Vec<(String, &_)>>();

    let mut type_items = Vec::new();
    for (idx, (type_id, tp)) in visible.iter().enumerate() {
        let mut style = Style::default();
        if idx == app.type_index {
            style = style.fg(Color::Yellow).add_modifier(Modifier::BOLD);
        }
        if tp.known_unrelated {
            style = style.fg(Color::DarkGray);
        }
        let name = app.model.type_display_name(type_id.as_str());
        type_items.push(ListItem::new(Line::from(vec![Span::styled(
            format!(
                "{}  count={}  r={:.2} u={:.2}",
                name, tp.count, tp.latest_rate, tp.latest_uniq
            ),
            style,
        )])));
    }
    frame.render_widget(
        List::new(type_items).block(
            Block::default()
                .title("Types (filtered)")
                .borders(Borders::ALL),
        ),
        cols[0],
    );

    let mut lines = Vec::new();
    if let Some((type_id, tp)) = visible.get(app.type_index) {
        lines.push(Line::from(Span::styled(
            app.model.type_display_name(type_id.as_str()),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )));
        lines.push(Line::from(format!("id: {}", type_id)));
        lines.push(Line::from(format!(
            "paths considered: {}",
            tp.considered_paths.len()
        )));
        lines.push(Line::from(
            "Legend: [AUTO ON] [AUTO OFF] [FORCED ON] [FORCED OFF]",
        ));
        lines.push(Line::from("space cycles: auto -> forced off/on -> auto"));
        lines.push(Line::from(""));

        for (idx, (path, on)) in tp.considered_paths.iter().enumerate() {
            let sel = if idx == app.path_index { ">" } else { " " };
            let override_mode = tp.path_overrides.get(path.as_str()).copied();
            let (marker, color) = match (override_mode, *on) {
                (Some(PathOverride::ForcedOn), _) => ("[FORCED ON]", Color::LightGreen),
                (Some(PathOverride::ForcedOff), _) => ("[FORCED OFF]", Color::LightRed),
                (None, true) => ("[AUTO ON]", Color::Green),
                (None, false) => ("[AUTO OFF]", Color::DarkGray),
            };
            let mode = if override_mode.is_some() {
                "user override"
            } else {
                "auto"
            };
            lines.push(Line::from(vec![
                Span::raw(format!("{} ", sel)),
                Span::styled(marker, Style::default().fg(color)),
                Span::raw(" "),
                Span::raw(format!("{} ({})", path, mode)),
            ]));
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
            .block(Block::default().title("Type Details").borders(Borders::ALL)),
        cols[1],
    );
}

fn draw_data(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let rows = app.model.filtered_events(&app.event_filters);
    let start = app.data_index.min(rows.len().saturating_sub(1));
    let slice = rows.into_iter().skip(start).take(120);

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
        let selected = idx == app.data_index;
        lines.push(render_event_line(app, e, selected, data_inner_width));
    }

    frame.render_widget(
        Paragraph::new(Text::from(lines))
            .wrap(Wrap { trim: true })
            .block(
                Block::default()
                    .title("Data Explorer")
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
            if medium { "anomaly (a)" } else { "a" },
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ));
        row.push(Span::raw(":"));
        row.push(Span::styled(
            app.anomaly_view.label(),
            Style::default().fg(Color::Cyan),
        ));
        row.push(Span::raw("  "));

        row.push(Span::styled(
            if medium { "rate (g)" } else { "g" },
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ));
        row.push(Span::raw(":"));
        row.push(Span::styled(
            app.rate_view.label(),
            Style::default().fg(Color::Cyan),
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
        Paragraph::new(Text::from(text)).block(
            Block::default()
                .title("Input / Toggles")
                .borders(Borders::ALL),
        ),
        area,
    );
}

fn draw_help(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let width = area.width.saturating_sub(2) as usize;
    let filters_active = app.event_filters.active_count();
    let wide = width >= 110;
    let key = if app.event_filters.key_filter.is_empty() {
        "off".to_string()
    } else {
        truncate_text(&app.event_filters.key_filter, 20)
    };
    let typ = if app.event_filters.type_filter.is_empty() {
        "off".to_string()
    } else {
        truncate_text(
            &app.model
                .display_type_filter_value(&app.event_filters.type_filter),
            20,
        )
    };
    let fuzzy = if app.event_filters.fuzzy_filter.is_empty() {
        "off".to_string()
    } else {
        truncate_text(&app.event_filters.fuzzy_filter, 20)
    };
    let exact = if app.event_filters.exact_filter.is_empty() {
        "off".to_string()
    } else {
        truncate_text(&app.event_filters.exact_filter, 20)
    };

    let mut row = vec![
        Span::styled("set ", Style::default().fg(Color::Gray)),
        Span::styled(
            "k",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(" key  ", Style::default().fg(Color::Gray)),
        Span::styled(
            "t",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(" type  ", Style::default().fg(Color::Gray)),
        Span::styled(
            "/",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(" fuzzy  ", Style::default().fg(Color::Gray)),
        Span::styled(
            "e",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(" exact  ", Style::default().fg(Color::Gray)),
        Span::styled(
            "c",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(" clear", Style::default().fg(Color::Gray)),
        Span::raw("  "),
        Span::styled("state:", Style::default().fg(Color::Gray)),
    ];
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

    if wide {
        row.push(Span::raw("  "));
        row.push(Span::styled("k=", Style::default().fg(Color::Yellow)));
        row.push(Span::styled(
            key,
            Style::default().fg(if app.event_filters.key_filter.is_empty() {
                Color::DarkGray
            } else {
                Color::LightGreen
            }),
        ));
        row.push(Span::raw("  "));
        row.push(Span::styled("t=", Style::default().fg(Color::Yellow)));
        row.push(Span::styled(
            typ,
            Style::default().fg(if app.event_filters.type_filter.is_empty() {
                Color::DarkGray
            } else {
                Color::LightGreen
            }),
        ));
        row.push(Span::raw("  "));
        row.push(Span::styled("/=", Style::default().fg(Color::Yellow)));
        row.push(Span::styled(
            fuzzy,
            Style::default().fg(if app.event_filters.fuzzy_filter.is_empty() {
                Color::DarkGray
            } else {
                Color::LightGreen
            }),
        ));
        row.push(Span::raw("  "));
        row.push(Span::styled("e=", Style::default().fg(Color::Yellow)));
        row.push(Span::styled(
            exact,
            Style::default().fg(if app.event_filters.exact_filter.is_empty() {
                Color::DarkGray
            } else {
                Color::LightGreen
            }),
        ));
    }

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
        Line::from("  q quit | h/? help | 1 Live | 2 Periods | 3 Types | 4 Data | a anomaly view | g rate view"),
        Line::from(""),
        Line::from("Live"),
        Line::from("  m toggle action period"),
        Line::from("  f toggle follow"),
        Line::from("  up/down move cursor (disables follow)"),
        Line::from("  Home/End jump to top/bottom"),
        Line::from("  PageUp/PageDown move viewport and cursor (Ctrl+U / Ctrl+D also)"),
        Line::from("  enter inspect selected event"),
        Line::from("  k/t//e set filters, c clear filters, y toggle filters on/off"),
        Line::from(""),
        Line::from("Periods"),
        Line::from("  up/down choose period"),
        Line::from("  left/right choose event in selected period"),
        Line::from("  enter inspect selected event"),
        Line::from(""),
        Line::from("Types"),
        Line::from("  / filter types by id or name"),
        Line::from("  t apply selected type as event filter and jump to Data"),
        Line::from("  r rename selected type"),
        Line::from("  left/right choose path, space cycles AUTO/FORCED state"),
        Line::from("  u mark type known unrelated"),
        Line::from(""),
        Line::from("Data"),
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

    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(40), Constraint::Percentage(60)])
        .split(rows[1]);

    let mut key_items = Vec::new();
    for (idx, key) in inspector.key_paths.iter().enumerate() {
        let sel = if idx == inspector.key_index { ">" } else { " " };
        let style = if idx == inspector.key_index {
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default()
        };
        key_items.push(ListItem::new(Line::from(vec![Span::styled(
            format!("{} {}", sel, key),
            style,
        )])));
    }
    frame.render_widget(
        List::new(key_items).block(Block::default().title("Keys").borders(Borders::ALL)),
        cols[0],
    );

    let obj =
        serde_json::to_string_pretty(&inspector.event.obj).unwrap_or_else(|_| "{}".to_string());
    frame.render_widget(
        Paragraph::new(obj)
            .wrap(Wrap { trim: false })
            .block(Block::default().title("Object").borders(Borders::ALL)),
        cols[1],
    );
}

fn render_event_line(
    app: &App,
    e: &EventRecord,
    selected: bool,
    row_width: usize,
) -> Line<'static> {
    let obj = serde_json::to_string(&e.obj).unwrap_or_default();
    let name = app.model.type_display_name(&e.type_id);
    let mut style = event_style(app, e);
    if selected {
        style = style.add_modifier(Modifier::UNDERLINED | Modifier::BOLD);
    }
    let action_marker = if e.in_action_period {
        Span::styled("  ", Style::default().bg(Color::Red))
    } else {
        Span::raw("  ")
    };
    let sel = if selected { "->" } else { "  " };
    let name_style = if selected {
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::UNDERLINED | Modifier::BOLD)
    } else {
        Style::default().fg(Color::Cyan)
    };
    let show_metrics = e.in_action_period;
    let (show_uniq, show_rate) = displayed_anomaly_scores(app, e);
    let (rate_text, value_text, tail_len) = if show_metrics {
        let (rate_snap_text, rate_live_text, value_snap_text, value_live_text) = (
            format!("{:>5.2}", e.rate_score),
            format!("{:>5.2}", e.live_rate_score),
            format!("{:>5.2}", e.uniq_score),
            format!("{:>5.2}", e.live_uniq_score),
        );
        match app.anomaly_view {
            AnomalyViewMode::Both => {
                let live_rate_text = match app.rate_view {
                    RateBoundaryViewMode::Point => rate_live_text,
                    RateBoundaryViewMode::Interval => {
                        format!("[{:>4.2}..{:>4.2}]", e.live_rate_low, e.live_rate_high)
                    }
                };
                let rate_text = format!("{}/{}", rate_snap_text, live_rate_text);
                let value_text = format!("{}/{}", value_snap_text, value_live_text);
                let tail_len = 2 + 2 + rate_text.chars().count() + 3 + value_text.chars().count();
                (Some(rate_text), Some(value_text), tail_len)
            }
            _ => {
                let rate_live_text = match app.rate_view {
                    RateBoundaryViewMode::Point => rate_live_text,
                    RateBoundaryViewMode::Interval => {
                        format!("[{:>4.2}..{:>4.2}]", e.live_rate_low, e.live_rate_high)
                    }
                };
                let tail_len =
                    2 + 2 + rate_live_text.chars().count() + 3 + value_live_text.chars().count();
                (Some(rate_live_text), Some(value_live_text), tail_len)
            }
        }
    } else {
        (None, None, 0)
    };
    let fixed_prefix = 2 + 1 + 3 + name.chars().count() + 1;
    let mut obj_budget = row_width.saturating_sub(fixed_prefix + tail_len);
    obj_budget = obj_budget.min(48);
    let short = if obj_budget == 0 {
        String::new()
    } else {
        truncate_text(&obj, obj_budget)
    };
    let line_len = fixed_prefix + short.chars().count() + tail_len;
    let spacer_len = row_width.saturating_sub(line_len);
    let spacer = " ".repeat(spacer_len);

    let rate_color = if show_metrics {
        rate_anomaly_color(rate_anomaly_norm(show_rate))
    } else {
        Color::DarkGray
    };
    let value_color = if show_metrics {
        value_anomaly_color(value_anomaly_norm(show_uniq))
    } else {
        Color::DarkGray
    };
    let mut spans = vec![
        action_marker,
        Span::raw(" "),
        Span::raw(format!("{} ", sel)),
        Span::styled(format!("{} ", name), name_style),
        Span::styled(short, style),
    ];
    if let (Some(rate_text), Some(value_text)) = (rate_text, value_text) {
        spans.extend([
            Span::raw(spacer),
            Span::raw("  "),
            Span::styled("R:", Style::default().fg(Color::Gray)),
            Span::styled(rate_text, Style::default().fg(rate_color)),
            Span::raw("  "),
            Span::styled("V:", Style::default().fg(Color::Gray)),
            Span::styled(value_text, Style::default().fg(value_color)),
        ]);
    }
    Line::from(spans)
}

fn event_style(app: &App, e: &EventRecord) -> Style {
    let max_count = app.model.types.values().map(|t| t.count).max().unwrap_or(1) as f64;
    let count = app
        .model
        .types
        .get(&e.type_id)
        .map(|t| t.count)
        .unwrap_or(1) as f64;

    let commonness = (count / max_count.max(1.0)).sqrt().clamp(0.0, 1.0);
    let rarity = 1.0 - commonness;
    let base = lerp_rgb((112, 112, 112), (0, 220, 70), rarity);
    let (show_uniq, show_rate) = displayed_anomaly_scores(app, e);
    let rate_norm = show_rate.clamp(0.0, 1.0);
    let anomaly = (0.5 * show_uniq + 0.5 * rate_norm).clamp(0.0, 1.0);
    let orange_strength = anomaly * 0.9;
    let mixed = lerp_rgb(base, (255, 140, 0), orange_strength);

    let mut style = Style::default().fg(Color::Rgb(mixed.0, mixed.1, mixed.2));
    if count <= 2.0 {
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

fn value_anomaly_norm(value_score: f64) -> f64 {
    value_score.clamp(0.0, 1.0)
}

fn rate_anomaly_norm(rate_score: f64) -> f64 {
    rate_score.clamp(0.0, 1.0)
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

fn displayed_anomaly_scores(app: &App, e: &EventRecord) -> (f64, f64) {
    match app.anomaly_view {
        AnomalyViewMode::Snapshot => (e.uniq_score, e.rate_score),
        AnomalyViewMode::Both | AnomalyViewMode::Recomputed => {
            let rate = match app.rate_view {
                RateBoundaryViewMode::Point => e.live_rate_score,
                RateBoundaryViewMode::Interval => 0.5 * (e.live_rate_low + e.live_rate_high),
            };
            (e.live_uniq_score, rate)
        }
    }
}
