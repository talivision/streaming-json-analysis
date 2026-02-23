use json_analyzer::domain::{ActionPeriod, AnalyzerModel, DataFilters};
use serde_json::json;

#[test]
fn action_period_anomaly_scores_and_closed_periods_flow() {
    let mut model = AnalyzerModel::new();

    model.ingest(json!({"event":"login","user":"u1"}), 0.0);
    model.ingest(json!({"event":"login","user":"u2"}), 1.0);
    model.ingest(json!({"event":"login","user":"u3"}), 2.0);

    assert!(model.toggle_period());
    assert!(model.active_period().is_some());

    model.ingest(json!({"event":"login","user":"u4"}), 3.0);
    model.ingest(json!({"event":"login","user":"u5"}), 4.0);
    model.refresh_live_anomaly_scores();

    let action_event = model
        .events
        .iter()
        .find(|e| e.in_action_period)
        .expect("at least one action event");
    assert!(action_event.live_rate_score >= 0.0);
    assert!(action_event.live_rate_score <= 1.0);
    assert!(action_event.live_uniq_score >= 0.0);
    assert!(action_event.live_uniq_score <= 1.0);

    assert!(model.toggle_period());
    assert!(model.active_period().is_none());
    assert_eq!(model.closed_periods().len(), 1);
}

#[test]
fn filtered_events_range_is_inclusive() {
    let mut model = AnalyzerModel::new();
    model.ingest(json!({"k":"a"}), 10.0);
    model.ingest(json!({"k":"b"}), 20.0);
    model.ingest(json!({"k":"c"}), 30.0);

    let filters = DataFilters::default();
    let events = model.filtered_events_in_range(&filters, Some((20.0, 30.0)));
    let values: Vec<String> = events
        .iter()
        .filter_map(|e| {
            e.obj
                .get("k")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        })
        .collect();

    assert_eq!(values, vec!["c".to_string(), "b".to_string()]);
}

#[test]
fn replay_with_preloaded_periods_keeps_rate_anomaly_signal() {
    let mut model = AnalyzerModel::new();
    model.set_periods(vec![ActionPeriod {
        id: 1,
        label: "action".to_string(),
        start: 10.0,
        end: Some(20.0),
    }]);

    model.ingest(json!({"event":"login","user":"u1"}), 0.0);
    model.ingest(json!({"event":"login","user":"u2"}), 5.0);
    model.ingest(json!({"event":"login","user":"u3"}), 10.5);
    model.ingest(json!({"event":"login","user":"u4"}), 11.0);
    model.ingest(json!({"event":"login","user":"u5"}), 11.5);
    model.ingest(json!({"event":"login","user":"u6"}), 12.0);
    model.ingest(json!({"event":"login","user":"u7"}), 25.0);
    model.refresh_live_anomaly_scores();

    let action_rates: Vec<f64> = model
        .events
        .iter()
        .filter(|e| e.in_action_period)
        .map(|e| e.live_rate_score)
        .collect();
    assert!(!action_rates.is_empty());
    assert!(action_rates.iter().any(|score| *score > 0.0));
}
