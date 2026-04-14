use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode};
use json_analyzer::domain::{ActionPeriod, AnalyzerModel, DataFilters};
use serde_json::json;

fn make_model(n: usize) -> AnalyzerModel {
    let mut m = AnalyzerModel::new();
    let types = ["login", "purchase", "pageview", "error", "heartbeat"];
    for i in 0..n {
        let t = types[i % types.len()];
        m.ingest(
            json!({
                "_timestamp": 1_700_000_000_000u64 + i as u64 * 50,
                "type": t,
                "user_id": i % 500,
                "status": if i % 10 == 0 { "error" } else { "ok" },
                "url": format!("/page/{}", i % 100),
                "duration_ms": i % 2000,
            }),
            i as f64 * 0.05,
        );
    }
    m
}

fn make_model_with_periods(n: usize) -> AnalyzerModel {
    let mut m = make_model(n);
    m.set_periods(vec![
        ActionPeriod {
            id: 1,
            label: "a".into(),
            start: 1000.0,
            end: Some(5000.0),
        },
        ActionPeriod {
            id: 2,
            label: "b".into(),
            start: 10000.0,
            end: Some(20000.0),
        },
    ]);
    m
}

fn bench_filter_no_filters(c: &mut Criterion) {
    let mut group = c.benchmark_group("filtered_event_indices/no_filters");
    for n in [100_000usize, 1_000_000] {
        let model = make_model(n);
        let filters = DataFilters::default();
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| model.filtered_event_indices(black_box(&filters), black_box(None)));
        });
    }
    group.finish();
}

fn bench_filter_type(c: &mut Criterion) {
    let mut group = c.benchmark_group("filtered_event_indices/type_filter");
    for n in [100_000usize, 1_000_000] {
        let model = make_model(n);
        let filters = DataFilters {
            type_filter: "login".into(),
            ..Default::default()
        };
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| model.filtered_event_indices(black_box(&filters), black_box(None)));
        });
    }
    group.finish();
}

fn bench_filter_type_negated(c: &mut Criterion) {
    let mut group = c.benchmark_group("filtered_event_indices/type_negated");
    for n in [100_000usize, 1_000_000] {
        let model = make_model(n);
        let filters = DataFilters {
            type_filter: "!login".into(),
            ..Default::default()
        };
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| model.filtered_event_indices(black_box(&filters), black_box(None)));
        });
    }
    group.finish();
}

fn bench_filter_substring(c: &mut Criterion) {
    // 1M substring is ~17s/run — cap at 100k; the ratio is the same.
    let mut group = c.benchmark_group("filtered_event_indices/substring_filter");
    group.sampling_mode(SamplingMode::Flat).sample_size(20);
    for n in [100_000usize] {
        let model = make_model(n);
        let filters = DataFilters {
            substring_filter: "error".into(),
            ..Default::default()
        };
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| model.filtered_event_indices(black_box(&filters), black_box(None)));
        });
    }
    group.finish();
}

fn bench_filter_key(c: &mut Criterion) {
    let mut group = c.benchmark_group("filtered_event_indices/key_filter");
    for n in [100_000usize, 1_000_000] {
        let model = make_model(n);
        let filters = DataFilters {
            key_filter: "status".into(),
            ..Default::default()
        };
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| model.filtered_event_indices(black_box(&filters), black_box(None)));
        });
    }
    group.finish();
}

fn bench_filter_exact(c: &mut Criterion) {
    let mut group = c.benchmark_group("filtered_event_indices/exact_filter");
    for n in [100_000usize, 1_000_000] {
        let model = make_model(n);
        let filters = DataFilters {
            exact_filter: "status=s:error".into(),
            ..Default::default()
        };
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| model.filtered_event_indices(black_box(&filters), black_box(None)));
        });
    }
    group.finish();
}

fn bench_filter_range(c: &mut Criterion) {
    let mut group = c.benchmark_group("filtered_event_indices/period_range");
    for n in [100_000usize, 1_000_000] {
        let model = make_model_with_periods(n);
        let filters = DataFilters::default();
        // Range covers ~8% of events
        let range = Some((1000.0, 5000.0));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| model.filtered_event_indices(black_box(&filters), black_box(range)));
        });
    }
    group.finish();
}

fn bench_set_periods(c: &mut Criterion) {
    let mut group = c.benchmark_group("set_periods");
    for n in [100_000usize, 1_000_000] {
        let model = make_model(n);
        let periods = vec![
            ActionPeriod {
                id: 1,
                label: "a".into(),
                start: 1000.0,
                end: Some(5000.0),
            },
            ActionPeriod {
                id: 2,
                label: "b".into(),
                start: 10000.0,
                end: Some(20000.0),
            },
            ActionPeriod {
                id: 3,
                label: "c".into(),
                start: 30000.0,
                end: Some(40000.0),
            },
        ];
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            let mut m = model.clone();
            b.iter(|| m.set_periods(black_box(periods.clone())));
        });
    }
    group.finish();
}

fn bench_refresh_anomaly_scores(c: &mut Criterion) {
    let mut group = c.benchmark_group("refresh_live_anomaly_scores");
    for n in [100_000usize, 1_000_000] {
        let model = make_model_with_periods(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            let mut m = model.clone();
            b.iter(|| m.refresh_live_anomaly_scores());
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_filter_no_filters,
    bench_filter_type,
    bench_filter_type_negated,
    bench_filter_key,
    bench_filter_exact,
    bench_filter_substring,
    bench_filter_range,
    bench_set_periods,
    bench_refresh_anomaly_scores,
);
criterion_main!(benches);
