# Anomaly Scoring

Both scores are in **[0, 1]** and are computed per-event during a marked action period, relative to a baseline corpus.

---

## Rate Anomaly

Measures whether an event type is arriving faster or slower than it did in the baseline.

**Baseline rate** (events/sec) for a type:
```
baseline_rate = baseline_count / baseline_elapsed_secs
```

**Action period rate**, using the unbiased interval estimator:
```
action_rate = (count - 1) / (last_event_ts - first_event_ts)
```

`count - 1` is the number of complete inter-arrival intervals observed. Using the span between the first and last observed event of this type eliminates the start-of-period edge bias that `count / period_elapsed` would introduce.

**Score** (symmetric — detects both spikes and drops):
```
rate_score = 1 - min(action_rate, baseline_rate) / max(action_rate, baseline_rate)
```

A 2× rate change scores 0.5. A 10× change scores 0.9.

**Guards:**
- Returns 0 if fewer than 2 events of this type have been seen (no complete interval yet).
- Returns 0 if less than 1 second of period wall-clock time has elapsed.
- Returns 1.0 if the type never appeared in the baseline (no reference to compare against).

### Inter-arrival fallback

When the action period is too short or the in-period count for a type is < 2, the rate score falls back to an inter-arrival estimate:

- **Actual inter-arrival**: time elapsed since the previous occurrence of the same type (regardless of period membership).
- **Baseline rate**: `baseline_count / baseline_elapsed_secs`, converted to the same rate units.

Fallback scoring rules:
- If the type has never appeared in the baseline (`baseline_count == 0`): returns `1.0`.
- If there is no prior occurrence in loaded history but the baseline has seen the type: returns `0.0` (insufficient temporal evidence).
- If a prior occurrence exists: computes `1 / inter_arrival_secs` as the action rate and applies the same symmetric rate-difference formula as above.

This fallback is active when:
- The action-period elapsed time is below the minimum threshold (1 s), or
- The in-period count for the type is < 2, or
- The in-period elapsed time for that type is non-positive.

**Known limits of the fallback:**
- If baseline has seen a type but there is no prior occurrence in loaded history, the fallback is conservative (returns `0.0`).
- Precision depends on baseline-rate quality; noisy baselines reduce signal.
- A richer model (per-type inter-arrival distribution with quantiles) could improve sparse-type handling further.

---

## Value Uniqueness

Measures whether the values seen in an event's fields during an action period are rare compared to baseline.

For each scalar field path in the event, we look up how often the current value appeared in the baseline:

```
freq = baseline_value_count / baseline_total_for_path
```

The per-path score:
```
path_score = 1 - (freq / RARE_THRESHOLD) ^ CURVE
```

Where `RARE_THRESHOLD = 0.25` and `CURVE = 0.6`. A value that appeared in ≥25% of baseline events scores 0. A never-seen value scores 1.0.

**Event score** = maximum path score across all considered scalar paths.

Paths are **considered** if they pass an automatic noise filter (low-cardinality fields like status codes or types, not high-entropy fields like IDs or free text). The minimum observation count to score a path is 7 events. Paths can be forced on or off per type in the Types view.

---

## Display Normalisation

Raw scores are passed through `anomaly_norm` before being used in colour gradients:

```
display = sqrt(score)
```

This compresses the colour scale so mid-range anomalies are more visible. The numeric scores shown in the UI are always the raw values.
