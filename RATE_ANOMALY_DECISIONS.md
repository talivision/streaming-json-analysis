# Rate Anomaly Decisions

## Problem Summary
- First appearances of fully new types during an action window were not consistently surfacing as maximal anomaly.
- Infrequent-but-normal event types (for example, every ~10 minutes) could be under-scored when they appeared only once in an action window.
- Existing period-rate logic required at least two in-period samples before producing a meaningful score.

## Chosen Approach
- Keep existing period-rate computation when there is enough in-period support.
- Add an inter-arrival fallback for action events when period-rate support is weak.
- The fallback compares:
  - `actual inter-arrival`: time since the previous occurrence of the same type (regardless of period membership).
  - `baseline rate`: existing baseline type rate (`baseline_count / baseline_elapsed_secs`), converted implicitly to a comparable rate.

## Scoring Rules Implemented
- If baseline has never seen a type (`baseline_count == 0`), fallback returns `1.0`.
- If no previous occurrence exists but baseline has seen the type, fallback returns `0.0` (insufficient temporal evidence).
- If previous occurrence exists, fallback uses `1 / inter_arrival_secs` vs baseline rate and reuses normalized symmetric rate-difference scoring.
- This fallback is used when:
  - action-period elapsed time is still too short, or
  - action count for the type is `< 2`, or
  - in-period elapsed for that type is non-positive.

## Why This Tradeoff
- Preserves current behaviour for well-sampled action bursts.
- Fixes sparse/infrequent event treatment without waiting for a second in-period event.
- Ensures unseen-in-baseline types in action windows produce maximal anomaly.
- Avoids introducing heavy stateful distribution modeling in this pass.

## Known Limits
- If baseline has seen a type but there is no prior occurrence in loaded history, fallback is conservative (`0.0`).
- Fallback still depends on baseline-rate quality; noisy baselines can reduce precision.
- A richer model (per-type inter-arrival distribution with quantiles) could improve this further.
