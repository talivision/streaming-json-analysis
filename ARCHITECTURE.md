# Architecture

This document describes the system architecture with emphasis on the analysis engine (`engine.py`).  
UI and source components are included only to explain how engine decisions are driven.

## High-Level Summary

The system is a streaming correlation pipeline for JSON objects:

1. A source emits JSON events (live stream or replay input).
2. The analyzer feeds every event into the engine.
3. The engine:
   - groups objects into discovered types (no predefined schema),
   - learns a baseline of normal background traffic,
   - records observations during analyst-marked action periods,
   - computes confidence scores per action-label/type pair.
4. The analyzer renders those results for investigation.

Core idea: **causal candidates are types that are both consistent during an action and specific relative to baseline**.

## Scope and Responsibilities

- `engine.py` (primary):
  - structural + semantic fingerprinting,
  - adaptive type discovery and fuzzy merging,
  - baseline rate modeling,
  - action-period observation capture,
  - correlation scoring and raw evidence retrieval.
- `demo_analyzer.py` (secondary):
  - stream ingestion, keyboard-driven period toggling, rendering.
  - calls engine APIs; does not implement correlation logic itself.
- `demo_source.py` (secondary):
  - emits noisy background + triggered action responses for simulation.

## Engine Architecture

### 1) Type Discovery (`TypeRegistry`)

Input: raw JSON object + timestamp.

Processing steps:

1. Extract structural shape (`extract_shape`):
   - dict -> recursively typed key map,
   - list -> up to first 5 unique element-shapes,
   - scalar -> token (`string`, `number`, `boolean`, `null`).
2. Collect semantic candidates:
   - categorical strings / booleans by JSON path.
3. Update adaptive stats per path (`_semantic_stats`):
   - total observations,
   - value frequency histogram.
4. Promote discriminator paths if stable enough:
   - minimum support,
   - bounded cardinality,
   - recurring values,
   - not near-unique.
5. Build semantic signature set (`path=value` tokens) from promoted paths only.
6. Fingerprint payload `{shape, semantic}` using SHA-256 (12 hex chars).
7. Merge or create:
   - if unseen fingerprint, compare with known types using `shape_similarity`,
   - optional semantic gating and blended similarity (`0.80 structural + 0.20 semantic`),
   - merge if above threshold, else create a new `ObjectType`.

Output: `(type_id, is_new)`.

Important behavior:

- Optional fields can merge into an existing type (fuzzy clustering).
- Same shape but distinct discriminator values can stay split.
- High-cardinality fields are intentionally ignored as discriminators.

### 2) Baseline Model (`BaselineModel`)

Purpose: estimate normal background rates while excluding action contamination.

Mechanics:

- Starts recording immediately at analyzer startup.
- Pauses during active action periods; resumes when period closes.
- Tracks:
  - total baseline objects,
  - per-type counts,
  - effective duration (wall-clock minus paused time).
- Provides:
  - `rate(type_id)` in objects/sec,
  - `total_rate()`,
  - readiness signal (`duration > 1s && total_objects > 10`).

### 3) Action Period + Observation Capture (`CorrelationEngine`)

Key entities:

- `ActionPeriod`: `{id, label, start, end}`.
- `Observation`: `{type_id, latency_from_start, latency_from_phase_start, phase, timestamp, raw_obj}`.

Modes:

- Live mode:
  - `toggle(label)` starts/stops periods and pauses/resumes baseline automatically.
  - `observe(...)` records events as:
    - `during` if period currently active,
    - optionally `post` if within configured post-window after a closed period.
- Replay mode:
  - preloads closed periods (`add_period`),
  - uses timestamp checks (`is_in_period`, `observe_at`) rather than live active state.

### 4) Correlation Scoring

Computed per `action_label` from closed periods only.

For each type:

1. `appearance_rate` = fraction of trials where type appears at least once.
2. `baseline_rate` = expected frequency from baseline.
3. `expected_in_window` = `baseline_rate * avg_period_duration`.
4. `specificity`:
   - `1.0` if expected is near zero (`< 0.01`),
   - else based on lift (`observed_per_trial / expected_in_window`), with values <=1 treated as non-specific.
5. `confidence` = `appearance_rate * specificity`.

Confidence interpretation:

- `>= 0.8`: STRONG
- `>= 0.5`: MODERATE
- `>= 0.2`: WEAK
- else: NOISE

Returned metrics include appearances, trials, mean latency, latency stddev, baseline rate, confidence, assessment.

## End-to-End Data Flow

1. Event enters analyzer.
2. Analyzer calls `TypeRegistry.register(obj, ts)`.
3. Analyzer records baseline + correlation:
   - live: baseline always records; correlation engine decides whether event belongs to period/post-window.
   - replay: baseline recording is gated out when timestamp falls in known period.
4. Engine stores raw observations for inspectability (`raw_observations`).
5. Analyzer queries `correlations(label)` and renders ranked candidates.

## Key Tunables

- Type discovery:
  - `similarity_threshold` (default `0.85`),
  - `semantic_overlap_threshold` (default `0.50`),
  - semantic support/cardinality/uniqueness thresholds.
- Correlation:
  - `post_window_sec` (delayed-event capture).

Higher thresholds reduce false merges but increase type fragmentation.  
Lower thresholds merge more aggressively but risk conflating distinct causes.

## Evidence and Test Coverage

`tests/test_type_registry.py` validates core engine behavior:

- semantic splits for same-shape variants,
- optional-field merge behavior at default thresholds,
- strict-threshold split edge cases,
- high-cardinality discriminator suppression,
- post-window delayed observation capture,
- replay period gating via timestamp checks.

## Practical Limits and Tradeoffs

- Baseline uses global per-type rates (no time-of-day segmentation).
- Confidence is heuristic, not causal proof.
- Structural extraction samples list content (first few unique shapes) for speed.
- Semantic learning is conservative to avoid exploding type count on IDs/noisy fields.

These are intentional tradeoffs for real-time usability and analyst-guided discovery.
