# JSON Stream Analysis: Techniques & Status

## Techniques Applied
*   **Complex Event Processing (CEP) - Windowed Correlation:** Marked action periods are correlated with output events in time windows.  
    Example uses: fraud pipelines, SIEM alert correlation, telecom event processing.
*   **Unsupervised Schema Inference / Event Typing:** Objects are grouped by structure and low-cardinality semantic discriminators (`event`, `type`, `status`, `variant`) to discover event families without manual schemas.  
    Example uses: log analytics, schema discovery in data lakes, protocol reverse engineering.
*   **Streaming Anomaly Detection via Background Modeling:** A continuous baseline models normal output traffic; action-period observations are contrasted against baseline lift.  
    Example uses: observability platforms, intrusion detection, telemetry monitoring.
*   **Heuristic Causal Attribution (Observational):** `Confidence = Consistency * Specificity`.
    *   *Consistency:* Does it happen every time? (e.g., 9/10 trials).
    *   *Specificity:* Is it rare otherwise? (High lift over baseline).
    Example uses: product analytics attribution, incident triage, experiment diagnostics.
*   **Concept Drift Handling (Online):** Baseline pauses during action periods (anti-contamination) and resumes outside them (adaptation).  
    Example uses: live risk scoring, adaptive monitoring systems.
*   **Visual Anomaly Cueing:** Sliding-scale color coding (Neon Green → Grey) gives instant rarity/novelty feedback.

## How They Work Here
*   **Objective:** We are looking for observable indicators of user inputs by analyzing outputs, so we can make stronger inferences from black-box behavior.
*   **Live Stream Analysis:** Correlations update live, so repeated trials quickly strengthen or weaken candidate indicators.
*   **Marked Action Periods:** `m` toggles action periods on/off; in-period objects become candidates while out-of-period objects feed baseline.
*   **Inspection Workflow:** Inspect first at candidate type level, then drill into raw objects for the selected candidate.
*   **Discriminator Promotion (Current):** A field path is promoted only if it looks stable categorical (`min_support`, `max_cardinality`, `max_unique_ratio`, recurring values). This can increase specificity (less signal dilution) but may fragment types if too aggressive.

## Missed / Next Steps
*   **Protobuf Support:** Discussed but not implemented; current system assumes JSON.
*   **Delayed Emission Modeling:** Objects emitted after action windows can be missed. Add configurable post-action windows plus matched control windows/significance checks.
*   **Threshold Sensitivity:** Similarity and confidence thresholds materially change merges/rankings; expose and tune these explicitly.
*   **Discriminator Thresholds Are Heuristic Defaults:** Current values (`min_support=8`, `max_cardinality=24`, `max_unique_ratio=0.80`, `value_min_count=2`) are practical guardrails, not universal constants.
*   **Auto-Tuning Discriminator Thresholds:** Add offline/online calibration that optimizes indicator quality on replayed/marked data (e.g., maximize consistency-lift stability while penalizing type fragmentation), then persist profile-specific defaults.
*   **Drift Tradeoff:** Continuous baseline stays fresh, but slow changes can absorb once-distinct indicators; periodic controlled recalibration is needed.
*   **Automated Action Detection:** Currently manual (`m` key). Could be automated via API hooks or log tailing.
*   **Replay / Batch Mode:** Current UX is optimized for live streams; pre-captured log replay needs a dedicated flow.
