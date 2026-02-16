# JSON Stream Analysis: Techniques & Status

## Techniques Applied
*   **Structural Fingerprinting:** We group objects by "shape" (keys, nesting, value types) rather than values. This creates a resilient type system without manual schemas.
*   **Baseline Subtraction (Signal-to-Noise):** "Normal" traffic is learned continuously. When you act, we look for what's *new* or *statistically elevated* above this background.
*   **Continuous Baseline:** The baseline pauses during action windows to avoid contamination, allowing it to adapt to drift without "learning" your triggers.
*   **Confidence Scoring:** `Confidence = Consistency * Specificity`.
    *   *Consistency:* Does it happen every time? (e.g., 9/10 trials).
    *   *Specificity:* Is it rare otherwise? (High lift over baseline).
*   **Visual Anomaly Detection:** Sliding-scale color coding (Neon Green → Grey) provides instant visual feedback on object rarity/novelty, similar to heatmap dashboards.

## How They Work Here
*   **Live Stream Analysis:** Correlations update live (every ~20 objects), giving immediate feedback.
*   **Action Windows:** Time-boxing focuses analysis on relevant periods, reducing false positives.
*   **Aggregated Inspection:** The "Inspect" (`i`) modal aggregates data across *all* trials to find robust patterns, avoiding the noise of single-event analysis.

## Missed / Next Steps
*   **Protobuf Support:** Discussed but not implemented; current system assumes JSON.
*   **Advanced Clustering:** We use strict shape hashing. Fuzzy clustering (e.g., Levenshtein on keys) could handle minor schema evolution better.
*   **Automated Action Detection:** Currently manual (`m` key). Could be automated via API hooks or log tailing.
*   **Post-Action Analysis:** The system is optimized for live streams; analyzing pre-captured logs would require a different "replay" mode.
