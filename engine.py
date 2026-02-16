"""
engine.py — Core analysis engine for JSON object stream correlation.

This module is the heart of the system. It provides four capabilities:

1. Structural Fingerprinting
   Automatically classify JSON objects by their "shape" (keys, nesting,
   value types), with optional semantic discriminators for enum-like fields.
   Objects with the same structure and semantic hints get the same fingerprint.

2. Type Registry
   Discover and track distinct object types as they appear in the stream.
   No predefined schemas needed — types emerge from the data.

3. Baseline Model
   Learn the "normal" background distribution of object types during
   quiet periods. This lets us distinguish signal from noise later.

4. Correlation Engine
   Match analyst-marked actions against observed objects. Compute
   confidence scores indicating how likely an object type is to be
   caused by a specific action.
"""

import hashlib
import json
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Optional


# ============================================================================
# 1. STRUCTURAL FINGERPRINTING
# ============================================================================
#
# The key insight: we don't need to know what JSON objects *mean* to group
# them by type. We just need to know their *shape* — what keys exist, how
# they're nested, and what types the values are.
#
# Example:
#   {"user": "alice", "age": 30}  → shape: {"age": "number", "user": "string"}
#   {"user": "bob",   "age": 25}  → shape: {"age": "number", "user": "string"}
#   Same shape (and same semantic hints, if present) → same fingerprint.


def structural_fingerprint(obj: dict) -> str:
    """
    Compute a short hash representing the structural shape of a JSON object.

    Returns a 12-character hex string. The hash is primarily structural
    (keys, nesting, value types), with a compact semantic add-on for known
    discriminator fields (e.g., event/type/status/variant).
    """
    shape = extract_shape(obj)
    semantic = sorted(extract_semantic_signature(obj))
    # sort_keys gives deterministic serialization.
    payload = {"shape": shape, "semantic": semantic}
    payload_str = json.dumps(payload, sort_keys=True)
    return hashlib.sha256(payload_str.encode()).hexdigest()[:12]


def extract_shape(value: Any) -> Any:
    """
    Recursively extract the type skeleton of a JSON value.

    Dicts become {key: shape(value)} for each key.
    Lists become [shape1, shape2, ...] from up to the first few unique
    element shapes. This handles mixed arrays better than first-element-only.
    Scalars become their type name as a string.

    This is the raw shape used for both hashing and similarity comparison.
    """
    if value is None:
        return "null"
    elif isinstance(value, bool):
        # Must check bool before int — bool is a subclass of int in Python
        return "boolean"
    elif isinstance(value, (int, float)):
        return "number"
    elif isinstance(value, str):
        return "string"
    elif isinstance(value, list):
        if not value:
            return ["empty"]
        # Capture up to a few unique element shapes for heterogeneous arrays.
        # We sort by serialized form for deterministic output.
        unique: dict[str, Any] = {}
        for item in value[:5]:
            item_shape = extract_shape(item)
            key = json.dumps(item_shape, sort_keys=True)
            unique[key] = item_shape
        shapes = [unique[k] for k in sorted(unique.keys())]
        return shapes
    elif isinstance(value, dict):
        return {k: extract_shape(v) for k, v in sorted(value.items())}
    return "unknown"


def shape_similarity(shape_a: Any, shape_b: Any) -> float:
    """
    Compute structural similarity between two shapes (0.0 to 1.0).

    For dicts: blend key overlap with recursive child similarity.
    For lists: best-match overlap between observed element-shapes.
    For scalars: exact type/value-token match.

    Used for fuzzy clustering — merging types that are structurally close
    but not identical (e.g., one has an optional field the other lacks).
    """
    if isinstance(shape_a, dict) and isinstance(shape_b, dict):
        keys_a = set(shape_a.keys())
        keys_b = set(shape_b.keys())
        if not keys_a and not keys_b:
            return 1.0
        union = keys_a | keys_b
        intersection = keys_a & keys_b
        key_jaccard = len(intersection) / len(union)

        # Recursive value-shape agreement on shared keys.
        if intersection:
            child_scores = [
                shape_similarity(shape_a[k], shape_b[k]) for k in intersection
            ]
            child_similarity = sum(child_scores) / len(child_scores)
        else:
            child_similarity = 0.0

        # Weighted blend: keys matter a bit more than child detail.
        return 0.6 * key_jaccard + 0.4 * child_similarity

    if isinstance(shape_a, list) and isinstance(shape_b, list):
        if not shape_a and not shape_b:
            return 1.0
        if not shape_a or not shape_b:
            return 0.0

        # Symmetric best-match average to handle list shape sets.
        def avg_best(src: list[Any], dst: list[Any]) -> float:
            return sum(max(shape_similarity(s, d) for d in dst) for s in src) / len(src)

        return 0.5 * (avg_best(shape_a, shape_b) + avg_best(shape_b, shape_a))

    return 1.0 if shape_a == shape_b else 0.0


SEMANTIC_VALUE_KEYS = {
    "action",
    "auth_method",
    "channel",
    "currency",
    "event",
    "level",
    "method",
    "metric_name",
    "name",
    "priority",
    "status",
    "template",
    "type",
    "variant",
}


def extract_semantic_signature(obj: Any, path: str = "") -> set[str]:
    """
    Extract low-cardinality, enum-like value hints for merge decisions.

    We intentionally do not include high-entropy IDs/timestamps; the goal is
    to distinguish logical event families that share structure but differ by
    stable discriminator fields like `event`/`type`/`status`.
    """
    out: set[str] = set()
    if isinstance(obj, dict):
        for key, value in obj.items():
            child_path = f"{path}.{key}" if path else key
            out |= extract_semantic_signature(value, child_path)
            if key not in SEMANTIC_VALUE_KEYS:
                continue
            if isinstance(value, str):
                # Keep short-ish categorical strings; skip likely IDs.
                if len(value) <= 40 and sum(ch.isdigit() for ch in value) <= 6:
                    out.add(f"{child_path}={value}")
            elif isinstance(value, bool):
                out.add(f"{child_path}={value}")
            elif isinstance(value, (int, float)):
                # Numeric semantic flags are useful when they are bounded.
                if abs(float(value)) < 10000:
                    out.add(f"{child_path}={value}")
    elif isinstance(obj, list):
        for item in obj[:3]:
            out |= extract_semantic_signature(item, f"{path}[]")
    return out


def semantic_similarity(a: set[str], b: set[str]) -> Optional[float]:
    """
    Similarity for semantic signatures. Returns None when both are empty.
    """
    if not a and not b:
        return None
    union = a | b
    if not union:
        return None
    return len(a & b) / len(union)


# ============================================================================
# 2. TYPE REGISTRY
# ============================================================================
#
# As objects stream in, the registry automatically discovers and tracks
# distinct structural types. Each unique fingerprint becomes a "type."
#
# Fuzzy clustering is built in: if a new fingerprint is structurally
# similar to an existing type (above a threshold), they get merged.
# This handles optional fields, minor schema variations, etc.
#
# The analyst can optionally label types with human-readable names
# once they understand what they represent.


@dataclass
class ObjectType:
    """A discovered object type."""
    type_id: str                    # The structural fingerprint hash
    shape: Any                      # Full shape structure (for similarity checks)
    label: Optional[str] = None     # Analyst-assigned label (e.g., "heartbeat")
    count: int = 0                  # Total instances observed
    first_seen: float = 0.0         # Timestamp of first instance
    example: Optional[dict] = None  # First instance seen (for inspection)
    ignored: bool = False           # Analyst has marked this as uninteresting noise
    semantic_signature: set[str] = field(default_factory=set)

    @property
    def display_name(self) -> str:
        """Human-readable name: label if set, otherwise truncated fingerprint."""
        return self.label or f"type-{self.type_id[:8]}"


class TypeRegistry:
    """
    Discovers and tracks object types automatically.

    Every object that arrives gets fingerprinted. If we've seen that
    fingerprint before, increment the count. If not, register a new type
    (or merge with a similar existing type if above the similarity threshold).

    No manual type definitions needed — types emerge from the data.
    """

    def __init__(
        self,
        similarity_threshold: float = 0.85,
        semantic_overlap_threshold: float = 0.50,
    ):
        self.types: dict[str, ObjectType] = {}
        self.similarity_threshold = similarity_threshold
        self.semantic_overlap_threshold = semantic_overlap_threshold
        # Maps variant fingerprints → canonical type_id (for merged types)
        self._canonical: dict[str, str] = {}

    def register(self, obj: dict, timestamp: float) -> tuple[str, bool]:
        """
        Register an observed object.

        Returns:
            (type_id, is_new) — is_new is True if this is a brand-new type
            never seen before. Useful for highlighting novel objects.
        """
        fp = structural_fingerprint(obj)
        canonical_id = self._canonical.get(fp, fp)

        is_new = canonical_id not in self.types

        if is_new:
            # Before creating a new type, check if it's similar enough
            # to an existing type to merge (fuzzy clustering).
            obj_shape = extract_shape(obj)
            obj_signature = extract_semantic_signature(obj)
            for existing_id, existing_type in self.types.items():
                structural = shape_similarity(obj_shape, existing_type.shape)
                semantic = semantic_similarity(obj_signature, existing_type.semantic_signature)
                if semantic is None:
                    sim = structural
                else:
                    if semantic < self.semantic_overlap_threshold:
                        continue
                    # Structural match is primary; semantic cues prevent
                    # accidental merges when discriminator values differ.
                    sim = 0.80 * structural + 0.20 * semantic
                if sim >= self.similarity_threshold:
                    # Close enough — treat as a variant of the existing type
                    self._canonical[fp] = existing_id
                    canonical_id = existing_id
                    is_new = False
                    break

            if is_new:
                # Genuinely new type — register it
                self.types[canonical_id] = ObjectType(
                    type_id=canonical_id,
                    shape=obj_shape,
                    count=0,
                    first_seen=timestamp,
                    example=obj,
                    semantic_signature=obj_signature,
                )

        self.types[canonical_id].count += 1
        # Keep semantic signature up to date for evolving variants.
        self.types[canonical_id].semantic_signature |= extract_semantic_signature(obj)
        return canonical_id, is_new

    def get(self, type_id: str) -> Optional[ObjectType]:
        """Look up a type by its ID."""
        return self.types.get(type_id)

    def label_type(self, type_id: str, name: str):
        """Assign a human-readable label to a type."""
        if type_id in self.types:
            self.types[type_id].label = name

    def ignore_type(self, type_id: str):
        """Mark a type as noise — it will be hidden from the stream view."""
        if type_id in self.types:
            self.types[type_id].ignored = True

    def all_types(self, include_ignored: bool = False) -> list[ObjectType]:
        """Get all types, sorted by count (most frequent first)."""
        types = list(self.types.values())
        if not include_ignored:
            types = [t for t in types if not t.ignored]
        types.sort(key=lambda t: t.count, reverse=True)
        return types


# ============================================================================
# 3. BASELINE MODEL
# ============================================================================
#
# The baseline builds CONTINUOUSLY from all objects observed outside of
# action periods. No manual start/stop — it just works:
#
#   baseline ████████  action ░░░░  baseline ████████  action ░░░░  baseline ████
#
# When the analyst toggles an action period ON, the baseline pauses.
# When they toggle it OFF, the baseline resumes. This means:
#   - The baseline never gets contaminated by action-caused objects
#   - The baseline stays fresh and adapts to system drift over time
#   - No manual "lock" step — it's always ready


class BaselineModel:
    """
    Continuously tracks background rates of object types.

    Records all objects EXCEPT those during action periods. The baseline
    grows throughout the session, always reflecting current "normal."

    Usage:
        baseline = BaselineModel()
        baseline.record("type-a")         # record (if not in action period)
        baseline.pause()                  # action period started
        # ... objects during action are NOT recorded ...
        baseline.resume()                 # action period ended
        baseline.rate("type-a")           # current background rate
    """

    def __init__(self):
        self._start_time: float = time.time()
        self._type_counts: dict[str, int] = defaultdict(int)
        self.total_objects: int = 0
        # Track how much time has been spent in action periods
        # so we can subtract it from total duration
        self._paused: bool = False
        self._pause_start: Optional[float] = None
        self._total_paused: float = 0.0

    @property
    def is_paused(self) -> bool:
        """Is the baseline currently paused (during an action period)?"""
        return self._paused

    @property
    def duration(self) -> float:
        """Total observation time, excluding action periods."""
        total = time.time() - self._start_time
        paused = self._total_paused
        if self._paused and self._pause_start:
            paused += time.time() - self._pause_start
        return max(0, total - paused)

    @property
    def is_ready(self) -> bool:
        """Has enough baseline been recorded to be useful?"""
        return self.duration > 1.0 and self.total_objects > 10

    def pause(self):
        """Pause baseline recording (action period starting)."""
        if not self._paused:
            self._paused = True
            self._pause_start = time.time()

    def resume(self):
        """Resume baseline recording (action period ended)."""
        if self._paused:
            self._paused = False
            if self._pause_start:
                self._total_paused += time.time() - self._pause_start
                self._pause_start = None

    def record(self, type_id: str):
        """
        Record an object observation.
        Only counts if NOT currently in an action period.
        """
        if not self._paused:
            self._type_counts[type_id] += 1
            self.total_objects += 1

    def rate(self, type_id: str) -> float:
        """Expected rate (objects per second) for a given type."""
        d = self.duration
        if d <= 0:
            return 0.0
        return self._type_counts.get(type_id, 0) / d

    def total_rate(self) -> float:
        """Total object rate across all types."""
        d = self.duration
        if d <= 0:
            return 0.0
        return self.total_objects / d

    def is_known_type(self, type_id: str) -> bool:
        """Was this type observed during baseline (non-action) periods?"""
        return type_id in self._type_counts


# ============================================================================
# 4. CORRELATION ENGINE
# ============================================================================
#
# The analyst toggles action periods on/off with a single key:
#
#   Press 'm' → action period STARTS (timestamp recorded instantly)
#   Press 'm' → action period ENDS
#
# During an action period:
#   - Objects are candidates for correlation (not baseline)
#   - Latencies are measured from the action period start
#
# Outside an action period:
#   - Objects feed the baseline
#   - No correlation tracking
#
# Over multiple action periods with the same label, the engine builds
# confidence scores. The analyst uses these — plus their own judgment —
# to decide what's causal.
#
# CONFIDENCE SCORE explained:
#
#   confidence = consistency * specificity
#
#   Consistency (appearance_rate):
#     "Does this type reliably show up during this action?"
#     If type T appears in 9 out of 10 action periods → high consistency.
#
#   Specificity (lift over baseline):
#     "Does this type appear MORE during actions than normal?"
#     If type T appears all the time anyway → low specificity (noise).
#     If type T is rare or never seen in baseline → high specificity.
#
#   Both must be high for high confidence.


@dataclass
class ActionPeriod:
    """
    A time span during which the analyst was performing an action.
    Created when 'm' is pressed (start), closed when 'm' is pressed again (end).
    """
    id: int
    label: str
    start: float
    end: Optional[float] = None

    @property
    def is_open(self) -> bool:
        return self.end is None

    @property
    def duration(self) -> float:
        end = self.end or time.time()
        return end - self.start


class CorrelationEngine:
    """
    Matches analyst-defined action periods to object types.

    The analyst toggles action periods with 'm'. The engine watches what
    objects appear during each period. Over multiple periods with the same
    label, it builds confidence scores to advise the analyst — but the
    analyst makes the final call.

    Args:
        baseline: The baseline model (paused/resumed automatically).
    """

    def __init__(self, baseline: BaselineModel):
        self.baseline = baseline
        self._periods: list[ActionPeriod] = []
        self._next_id: int = 1

        # Observations per period:
        # period_id → [(type_id, latency, timestamp, raw_obj), ...]
        self._period_observations: dict[int, list[tuple[str, float, float, Optional[dict]]]] = defaultdict(list)

    @property
    def active_period(self) -> Optional[ActionPeriod]:
        """The currently open action period, if any."""
        if self._periods and self._periods[-1].is_open:
            return self._periods[-1]
        return None

    @property
    def is_in_action(self) -> bool:
        """Is an action period currently active?"""
        return self.active_period is not None

    def toggle(self, label: str) -> tuple[ActionPeriod, bool]:
        """
        Toggle action period on/off. Returns (period, started).

        If no period is active: starts a new one (started=True).
        If a period is active: closes it (started=False).

        Automatically pauses/resumes the baseline model.
        """
        current = self.active_period
        if current is not None:
            # Close the active period
            current.end = time.time()
            self.baseline.resume()
            return current, False
        else:
            # Start a new period
            period = ActionPeriod(
                id=self._next_id,
                label=label,
                start=time.time(),
            )
            self._next_id += 1
            self._periods.append(period)
            self.baseline.pause()
            return period, True

    def observe(self, type_id: str, timestamp: float, raw_obj: Optional[dict] = None):
        """
        Record an observed object. If an action period is active,
        the object is recorded as a candidate with its latency from
        the period start.

        Call this for EVERY object. The engine decides whether it's
        a baseline object or an action candidate.
        """
        current = self.active_period
        if current is not None:
            latency = timestamp - current.start
            if latency >= 0:
                self._period_observations[current.id].append((type_id, latency, timestamp, raw_obj))

    def period_count(self, label: str) -> int:
        """How many completed action periods have this label?"""
        return sum(
            1 for p in self._periods
            if p.label == label and not p.is_open
        )

    def action_labels(self) -> list[str]:
        """All distinct action labels, in order of first appearance."""
        return list(dict.fromkeys(p.label for p in self._periods))

    def relabel(self, period_id: int, new_label: str):
        """Change the label on a period. Affects future correlation results."""
        for period in self._periods:
            if period.id == period_id:
                period.label = new_label
                return

    def correlations(self, action_label: str) -> list[dict]:
        """
        Compute correlation results for a specific action label.

        Only considers CLOSED periods (completed action cycles).
        Aggregates observations and computes confidence scores.

        Returns a list of dicts sorted by confidence (highest first):
        {
            "type_id":          str,
            "appearances":      int,    # total times this type appeared
            "trials":           int,    # number of completed action periods
            "appearance_rate":  float,  # fraction of periods where this type appeared
            "avg_latency_ms":   float,  # average delay from period start
            "latency_std_ms":   float,  # timing consistency
            "baseline_rate":    float,  # normal rate from baseline
            "confidence":       float,  # 0.0 to 1.0 overall score
            "assessment":       str,    # human-readable interpretation
        }
        """
        # Only count closed periods
        closed = [p for p in self._periods if p.label == action_label and not p.is_open]
        n_trials = len(closed)
        if n_trials == 0:
            return []

        # Compute average period duration (used for baseline comparison)
        avg_duration = sum(p.duration for p in closed) / n_trials

        # Aggregate observations across all periods with this label.
        # Track per-period presence for appearance_rate calculation.
        type_latencies: dict[str, list[float]] = defaultdict(list)
        type_period_presence: dict[str, set[int]] = defaultdict(set)

        for period in closed:
            for type_id, latency, _, _ in self._period_observations.get(period.id, []):
                type_latencies[type_id].append(latency)
                type_period_presence[type_id].add(period.id)

        results = []

        for type_id, latencies in type_latencies.items():
            appearances = len(latencies)
            # appearance_rate = in how many periods did this type appear?
            # (not total count — a type appearing 10x in one period counts as 1)
            periods_present = len(type_period_presence[type_id])
            appearance_rate = periods_present / n_trials

            avg_latency = sum(latencies) / len(latencies)
            if len(latencies) > 1:
                variance = sum((l - avg_latency) ** 2 for l in latencies) / (
                    len(latencies) - 1
                )
                latency_std = variance**0.5
            else:
                latency_std = 0.0

            # --- Confidence Calculation ---
            baseline_rate = self.baseline.rate(type_id)
            # How many of this type would we expect in a window of this
            # duration, based on baseline?
            expected_in_window = baseline_rate * avg_duration

            if expected_in_window < 0.01:
                # Type is rare or unseen in baseline — highly specific
                specificity = 1.0
            else:
                # Lift: how much more frequent during actions vs. normal?
                observed_per_trial = appearances / n_trials
                lift = observed_per_trial / expected_in_window
                if lift <= 1.0:
                    specificity = 0.0
                else:
                    # lift=2 → 0.50, lift=5 → 0.80, lift=10 → 0.90
                    specificity = 1.0 - (1.0 / lift)

            confidence = appearance_rate * specificity

            if confidence >= 0.8:
                assessment = "STRONG — likely causal"
            elif confidence >= 0.5:
                assessment = "MODERATE — worth investigating"
            elif confidence >= 0.2:
                assessment = "WEAK — possibly coincidence"
            else:
                assessment = "NOISE — probably unrelated"

            results.append(
                {
                    "type_id": type_id,
                    "appearances": appearances,
                    "trials": n_trials,
                    "appearance_rate": round(appearance_rate, 3),
                    "avg_latency_ms": round(avg_latency * 1000, 1),
                    "latency_std_ms": round(latency_std * 1000, 1),
                    "baseline_rate": round(baseline_rate, 2),
                    "confidence": round(confidence, 3),
                    "assessment": assessment,
                }
            )

        results.sort(key=lambda r: r["confidence"], reverse=True)
        return results

    def raw_observations(
        self,
        action_label: str,
        type_id: str,
        limit: int = 20,
    ) -> tuple[list[dict], int]:
        """
        Get raw observed objects for a candidate type during closed periods.

        Returns:
            (rows, total_count) where rows are sorted by timestamp ascending.
            Each row has:
              {
                "period_id": int,
                "timestamp": float,
                "latency_ms": float,
                "obj": dict,
              }
        """
        closed = [p for p in self._periods if p.label == action_label and not p.is_open]
        closed_ids = {p.id for p in closed}
        matches: list[dict] = []

        for pid in closed_ids:
            for obs_type, latency, ts, raw_obj in self._period_observations.get(pid, []):
                if obs_type != type_id or raw_obj is None:
                    continue
                matches.append(
                    {
                        "period_id": pid,
                        "timestamp": ts,
                        "latency_ms": latency * 1000.0,
                        "obj": raw_obj,
                    }
                )

        matches.sort(key=lambda r: r["timestamp"])
        total = len(matches)
        if limit > 0:
            matches = matches[:limit]
        return matches, total
