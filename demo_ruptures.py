"""
demo_ruptures.py — Quick changepoint demo for JSON stream files.

Reads the same source stream used by the demo (`/tmp/json_demo/stream.jsonl`),
builds simple per-bin type-distribution features, and runs changepoint detection
with `ruptures`.

This is intentionally an advisory/experimental tool for replay-style analysis.
"""

from __future__ import annotations

import argparse
import json
import os
from collections import Counter
from typing import Any

from engine import TypeRegistry


DEFAULT_STREAM_DIR = "/tmp/json_demo"


def resolve_stream_path(path_arg: str) -> str:
    if os.path.isdir(path_arg):
        return os.path.join(path_arg, "stream.jsonl")
    return path_arg


def load_type_sequence(
    stream_path: str, max_rows: int = 0
) -> tuple[list[str], list[dict[str, Any]], TypeRegistry]:
    if not os.path.exists(stream_path):
        raise FileNotFoundError(f"stream file not found: {stream_path}")

    registry = TypeRegistry()
    sequence: list[str] = []
    raw_objects: list[dict[str, Any]] = []
    with open(stream_path, "r") as f:
        for i, line in enumerate(f):
            if max_rows and i >= max_rows:
                break
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(obj, dict):
                continue
            type_id, _ = registry.register(obj, timestamp=float(i))
            sequence.append(type_id)
            raw_objects.append(obj)
    return sequence, raw_objects, registry


def build_feature_matrix(
    type_sequence: list[str],
    *,
    bin_size: int,
    top_k: int,
) -> tuple[Any, list[tuple[int, int]], list[str]]:
    try:
        import numpy as np
    except Exception as exc:  # pragma: no cover - user environment dependent
        raise RuntimeError(
            "numpy is required for demo_ruptures. Install with: pip install numpy ruptures"
        ) from exc
    n = len(type_sequence)
    if n == 0:
        return np.zeros((0, 0)), [], []

    global_counts = Counter(type_sequence)
    top_types = [tid for tid, _ in global_counts.most_common(max(1, top_k))]

    bins: list[tuple[int, int]] = []
    rows: list[list[float]] = []
    seen: set[str] = set()

    for start in range(0, n, bin_size):
        end = min(start + bin_size, n)
        chunk = type_sequence[start:end]
        chunk_counts = Counter(chunk)
        chunk_len = max(1, len(chunk))

        # Features:
        # 1) unique type count ratio in this bin
        # 2) novelty ratio (types never seen before this bin)
        # 3..(3+top_k-1) top type proportions
        # last) "other" proportion
        unique_ratio = len(chunk_counts) / chunk_len
        unseen = [tid for tid in chunk_counts if tid not in seen]
        novelty_ratio = len(unseen) / max(1, len(chunk_counts))

        row = [unique_ratio, novelty_ratio]
        top_mass = 0.0
        for tid in top_types:
            p = chunk_counts.get(tid, 0) / chunk_len
            row.append(p)
            top_mass += p
        row.append(max(0.0, 1.0 - top_mass))

        rows.append(row)
        bins.append((start, end))
        seen.update(chunk_counts.keys())

    signal = np.asarray(rows, dtype=float)
    if signal.shape[0] == 0:
        return signal, bins, top_types

    # Standardize features for better cross-scale detection behavior.
    means = signal.mean(axis=0, keepdims=True)
    stds = signal.std(axis=0, keepdims=True)
    stds[stds == 0.0] = 1.0
    signal = (signal - means) / stds
    return signal, bins, top_types


def run_changepoints(
    signal: Any,
    *,
    model: str,
    pen: float,
    min_size: int,
    jump: int,
) -> list[int]:
    try:
        import ruptures as rpt
    except Exception as exc:  # pragma: no cover - user environment dependent
        raise RuntimeError(
            "ruptures is not installed. Install with: pip install ruptures"
        ) from exc

    if signal.shape[0] < max(2, min_size + 1):
        return []
    algo = rpt.Pelt(model=model, min_size=min_size, jump=jump).fit(signal)
    bkps = algo.predict(pen=pen)
    # ruptures includes terminal point (= len(signal)); omit for reporting.
    return [b for b in bkps if b < len(signal)]


def summarize_bin(
    type_sequence: list[str],
    start: int,
    end: int,
    registry: TypeRegistry,
    top_n: int = 3,
) -> str:
    chunk = type_sequence[start:end]
    counts = Counter(chunk)
    parts: list[str] = []
    for tid, c in counts.most_common(top_n):
        t = registry.get(tid)
        name = t.display_name if t else tid[:8]
        parts.append(f"{name}:{c}")
    return ", ".join(parts) if parts else "-"


def _compact_json(obj: dict[str, Any], limit: int = 120) -> str:
    s = json.dumps(obj, separators=(",", ":"))
    if len(s) > limit:
        return s[: limit - 3] + "..."
    return s


def dump_suspect_objects_for_cp(
    *,
    cp: int,
    bins: list[tuple[int, int]],
    type_sequence: list[str],
    raw_objects: list[dict[str, Any]],
    registry: TypeRegistry,
    suspect_limit: int,
    suspect_type_count: int,
):
    left_idx = max(0, cp - 1)
    right_idx = min(len(bins) - 1, cp)
    left_start, left_end = bins[left_idx]
    right_start, right_end = bins[right_idx]

    left_chunk = type_sequence[left_start:left_end]
    right_chunk = type_sequence[right_start:right_end]
    left_counts = Counter(left_chunk)
    right_counts = Counter(right_chunk)
    left_len = max(1, len(left_chunk))
    right_len = max(1, len(right_chunk))

    deltas: list[tuple[float, str]] = []
    for tid in set(left_counts.keys()) | set(right_counts.keys()):
        p_left = left_counts.get(tid, 0) / left_len
        p_right = right_counts.get(tid, 0) / right_len
        deltas.append((p_right - p_left, tid))
    deltas.sort(reverse=True, key=lambda x: x[0])
    suspect_types = [tid for d, tid in deltas if d > 0][: max(1, suspect_type_count)]
    if not suspect_types:
        print("  suspects: none (no positive type lift)")
        return

    print("  suspects:")
    printed = 0
    for row_idx in range(right_start, right_end):
        tid = type_sequence[row_idx]
        if tid not in suspect_types:
            continue
        t = registry.get(tid)
        name = t.display_name if t else tid[:8]
        print(f"    [{name}] {_compact_json(raw_objects[row_idx])}")
        printed += 1
        if printed >= max(1, suspect_limit):
            break
    if printed == 0:
        print("    (no matching objects in right-side bin)")


def dump_unexpected_objects_for_cp(
    *,
    cp: int,
    bins: list[tuple[int, int]],
    type_sequence: list[str],
    raw_objects: list[dict[str, Any]],
    registry: TypeRegistry,
    global_counts: Counter,
    limit: int,
    max_global_count: int,
):
    left_idx = max(0, cp - 1)
    right_idx = min(len(bins) - 1, cp)
    left_start, left_end = bins[left_idx]
    right_start, right_end = bins[right_idx]

    left_types = set(type_sequence[left_start:left_end])
    printed = 0
    print("  unexpected:")
    for row_idx in range(right_start, right_end):
        tid = type_sequence[row_idx]
        if tid in left_types:
            continue
        if global_counts.get(tid, 0) > max(1, max_global_count):
            continue
        t = registry.get(tid)
        name = t.display_name if t else tid[:8]
        gc = global_counts.get(tid, 0)
        print(f"    [{name}] count={gc} {_compact_json(raw_objects[row_idx])}")
        printed += 1
        if printed >= max(1, limit):
            break
    if printed == 0:
        print("    (none by current rarity rules)")


def main():
    parser = argparse.ArgumentParser(description="Quick ruptures demo on JSON stream")
    parser.add_argument(
        "--path",
        default=DEFAULT_STREAM_DIR,
        help=f"Stream dir or stream.jsonl path (default: {DEFAULT_STREAM_DIR})",
    )
    parser.add_argument(
        "--bin-size",
        type=int,
        default=60,
        help="Objects per analysis bin (default: 60)",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=8,
        help="Top discovered types used as distribution features (default: 8)",
    )
    parser.add_argument(
        "--pen",
        type=float,
        default=8.0,
        help="PELT penalty; larger means fewer changepoints (default: 8.0)",
    )
    parser.add_argument(
        "--min-size",
        type=int,
        default=2,
        help="Minimum segment length in bins (default: 2)",
    )
    parser.add_argument(
        "--jump",
        type=int,
        default=1,
        help="Subsampling step for candidate split points (default: 1)",
    )
    parser.add_argument(
        "--model",
        default="l2",
        choices=["l1", "l2", "rbf"],
        help="ruptures cost model (default: l2)",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=0,
        help="Limit rows from stream (0 = all)",
    )
    parser.add_argument(
        "--dump-suspects",
        action="store_true",
        help="Print suspect raw objects around each change point",
    )
    parser.add_argument(
        "--suspect-limit",
        type=int,
        default=8,
        help="Max suspect raw objects printed per change point (default: 8)",
    )
    parser.add_argument(
        "--suspect-types",
        type=int,
        default=3,
        help="Number of lifted types to treat as suspect near a change point (default: 3)",
    )
    parser.add_argument(
        "--dump-unexpected",
        action="store_true",
        help="Print individually unexpected objects near each change point",
    )
    parser.add_argument(
        "--unexpected-limit",
        type=int,
        default=6,
        help="Max unexpected objects printed per change point (default: 6)",
    )
    parser.add_argument(
        "--unexpected-max-global-count",
        type=int,
        default=5,
        help="Treat types seen <= this many times globally as unexpected (default: 5)",
    )
    args = parser.parse_args()

    stream_path = resolve_stream_path(args.path)
    sequence, raw_objects, registry = load_type_sequence(stream_path, max_rows=args.max_rows)
    if not sequence:
        print(f"No parseable JSON objects found in {stream_path}")
        return

    try:
        signal, bins, top_types = build_feature_matrix(
            sequence,
            bin_size=max(10, args.bin_size),
            top_k=max(1, args.top_k),
        )
    except RuntimeError as exc:
        print(str(exc))
        return
    if signal.shape[0] < 3:
        print(
            f"Not enough bins for changepoints: {signal.shape[0]} bins from {len(sequence)} objects. "
            "Try smaller --bin-size or collect more data."
        )
        return

    try:
        cps = run_changepoints(
            signal,
            model=args.model,
            pen=max(0.1, args.pen),
            min_size=max(1, args.min_size),
            jump=max(1, args.jump),
        )
    except RuntimeError as exc:
        print(str(exc))
        return

    print(f"Stream: {stream_path}")
    print(f"Objects: {len(sequence)}")
    print(f"Discovered types: {len(registry.types)}")
    print(f"Bins: {len(bins)} (bin_size={args.bin_size})")
    print(f"Top feature types: {len(top_types)}")
    global_counts = Counter(sequence)
    if not cps:
        print("Changepoints: none detected with current settings")
        return

    print(f"Changepoints: {len(cps)}")
    for cp in cps:
        left_idx = max(0, cp - 1)
        right_idx = min(len(bins) - 1, cp)
        left_start, left_end = bins[left_idx]
        right_start, right_end = bins[right_idx]
        left_summary = summarize_bin(sequence, left_start, left_end, registry)
        right_summary = summarize_bin(sequence, right_start, right_end, registry)
        print(
            f"- bin {cp}  rows {left_end}->{right_start} "
            f"(around objects {left_end})"
        )
        print(f"  before: {left_summary}")
        print(f"  after : {right_summary}")
        if args.dump_suspects:
            dump_suspect_objects_for_cp(
                cp=cp,
                bins=bins,
                type_sequence=sequence,
                raw_objects=raw_objects,
                registry=registry,
                suspect_limit=args.suspect_limit,
                suspect_type_count=args.suspect_types,
            )
        if args.dump_unexpected:
            dump_unexpected_objects_for_cp(
                cp=cp,
                bins=bins,
                type_sequence=sequence,
                raw_objects=raw_objects,
                registry=registry,
                global_counts=global_counts,
                limit=args.unexpected_limit,
                max_global_count=args.unexpected_max_global_count,
            )


if __name__ == "__main__":
    main()
