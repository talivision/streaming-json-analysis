#!/usr/bin/env python3
"""Generate JSONL events with long arrays and wrap-heavy strings for preview testing."""

import argparse
import json
import time
from pathlib import Path


def make_wrapped_text(event_idx: int, item_idx: int, wrap_len: int) -> str:
    token = f"event{event_idx:02d}-item{item_idx:03d}"
    repeated = " ".join(f"{token}-segment{n:02d}" for n in range(max(1, wrap_len // 18)))
    return f"{repeated} tail-{token}"


def make_item(event_idx: int, item_idx: int, wrap_len: int) -> dict:
    item = {
        "id": item_idx,
        "shared_key": f"group-{item_idx % 8}",
        "stable_object": {
            "kind": "row",
            "bucket": item_idx % 4,
        },
        "wrapped_note": make_wrapped_text(event_idx, item_idx, wrap_len),
    }
    if item_idx % 5 == 0:
        item["variant_a"] = {"event": event_idx, "item": item_idx}
    if item_idx % 7 == 0:
        item["variant_b"] = [item_idx, item_idx + 1, item_idx + 2]
    if item_idx == 0:
        item["first_only"] = True
    if item_idx % 17 == 0:
        item["sparse_key"] = f"sparse-{event_idx}-{item_idx}"
    if item_idx == 63:
        item["late_unique_key"] = "appears-after-many-repeated-objects"
    return item


def make_event(event_idx: int, array_len: int, base_ms: int, wrap_len: int) -> dict:
    return {
        "_timestamp": base_ms + event_idx,
        "event": f"array-preview-{event_idx}",
        "payload": {
            "items": [make_item(event_idx, item_idx, wrap_len) for item_idx in range(array_len)],
            "scalar_items": [
                f"label-{event_idx % 3}",
                event_idx % 2 == 0,
                None,
                event_idx,
                f"label-{event_idx % 3}",
            ],
            "wrap_probe": {
                "header": make_wrapped_text(event_idx, 999, wrap_len * 2),
                "footer": make_wrapped_text(event_idx, 1000, wrap_len * 3),
            },
            "meta": {
                "array_len": array_len,
                "event_idx": event_idx,
                "wrap_len": wrap_len,
            },
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate long-array preview test data")
    parser.add_argument(
        "--output",
        default="/tmp/json_demo/array-preview-case.jsonl",
        help="output JSONL path",
    )
    parser.add_argument("--events", type=int, default=12, help="number of events")
    parser.add_argument("--array-len", type=int, default=96, help="items per event array")
    parser.add_argument(
        "--wrap-len",
        type=int,
        default=180,
        help="approximate length of long string fields used to force preview wrapping",
    )
    args = parser.parse_args()

    if args.events <= 0:
        raise SystemExit("--events must be > 0")
    if args.array_len <= 0:
        raise SystemExit("--array-len must be > 0")
    if args.wrap_len <= 0:
        raise SystemExit("--wrap-len must be > 0")

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    base_ms = int(time.time() * 1000)

    with out_path.open("w", encoding="utf-8") as handle:
        for event_idx in range(args.events):
            handle.write(
                json.dumps(
                    make_event(event_idx, args.array_len, base_ms, args.wrap_len),
                    separators=(",", ":"),
                )
            )
            handle.write("\n")

    print(
        f"wrote {args.events} events with arrays of {args.array_len} objects and wrap_len={args.wrap_len} to {out_path}"
    )


if __name__ == "__main__":
    main()
