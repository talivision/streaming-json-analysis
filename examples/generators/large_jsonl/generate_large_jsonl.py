#!/usr/bin/env python3
"""Generate a large JSONL corpus for UI performance testing."""

import argparse
import json
import time
from pathlib import Path


def make_event(idx: int, type_idx: int, base_ms: int) -> dict:
    width = type_idx + 1
    payload = {}
    for key_idx in range(width):
        key = f"f{key_idx:03d}"
        if key_idx % 3 == 0:
            payload[key] = idx + key_idx
        elif key_idx % 3 == 1:
            payload[key] = f"v-{type_idx:03d}-{idx % 1000:03d}"
        else:
            payload[key] = (idx + key_idx) % 2 == 0

    return {
        "_timestamp": base_ms + idx,
        "source": "perf-demo",
        "type_name": f"type_{type_idx:03d}",
        "payload": payload,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate large JSONL test data")
    parser.add_argument(
        "--output",
        default="/tmp/json_demo/large-180k-128types.jsonl",
        help="output JSONL path",
    )
    parser.add_argument("--objects", type=int, default=180_000, help="number of objects")
    parser.add_argument("--types", type=int, default=128, help="number of unique structural types")
    args = parser.parse_args()

    if args.types <= 0:
        raise SystemExit("--types must be > 0")
    if args.objects <= 0:
        raise SystemExit("--objects must be > 0")

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    base_ms = int(time.time() * 1000)

    with out_path.open("w", encoding="utf-8") as f:
        for idx in range(args.objects):
            type_idx = idx % args.types
            obj = make_event(idx, type_idx, base_ms)
            f.write(json.dumps(obj, separators=(",", ":")))
            f.write("\n")

    print(f"wrote {args.objects} objects across {args.types} types to {out_path}")


if __name__ == "__main__":
    main()
