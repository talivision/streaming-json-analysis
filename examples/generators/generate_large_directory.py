#!/usr/bin/env python3
"""Generate a large directory corpus for directory-mode manual testing."""

import argparse
import json
import time
from pathlib import Path


def make_event(idx: int, type_idx: int, base_ms: int) -> dict:
    payload = {
        "user_id": idx % 10000,
        "host": f"api-{(idx % 7) + 1}",
        "ok": (idx % 9) != 0,
        "latency_ms": (idx * 7) % 250,
    }
    return {
        "_timestamp": base_ms + idx,
        "type": f"type_{type_idx:03d}",
        "seq": idx,
        "payload": payload,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a large event directory")
    parser.add_argument(
        "--output-dir",
        default="/tmp/json_demo/dir-180k",
        help="output directory path",
    )
    parser.add_argument("--events", type=int, default=180_000, help="total events")
    parser.add_argument(
        "--events-per-file",
        type=int,
        default=1,
        help="events written per file",
    )
    parser.add_argument("--types", type=int, default=128, help="unique event types")
    args = parser.parse_args()

    if args.events <= 0:
        raise SystemExit("--events must be > 0")
    if args.events_per_file <= 0:
        raise SystemExit("--events-per-file must be > 0")
    if args.types <= 0:
        raise SystemExit("--types must be > 0")

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    base_ms = int(time.time() * 1000)

    file_count = 0
    for start_idx in range(0, args.events, args.events_per_file):
        end_idx = min(start_idx + args.events_per_file, args.events)
        rows = []
        for idx in range(start_idx, end_idx):
            type_idx = idx % args.types
            rows.append(make_event(idx, type_idx, base_ms))

        path = out_dir / f"event_{file_count:07d}.json"
        if len(rows) == 1:
            payload = json.dumps(rows[0], separators=(",", ":"))
        else:
            payload = json.dumps(rows, separators=(",", ":"))
        path.write_text(payload + "\n", encoding="utf-8")
        file_count += 1

    print(
        f"wrote {args.events} events into {file_count} files at {out_dir} "
        f"(events-per-file={args.events_per_file})"
    )


if __name__ == "__main__":
    main()
