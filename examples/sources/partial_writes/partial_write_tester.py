#!/usr/bin/env python3
"""Write JSONL records in partial chunks to exercise tailing behavior."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

def write_chunked_line(handle, payload: dict, split_at: int, delay: float) -> None:
    encoded = json.dumps(payload, separators=(",", ":"))
    split_at = max(1, min(split_at, len(encoded) - 1))
    handle.write(encoded[:split_at])
    handle.flush()
    time.sleep(delay)
    handle.write(encoded[split_at:] + "\n")
    handle.flush()


def now_ms() -> int:
    return int(time.time() * 1000)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", help="Path to the JSONL file to write")
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Seconds to wait between partial write chunks",
    )
    args = parser.parse_args()

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with out_path.open("w", encoding="utf-8") as handle:
        step = 1
        while True:
            handle.write(
                json.dumps(
                    {"_timestamp": now_ms(), "event": "complete", "step": step},
                    separators=(",", ":"),
                )
                + "\n"
            )
            handle.flush()
            step += 1
            time.sleep(args.delay)

            write_chunked_line(
                handle,
                {"_timestamp": now_ms(), "event": "partial", "step": step},
                split_at=22,
                delay=args.delay,
            )
            step += 1

            handle.write("   \t\r")
            handle.flush()
            time.sleep(args.delay)

            handle.write("\n")
            handle.flush()
            time.sleep(args.delay)


if __name__ == "__main__":
    main()
