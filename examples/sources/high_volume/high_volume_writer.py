#!/usr/bin/env python3
"""Append a large JSONL backlog, then continue at a low live-tail rate.

Usage:
  python3 examples/sources/high_volume/high_volume_writer.py \
      --output /tmp/json_demo/stream.jsonl --initial-events 100000 --steady-rate 10 --truncate

This writer is transport-agnostic — it just appends JSONL to a local file.
Point the analyzer at the file directly, or expose the file over HTTP with
`examples/sources/http_stream/file_server.py` and point the analyzer at the
URL.
"""

from __future__ import annotations

import argparse
import json
import random
import time
from pathlib import Path


def _event(rng: random.Random, seq: int) -> dict:
    kind = rng.choices(
        ["metric", "trace", "request", "job", "error"],
        weights=[55, 20, 18, 5, 2],
        k=1,
    )[0]
    base = {
        "_timestamp": int(time.time() * 1000),
        "_type": kind,
        "seq": seq,
        "host": f"worker-{rng.randrange(128):03d}",
        "region": rng.choice(["us-east", "us-west", "eu-west", "ap-south", "ap-southeast"]),
    }
    if kind == "metric":
        base.update(
            {
                "name": rng.choice(["cpu", "mem", "queue_depth", "latency_ms"]),
                "value": round(rng.random() * 1000, 3),
                "unit": rng.choice(["pct", "mb", "count", "ms"]),
            }
        )
    elif kind == "trace":
        base.update(
            {
                "trace_id": f"{rng.getrandbits(64):016x}",
                "span_id": f"{rng.getrandbits(32):08x}",
                "duration_us": rng.randrange(20, 250_000),
            }
        )
    elif kind == "request":
        base.update(
            {
                "method": rng.choice(["GET", "POST", "PUT", "DELETE"]),
                "path": rng.choice(["/api/items", "/api/search", "/login", "/checkout"]),
                "status": rng.choices([200, 201, 204, 400, 401, 404, 500], [70, 8, 8, 5, 3, 3, 3])[0],
                "bytes": rng.randrange(128, 2_000_000),
            }
        )
    elif kind == "job":
        base.update(
            {
                "queue": rng.choice(["ingest", "export", "billing", "notifications"]),
                "attempt": rng.randrange(1, 5),
                "runtime_ms": rng.randrange(5, 60_000),
            }
        )
    else:
        base.update(
            {
                "code": rng.choice(["E_TIMEOUT", "E_RATE_LIMIT", "E_DB", "E_PARSE"]),
                "message": rng.choice(["timeout", "rate limited", "deadlock", "invalid payload"]),
            }
        )
    return base


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default="/tmp/json_demo/stream.jsonl")
    parser.add_argument("--initial-events", type=int, default=100_000)
    parser.add_argument("--steady-rate", type=float, default=10.0, help="Tail rate after initial events")
    parser.add_argument("--batch-size", type=int, default=250)
    parser.add_argument("--duration", type=float, default=0.0, help="Seconds to run; 0 means forever")
    parser.add_argument("--seed", type=int, default=0xC0FFEE)
    parser.add_argument("--truncate", action="store_true", help="Replace the output file before writing")
    args = parser.parse_args()

    if args.initial_events < 0:
        parser.error("--initial-events must be non-negative")
    if args.steady_rate <= 0:
        parser.error("--steady-rate must be positive")
    if args.batch_size <= 0:
        parser.error("--batch-size must be positive")

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    mode = "w" if args.truncate else "a"
    rng = random.Random(args.seed)
    seq = 0
    total = 0
    start = time.monotonic()
    next_tick = start
    last_report = start

    print(f"writing to {output}", flush=True)
    print(
        f"initial backlog: {args.initial_events} events, then {args.steady_rate:.0f}/sec",
        flush=True,
    )
    with output.open(mode, buffering=1) as f:
        while total < args.initial_events:
            batch_size = min(args.batch_size, args.initial_events - total)
            batch = []
            for _ in range(batch_size):
                batch.append(json.dumps(_event(rng, seq), separators=(",", ":")))
                seq += 1
            f.write("\n".join(batch) + "\n")
            total += len(batch)
        f.flush()
        if args.initial_events > 0:
            elapsed = max(time.monotonic() - start, 0.001)
            print(
                f"initial backlog complete: {total} events in {elapsed:.2f}s "
                f"({total / elapsed:.0f}/sec)",
                flush=True,
            )

        next_tick = time.monotonic()
        while True:
            now = time.monotonic()
            if args.duration > 0 and now - start >= args.duration:
                break

            target_rate = args.steady_rate
            batch_size = 1 if target_rate <= 25 else args.batch_size
            batch = []
            for _ in range(batch_size):
                batch.append(json.dumps(_event(rng, seq), separators=(",", ":")))
                seq += 1
            f.write("\n".join(batch) + "\n")
            total += len(batch)

            next_tick += batch_size / target_rate
            sleep_for = next_tick - time.monotonic()
            if sleep_for > 0:
                time.sleep(sleep_for)
            else:
                next_tick = time.monotonic()

            now = time.monotonic()
            if now - last_report >= 1.0:
                elapsed = now - start
                print(f"{total} events written ({total / elapsed:.0f}/sec)", flush=True)
                last_report = now

    elapsed = max(time.monotonic() - start, 0.001)
    print(f"done: {total} events in {elapsed:.2f}s ({total / elapsed:.0f}/sec)", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
