#!/usr/bin/env python3
"""Continuously write events into a directory (one file per event by default)."""

import argparse
import json
import random
import signal
import time
from pathlib import Path

STOP = False


def on_sigint(_sig, _frame):
    global STOP
    STOP = True


def make_event(seq: int) -> dict:
    ev_type = random.choice(["hb", "status", "metric", "auth_log", "search"]) 
    obj = {
        "_timestamp": int(time.time() * 1000),
        "type": ev_type,
        "seq": seq,
    }
    if ev_type == "status":
        obj["uptime_sec"] = random.randint(10_000, 900_000)
        obj["active_connections"] = random.randint(10, 300)
    elif ev_type == "metric":
        obj["metric_name"] = random.choice(["cpu", "mem", "net", "disk"])
        obj["value"] = round(random.random() * 100.0, 2)
    elif ev_type == "auth_log":
        obj["event"] = "auth_log"
        obj["user"] = f"u{random.randint(1, 9999)}"
        obj["ok"] = random.random() > 0.03
    elif ev_type == "search":
        obj["query"] = random.choice(["laptop", "mouse", "coffee", "shoes"])
        obj["results"] = random.randint(0, 400)
    return obj


def main() -> None:
    parser = argparse.ArgumentParser(description="Stream events into a directory")
    parser.add_argument("--output-dir", default="/tmp/json_demo/stream-dir", help="directory path")
    parser.add_argument("--rate", type=float, default=200.0, help="events/second")
    parser.add_argument("--events", type=int, default=0, help="0 means infinite")
    parser.add_argument("--events-per-file", type=int, default=1, help="events per output file")
    parser.add_argument("--clean", action="store_true", help="delete existing files in output dir first")
    args = parser.parse_args()

    if args.rate <= 0:
        raise SystemExit("--rate must be > 0")
    if args.events < 0:
        raise SystemExit("--events must be >= 0")
    if args.events_per_file <= 0:
        raise SystemExit("--events-per-file must be > 0")

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    if args.clean:
        for p in out_dir.iterdir():
            if p.is_file():
                p.unlink()

    signal.signal(signal.SIGINT, on_sigint)
    signal.signal(signal.SIGTERM, on_sigint)

    interval = 1.0 / args.rate
    seq = 0
    file_seq = 0
    next_emit = time.perf_counter()

    print(f"writing to {out_dir} at {args.rate} ev/s (Ctrl+C to stop)")

    while not STOP:
        if args.events and seq >= args.events:
            break

        batch = []
        for _ in range(args.events_per_file):
            if args.events and seq >= args.events:
                break
            batch.append(make_event(seq))
            seq += 1

        if not batch:
            break

        path = out_dir / f"evt_{file_seq:09d}.json"
        if len(batch) == 1:
            payload = json.dumps(batch[0], separators=(",", ":"))
        else:
            payload = json.dumps(batch, separators=(",", ":"))
        path.write_text(payload + "\n", encoding="utf-8")
        file_seq += 1

        next_emit += interval
        sleep_for = next_emit - time.perf_counter()
        if sleep_for > 0:
            time.sleep(sleep_for)

    print(f"stopped after {seq} events in {file_seq} files")


if __name__ == "__main__":
    main()
