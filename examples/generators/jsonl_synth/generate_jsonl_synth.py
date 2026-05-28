#!/usr/bin/env python3
"""Generate a synthetic JSONL fixture for HTTP-stream perf / fuzz tests.

Unlike `generate_large_jsonl.py` (which produces a fixed `type_NNN`
distribution for type-list UI testing), this one emits a realistic mix
of typed events — heartbeats, logins, clicks, purchases, errors — with
varied scalar values so the analyzer's anomaly / uniqueness paths get
exercised.
"""

import argparse
import json
import random
import sys
import time
from pathlib import Path


_rand = random.Random(0xC0FFEE)


def _make_event(now_ms: int) -> dict:
    factories = [
        ("hb", _make_heartbeat, 6),
        ("click", _make_click, 4),
        ("login", _make_login, 2),
        ("purchase", _make_purchase, 1),
        ("error", _make_error, 1),
    ]
    expanded = [f for _, f, w in factories for _ in range(w)]
    obj = _rand.choice(expanded)()
    obj["_timestamp"] = now_ms
    return obj


def _make_heartbeat() -> dict:
    return {"_type": "hb", "seq": _rand.randint(1, 999_999)}


def _make_login() -> dict:
    return {
        "_type": "login",
        "user": f"user-{_rand.randint(0, 9999):04d}",
        "session": f"sess-{_rand.randint(0, 65535):04x}",
        "mfa": _rand.choice([True, False]),
        "region": _rand.choice(["us-east", "us-west", "eu-west", "ap-south"]),
    }


def _make_purchase() -> dict:
    return {
        "_type": "purchase",
        "order_id": f"ord_{_rand.randint(10000, 99999)}",
        "amount_cents": _rand.randint(99, 99999),
        "currency": _rand.choice(["USD", "EUR", "GBP", "AUD"]),
        "sku": f"sku-{_rand.randint(1000, 9999)}",
    }


def _make_click() -> dict:
    return {
        "_type": "click",
        "user": f"user-{_rand.randint(0, 9999):04d}",
        "element": _rand.choice(["nav.home", "btn.checkout", "btn.signup", "link.docs"]),
        "x": _rand.randint(0, 1920),
        "y": _rand.randint(0, 1080),
    }


def _make_error() -> dict:
    return {
        "_type": "error",
        "code": _rand.choice(["E_NET", "E_AUTH", "E_DB", "E_5XX"]),
        "message": _rand.choice([
            "connection refused",
            "token expired",
            "deadlock detected",
            "internal server error",
        ]),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a synthetic JSONL fixture")
    parser.add_argument(
        "--output",
        default="/tmp/json_demo/synth.jsonl",
        help="output JSONL path",
    )
    parser.add_argument(
        "--bytes",
        type=int,
        default=50_000_000,
        help="approximate output size in bytes (default 50 MB)",
    )
    args = parser.parse_args()
    if args.bytes <= 0:
        raise SystemExit("--bytes must be > 0")

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    start = time.monotonic()
    now_ms = int(time.time() * 1000)
    written = 0
    batch = []
    BATCH = 1 << 20  # flush per ~1 MB
    with out_path.open("w", encoding="utf-8") as f:
        while written < args.bytes:
            line = json.dumps(_make_event(now_ms), separators=(",", ":")) + "\n"
            batch.append(line)
            written += len(line)
            # interleave timestamps so periods marked over a range work cleanly
            now_ms += _rand.randint(1, 100)
            if sum(len(b) for b in batch) >= BATCH:
                f.write("".join(batch))
                batch.clear()
        if batch:
            f.write("".join(batch))
    elapsed = time.monotonic() - start
    print(
        f"wrote {written:,} bytes ({written / 1_000_000:.1f} MB) to {out_path} "
        f"in {elapsed:.1f}s ({written / elapsed / 1_000_000:.1f} MB/s)"
    )


if __name__ == "__main__":
    main()
