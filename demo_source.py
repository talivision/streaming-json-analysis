"""
demo_source.py — Simulated Black-Box Data Source

This simulates a system that produces a continuous stream of JSON objects.
Think of it as the "black box" the analyst is trying to understand.

It produces two kinds of objects:
  - Background noise: heartbeats, status checks, metrics (always flowing)
  - Action-triggered objects: appear after specific key presses

Usage:
  Terminal 1: python demo_source.py
  Terminal 2: cargo run --release -- /tmp/json_demo/stream.jsonl

In the source terminal, press:
  l = login
  p = purchase
  s = search
  c = experiment_control
  t = experiment_treatment
  h = source_like_heartbeat
  m = source_like_metric
  ? = help
  q = quit

On exit, /tmp/json_demo/ is cleaned up.
"""

import asyncio
import json
import os
import random
import shutil
import sys
import termios
import time
import tty
from dataclasses import dataclass

# --- Configuration ---
OUTPUT_DIR = "/tmp/json_demo"
STREAM_FILE = os.path.join(OUTPUT_DIR, "stream.jsonl")


def make_heartbeat():
    return {
        "type": "hb",
        "seq": random.randint(1, 999999),
        "ts": time.time(),
    }


def make_status():
    return {
        "status": random.choice(["ok", "ok", "ok", "ok", "degraded"]),
        "uptime_sec": random.randint(10000, 999999),
        "active_connections": random.randint(10, 200),
    }


def make_metric():
    return {
        "metric_name": random.choice(["cpu_usage", "mem_usage", "disk_io", "net_rx"]),
        "value": round(random.uniform(0, 100), 2),
        "host": random.choice(["web-1", "web-2", "web-3", "db-1"]),
    }


BACKGROUND = [
    (make_heartbeat, 1),
    (make_status, 3.3),
    (make_metric, 5),
]


def make_session_created():
    return {
        "event": "session_created",
        "session_id": f"sess_{random.randint(10000, 99999)}",
        "user_id": random.randint(1, 50),
        "auth_method": "password",
    }


def make_auth_log():
    return {
        "event": "auth_log",
        "level": "info",
        "message": "user authenticated successfully",
        "source_ip": f"10.0.{random.randint(1, 10)}.{random.randint(1, 254)}",
    }


def make_order_created():
    return {
        "event": "order_created",
        "order_id": f"ord_{random.randint(10000, 99999)}",
        "total_amount": round(random.uniform(9.99, 499.99), 2),
        "currency": "USD",
        "items_count": random.randint(1, 5),
    }


def make_inventory_update():
    return {
        "event": "inventory_update",
        "product_id": f"prod_{random.randint(100, 999)}",
        "quantity_delta": -random.randint(1, 3),
        "warehouse": random.choice(["east", "west"]),
    }


def make_notification():
    return {
        "event": "notification_queued",
        "channel": random.choice(["email", "email", "sms"]),
        "template": "order_confirmation",
        "priority": "normal",
    }


def make_query_log():
    return {
        "event": "query_executed",
        "query_text": random.choice(["shoes", "laptop", "headphones", "coffee maker"]),
        "results_count": random.randint(0, 500),
        "execution_ms": random.randint(5, 200),
    }


def make_experiment_exposure(variant: str):
    return {
        "event": "experiment_exposure",
        "experiment": "checkout_flow_v3",
        "variant": variant,
        "cohort": random.choice(["new_user", "returning_user"]),
    }


def make_triggered_heartbeat():
    return {
        "type": "hb_triggered",
        "seq": random.randint(1, 999999),
        "ts": time.time(),
    }


def make_triggered_metric():
    return {
        "metric_name": "trigger_load",
        "value": round(random.uniform(0, 100), 2),
        "host": random.choice(["web-1", "web-2", "web-3", "db-1"]),
    }


ACTION_RESPONSES = {
    "login": [
        (make_session_created, 80, 200),
        (make_auth_log, 150, 400),
    ],
    "purchase": [
        (make_order_created, 50, 150),
        (make_inventory_update, 100, 300),
        (make_notification, 200, 500),
    ],
    "search": [
        (make_query_log, 30, 80),
    ],
    "experiment_control": [
        (lambda: make_experiment_exposure("control"), 50, 120),
    ],
    "experiment_treatment": [
        (lambda: make_experiment_exposure("treatment"), 50, 120),
    ],
    "source_like_heartbeat": [
        (make_triggered_heartbeat, 20, 40),
        (make_triggered_heartbeat, 30, 60),
        (make_triggered_heartbeat, 50, 80),
    ],
    "source_like_metric": [
        (make_triggered_metric, 40, 90),
        (make_triggered_metric, 60, 120),
    ],
}

KEY_TO_ACTION = {
    "l": "login",
    "p": "purchase",
    "s": "search",
    "c": "experiment_control",
    "t": "experiment_treatment",
    "h": "source_like_heartbeat",
    "m": "source_like_metric",
}


class DataSource:
    def __init__(self, stream_path: str):
        self.stream_path = stream_path
        self._file = open(stream_path, "a", buffering=1)

    def emit(self, obj: dict):
        if "_timestamp" not in obj:
            obj = dict(obj)
            obj["_timestamp"] = int(time.time() * 1000)
        self._file.write(json.dumps(obj) + "\n")
        self._file.flush()

    def close(self):
        self._file.close()

    async def run_background(self, stop_event: asyncio.Event):
        tasks = [
            asyncio.create_task(self._generate_noise(factory, interval, stop_event))
            for factory, interval in BACKGROUND
        ]
        try:
            await stop_event.wait()
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _generate_noise(self, factory, avg_interval: float, stop_event: asyncio.Event):
        while not stop_event.is_set():
            jittered = avg_interval * random.uniform(0.7, 1.3)
            await asyncio.sleep(jittered)
            self.emit(factory())

    def trigger(self, action_name: str):
        if action_name not in ACTION_RESPONSES:
            print(f"Unknown action: {action_name}")
            return

        print(f"  Trigger: {action_name}")
        for factory, min_ms, max_ms in ACTION_RESPONSES[action_name]:
            delay = random.randint(min_ms, max_ms) / 1000.0
            asyncio.create_task(self._delayed_emit(factory, delay))

    async def _delayed_emit(self, factory, delay: float):
        await asyncio.sleep(delay)
        self.emit(factory())


@dataclass
class KeyboardController:
    stop_event: asyncio.Event

    def __post_init__(self):
        self._fd = None
        self._old = None

    def __enter__(self):
        if sys.stdin.isatty():
            self._fd = sys.stdin.fileno()
            self._old = termios.tcgetattr(self._fd)
            tty.setcbreak(self._fd)
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._fd is not None and self._old is not None:
            termios.tcsetattr(self._fd, termios.TCSADRAIN, self._old)

    async def run(self, source: DataSource):
        if not sys.stdin.isatty():
            print("stdin is not a TTY; keyboard triggers disabled")
            await self.stop_event.wait()
            return

        while not self.stop_event.is_set():
            ch = await asyncio.to_thread(sys.stdin.read, 1)
            if not ch:
                continue
            if ch == "\x03":  # Ctrl+C in cbreak mode
                self.stop_event.set()
                break
            key = ch.lower()
            if key == "q":
                self.stop_event.set()
                break
            if key == "?":
                print_key_help()
                continue
            action = KEY_TO_ACTION.get(key)
            if action:
                source.trigger(action)


def setup_output_dir():
    if os.path.exists(OUTPUT_DIR):
        shutil.rmtree(OUTPUT_DIR)
    os.makedirs(OUTPUT_DIR)
    print(f"Output directory: {OUTPUT_DIR}")
    print(f"Stream file: {STREAM_FILE}")


def cleanup_output_dir():
    if os.path.exists(OUTPUT_DIR):
        shutil.rmtree(OUTPUT_DIR)
        print(f"Cleaned up {OUTPUT_DIR}")


def print_key_help():
    print("\nActions (single key):")
    print("  l = login")
    print("  p = purchase")
    print("  s = search")
    print("  c = experiment_control")
    print("  t = experiment_treatment")
    print("  h = source_like_heartbeat")
    print("  m = source_like_metric")
    print("  ? = help")
    print("  q = quit\n")


async def main():
    setup_output_dir()
    source = DataSource(STREAM_FILE)
    stop_event = asyncio.Event()

    print("Start analyzer in another terminal:")
    print("  cargo run --release -- /tmp/json_demo/stream.jsonl")
    print_key_help()
    print("Streaming objects...")

    try:
        with KeyboardController(stop_event) as keyboard:
            bg_task = asyncio.create_task(source.run_background(stop_event))
            kb_task = asyncio.create_task(keyboard.run(source))
            await stop_event.wait()
            for t in (kb_task, bg_task):
                t.cancel()
            await asyncio.gather(kb_task, bg_task, return_exceptions=True)
    finally:
        source.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutting down.")
    finally:
        cleanup_output_dir()
