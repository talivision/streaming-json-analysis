"""
demo_source.py — Simulated Black-Box Data Source

This simulates a system that produces a continuous stream of JSON objects.
Think of it as the "black box" the analyst is trying to understand.

It produces two kinds of objects:
  - Background noise: heartbeats, status checks, metrics (always flowing)
  - Action-triggered objects: appear after specific triggers

The analyst does NOT know which objects are caused by which actions.
That's what they're trying to discover using the analyzer.

Architecture:
  Writes JSONL to /tmp/json_demo/stream.jsonl (one JSON object per line)
  Listens on UDP port 8766 for trigger commands

Usage:
  Terminal 1:  python demo_source.py
  Terminal 2:  python demo_analyzer.py
  Terminal 3:  python trigger.py login       (simulate an action)
               python trigger.py purchase
               python trigger.py search

The "secret" action→object mappings are defined in ACTION_RESPONSES below.
In a real scenario, this would be whatever opaque system the analyst observes.

On exit (Ctrl+C), the /tmp/json_demo/ directory is cleaned up.
"""

import asyncio
import json
import os
import random
import shutil
import time

# --- Configuration ---
OUTPUT_DIR = "/tmp/json_demo"
STREAM_FILE = os.path.join(OUTPUT_DIR, "stream.jsonl")
TRIGGER_PORT = 8766  # UDP port for trigger commands


# ============================================================================
# BACKGROUND NOISE DEFINITIONS
# ============================================================================
# These objects flow continuously regardless of any actions.
# They represent the constant "chatter" the analyst must filter through.
# Each generator returns a new random instance of that object type.


def make_heartbeat():
    """High-frequency heartbeat — the most common background noise."""
    return {
        "type": "hb",
        "seq": random.randint(1, 999999),
        "ts": time.time(),
    }


def make_status():
    """Periodic status check — moderately common."""
    return {
        "status": random.choice(["ok", "ok", "ok", "ok", "degraded"]),
        "uptime_sec": random.randint(10000, 999999),
        "active_connections": random.randint(10, 200),
    }


def make_metric():
    """System metrics — less common but still regular."""
    return {
        "metric_name": random.choice(["cpu_usage", "mem_usage", "disk_io", "net_rx"]),
        "value": round(random.uniform(0, 100), 2),
        "host": random.choice(["web-1", "web-2", "web-3", "db-1"]),
    }


# (generator_function, average_interval_in_seconds)
# Each runs independently so rates are additive.
BACKGROUND = [
    (make_heartbeat, 0.1),   # ~10/sec — dominant noise
    (make_status,    0.33),  # ~3/sec
    (make_metric,    0.5),   # ~2/sec
]


# ============================================================================
# ACTION-TRIGGERED OBJECT DEFINITIONS
# ============================================================================
# These are the "secret" mappings the analyst must discover.
# Each action triggers one or more objects after a randomized delay.
#
# In the real world, these would be whatever the black-box system emits
# when certain things happen. The analyst doesn't see this code — they
# only see the resulting objects in the stream.


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


# Action name → list of (factory, min_delay_ms, max_delay_ms)
#
# "login" produces 2 objects:  session_created (fast) + auth_log (slower)
# "purchase" produces 3 objects: order + inventory + notification (cascading)
# "search" produces 1 object:  query_log (very fast)
ACTION_RESPONSES = {
    "login": [
        (make_session_created,  80, 200),
        (make_auth_log,        150, 400),
    ],
    "purchase": [
        (make_order_created,    50, 150),
        (make_inventory_update, 100, 300),
        (make_notification,     200, 500),
    ],
    "search": [
        (make_query_log,        30, 80),
    ],
}


# ============================================================================
# FILE-BASED STREAM WRITER
# ============================================================================


class DataSource:
    """
    The simulated data source. Writes JSON objects to a shared file
    that the analyzer watches.
    """

    def __init__(self, stream_path: str):
        self.stream_path = stream_path
        # Open the file in append mode, line-buffered for near-instant visibility
        self._file = open(stream_path, "a", buffering=1)
        self._count = 0

    def emit(self, obj: dict):
        """Write a JSON object as a single line to the stream file."""
        self._file.write(json.dumps(obj) + "\n")
        self._file.flush()  # Ensure the analyzer sees it immediately
        self._count += 1

    def close(self):
        self._file.close()

    async def run_background(self):
        """
        Start all background noise generators.
        Each type runs as an independent async task at its own rate.
        """
        tasks = [
            self._generate_noise(factory, interval)
            for factory, interval in BACKGROUND
        ]
        await asyncio.gather(*tasks)

    async def _generate_noise(self, factory, avg_interval: float):
        """Generate one type of background noise at an average rate."""
        while True:
            # Add ±30% jitter to make the stream look realistic
            jittered = avg_interval * random.uniform(0.7, 1.3)
            await asyncio.sleep(jittered)
            self.emit(factory())

    def trigger(self, action_name: str):
        """
        Handle a trigger command. Schedules the action's response objects
        to be emitted after their defined delays.

        This simulates: the analyst did something to the real system,
        and the system is now producing response objects.
        """
        if action_name not in ACTION_RESPONSES:
            print(f"  Unknown trigger: '{action_name}' "
                  f"(available: {', '.join(ACTION_RESPONSES)})")
            return

        print(f"  Trigger: {action_name}")

        # Schedule each response object with its randomized delay
        for factory, min_ms, max_ms in ACTION_RESPONSES[action_name]:
            delay = random.randint(min_ms, max_ms) / 1000.0
            asyncio.ensure_future(self._delayed_emit(factory, delay))

    async def _delayed_emit(self, factory, delay: float):
        """Wait for the delay, then emit the object."""
        await asyncio.sleep(delay)
        self.emit(factory())


# ============================================================================
# UDP TRIGGER LISTENER
# ============================================================================


class TriggerProtocol(asyncio.DatagramProtocol):
    """
    Listens for UDP packets containing trigger commands.
    Each packet should contain a single action name (e.g., "login").
    """

    def __init__(self, source: DataSource):
        self.source = source

    def datagram_received(self, data: bytes, addr):
        action = data.decode().strip()
        if action:
            self.source.trigger(action)


# ============================================================================
# MAIN
# ============================================================================


def setup_output_dir():
    """Create (or clear) the output directory."""
    if os.path.exists(OUTPUT_DIR):
        shutil.rmtree(OUTPUT_DIR)
    os.makedirs(OUTPUT_DIR)
    print(f"Output directory: {OUTPUT_DIR}")
    print(f"Stream file: {STREAM_FILE}")


def cleanup_output_dir():
    """Remove the output directory on exit."""
    if os.path.exists(OUTPUT_DIR):
        shutil.rmtree(OUTPUT_DIR)
        print(f"Cleaned up {OUTPUT_DIR}")


async def main():
    setup_output_dir()
    source = DataSource(STREAM_FILE)

    # Start UDP listener for trigger commands
    loop = asyncio.get_event_loop()
    transport, _ = await loop.create_datagram_endpoint(
        lambda: TriggerProtocol(source),
        local_addr=("127.0.0.1", TRIGGER_PORT),
    )
    print(f"Trigger listener on udp://127.0.0.1:{TRIGGER_PORT}")
    print()
    print("Send triggers with:")
    for action in ACTION_RESPONSES:
        print(f"  python trigger.py {action}")
    print()
    print("Start the analyzer in another terminal:")
    print("  python demo_analyzer.py")
    print()
    print("Streaming objects... (Ctrl+C to stop)")

    # Run background noise generation forever
    try:
        await source.run_background()
    finally:
        source.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutting down.")
    finally:
        cleanup_output_dir()
