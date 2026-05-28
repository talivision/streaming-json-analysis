#!/usr/bin/env python3
"""http_stream.py — Simulated HTTP-backed JSONL stream.

Demonstrates the analyzer reading a remote JSONL stream over HTTP. Runs
both halves of the producer side in one process so the demo is single-
command:

  1. A tiny HTTP file server on http://0.0.0.0:8080/ exposing
     /tmp/json_demo/. Range-request support, cheap file-identity ETags,
     and per-range CRC headers so the analyzer can do incremental tail
     polls.
  2. A continuous writer appending JSONL events to
     /tmp/json_demo/stream.jsonl at ~10 events/sec.

Usage:
  Terminal 1:
      python3 examples/sources/http_stream/http_stream.py
  Terminal 2:
      ./target/release/json_analyzer http://127.0.0.1:8080/stream.jsonl

On exit (Ctrl-C or 'q'), the server stops and /tmp/json_demo/ is removed.
"""

from __future__ import annotations

import http.server
import json
import os
import random
import shutil
import signal
import sys
import threading
import time
import zlib
from pathlib import Path

# --- Configuration ---
OUTPUT_DIR = "/tmp/json_demo"
STREAM_FILE = os.path.join(OUTPUT_DIR, "stream.jsonl")
HOST = "0.0.0.0"
PORT = 8080
RATE_PER_SEC = 10.0


# ---------------------------------------------------------------------------
# Event factories — same flavour as demo_source.py: a mix of background
# heartbeats and richer typed events so the analyzer's anomaly path has
# something interesting to chew on.
# ---------------------------------------------------------------------------

_rand = random.Random(0xC0FFEE)


def _make_heartbeat() -> dict:
    return {
        "_type": "hb",
        "seq": _rand.randint(1, 999_999),
        "ts": time.time(),
    }


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


# Weighted mixture: lots of heartbeats and clicks, occasional login/purchase/error.
_FACTORIES = [
    (_make_heartbeat, 6),
    (_make_click,     4),
    (_make_login,     2),
    (_make_purchase,  1),
    (_make_error,     1),
]


def _make_event() -> dict:
    factories = [f for f, w in _FACTORIES for _ in range(w)]
    factory = _rand.choice(factories)
    obj = factory()
    obj["_timestamp"] = int(time.time() * 1000)
    return obj


# ---------------------------------------------------------------------------
# HTTP server
# ---------------------------------------------------------------------------

def _file_info(path: Path) -> tuple[int, str] | None:
    try:
        st = path.stat()
    except OSError:
        return None
    inode = getattr(st, "st_ino", 0)
    etag = f'"stat:{inode:x}:{st.st_mtime_ns:x}:{st.st_size:x}"'
    return st.st_size, etag


def _crc32_range(path: Path, start: int, length: int) -> int | None:
    try:
        with path.open("rb") as f:
            f.seek(start)
            remaining = length
            crc = 0
            while remaining > 0:
                chunk = f.read(min(remaining, 1 << 16))
                if not chunk:
                    break
                crc = zlib.crc32(chunk, crc)
                remaining -= len(chunk)
            return crc & 0xFFFFFFFF
    except OSError:
        return None


def _parse_range(header: str, total: int) -> tuple[int, int] | None:
    if not header.lower().startswith("bytes="):
        return None
    spec = header[len("bytes="):].strip()
    if "," in spec:
        return None
    try:
        if spec.startswith("-"):
            n = int(spec[1:])
            if n <= 0 or total == 0:
                return None
            return (max(0, total - n), total - 1)
        start_s, end_s = spec.split("-", 1)
        start = int(start_s)
        end = int(end_s) if end_s else total - 1
    except (ValueError, IndexError):
        return None
    if start < 0 or end < start or start >= total:
        return None
    return (start, min(end, total - 1))


class _Handler(http.server.BaseHTTPRequestHandler):
    root_dir: Path

    def log_message(self, fmt: str, *args) -> None:
        return  # quiet

    def _resolve(self) -> Path | None:
        rel = self.path.split("?", 1)[0].lstrip("/")
        full = (self.root_dir / rel).resolve()
        try:
            full.relative_to(self.root_dir)
        except ValueError:
            return None
        if not full.is_file():
            return None
        return full

    def _send_error(self, code: int) -> None:
        self.send_response(code)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_HEAD(self) -> None:  # noqa: N802
        path = self._resolve()
        if path is None:
            self._send_error(404); return
        info = _file_info(path)
        if info is None:
            self._send_error(404); return
        size, etag = info
        range_hdr = self.headers.get("Range")
        if range_hdr:
            r = _parse_range(range_hdr, size)
            if r is None:
                self.send_response(416)
                self.send_header("Content-Range", f"bytes */{size}")
                self.send_header("Content-Length", "0")
                self.send_header("ETag", etag)
                self.end_headers()
                return
            start, end = r
            length = end - start + 1
            range_crc = _crc32_range(path, start, length)
            if range_crc is None:
                self._send_error(500); return
            self.send_response(206)
            self.send_header("Content-Length", str(length))
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("ETag", etag)
            self.send_header("X-Content-CRC32", f"{range_crc:08x}")
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Length", str(size))
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("ETag", etag)
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        path = self._resolve()
        if path is None:
            self._send_error(404); return
        info = _file_info(path)
        if info is None:
            self._send_error(404); return
        size, etag = info

        range_hdr = self.headers.get("Range")
        if range_hdr:
            r = _parse_range(range_hdr, size)
            if r is None:
                self.send_response(416)
                self.send_header("Content-Range", f"bytes */{size}")
                self.send_header("Content-Length", "0")
                self.send_header("ETag", etag)
                self.end_headers()
                return
            start, end = r
            length = end - start + 1
            range_crc = _crc32_range(path, start, length)
            if range_crc is None:
                self._send_error(500); return
            self.send_response(206)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(length))
            self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("ETag", etag)
            self.send_header("X-Content-CRC32", f"{range_crc:08x}")
            self.end_headers()
            with path.open("rb") as f:
                f.seek(start)
                remaining = length
                while remaining > 0:
                    chunk = f.read(min(remaining, 1 << 20))
                    if not chunk:
                        break
                    self.wfile.write(chunk)
                    remaining -= len(chunk)
            return

        # No Range — full body.
        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(size))
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("ETag", etag)
        self.end_headers()
        with path.open("rb") as f:
            while True:
                chunk = f.read(1 << 20)
                if not chunk:
                    break
                self.wfile.write(chunk)


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

# Set to True only after _setup_output_dir() succeeds, so a failed startup
# (e.g. port already bound by someone else's process) doesn't wipe a
# directory that another process is serving from. Without this guard,
# a port collision below would still hit `finally: _cleanup_output_dir()`
# and shutil.rmtree someone else's data.
_we_own_output_dir = False


def _setup_output_dir() -> None:
    global _we_own_output_dir
    if os.path.exists(OUTPUT_DIR):
        shutil.rmtree(OUTPUT_DIR)
    os.makedirs(OUTPUT_DIR)
    _we_own_output_dir = True
    # Touch the file so the first poll has something to attach to.
    Path(STREAM_FILE).touch()
    print(f"Output directory: {OUTPUT_DIR}")
    print(f"Stream file:      {STREAM_FILE}")


def _cleanup_output_dir() -> None:
    if not _we_own_output_dir:
        return
    if os.path.exists(OUTPUT_DIR):
        shutil.rmtree(OUTPUT_DIR)
        print(f"Cleaned up {OUTPUT_DIR}")


def _writer_loop(stop: threading.Event) -> None:
    interval = 1.0 / RATE_PER_SEC if RATE_PER_SEC > 0 else 0.1
    with open(STREAM_FILE, "a", buffering=1) as f:
        while not stop.is_set():
            f.write(json.dumps(_make_event(), separators=(",", ":")) + "\n")
            stop.wait(interval)


def main() -> None:
    _setup_output_dir()
    _Handler.root_dir = Path(OUTPUT_DIR).resolve()

    server = http.server.ThreadingHTTPServer((HOST, PORT), _Handler)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()

    stop = threading.Event()
    writer_thread = threading.Thread(target=_writer_loop, args=(stop,), daemon=True)
    writer_thread.start()

    url = f"http://127.0.0.1:{PORT}/stream.jsonl"
    print(f"Serving on http://{HOST}:{PORT}/")
    print(f"Writing ~{RATE_PER_SEC:.0f} events/sec to stream.jsonl")
    print()
    print("Start the analyzer in another terminal:")
    print(f"  ./target/release/json_analyzer {url}")
    print()
    print("Press Ctrl-C to stop.")

    def _on_sigint(signum, frame):
        stop.set()
        server.shutdown()

    signal.signal(signal.SIGINT, _on_sigint)
    signal.signal(signal.SIGTERM, _on_sigint)
    try:
        while not stop.is_set():
            stop.wait(0.5)
    finally:
        server.shutdown()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nShutting down.")
    finally:
        _cleanup_output_dir()
