#!/usr/bin/env python3
"""Serve a directory of JSONL files over the analyzer HTTP stream protocol.

This is the canonical example server for remote tailing. It serves ordinary
files from a root directory and supports the pieces the analyzer expects:

  - `Range: bytes=N-M` with 206 / 416 responses
  - `Content-Range` and `Accept-Ranges: bytes`
  - `ETag` for cheap file identity
  - `X-Content-CRC32` for the returned byte range, including HEAD+Range

Usage:
  python3 examples/sources/http_stream/file_server.py /tmp/json_demo 8080
  target/release/json_analyzer http://127.0.0.1:8080/stream.jsonl
"""

from __future__ import annotations

import argparse
import http.server
import zlib
from pathlib import Path


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


class Handler(http.server.BaseHTTPRequestHandler):
    root_dir: Path

    def log_message(self, fmt: str, *args) -> None:
        return

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

    def _send_empty(self, code: int) -> None:
        self.send_response(code)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _send_416(self, size: int, etag: str) -> None:
        self.send_response(416)
        self.send_header("Content-Range", f"bytes */{size}")
        self.send_header("Content-Length", "0")
        self.send_header("ETag", etag)
        self.end_headers()

    def _send_headers(
        self,
        *,
        status: int,
        length: int,
        etag: str,
        content_range: str | None = None,
        range_crc: int | None = None,
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(length))
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("ETag", etag)
        self.send_header("Access-Control-Allow-Origin", "*")
        if content_range is not None:
            self.send_header("Content-Range", content_range)
        if range_crc is not None:
            self.send_header("X-Content-CRC32", f"{range_crc:08x}")
        self.end_headers()

    def _path_info(self) -> tuple[Path, int, str] | None:
        path = self._resolve()
        if path is None:
            return None
        info = _file_info(path)
        if info is None:
            return None
        size, etag = info
        return path, size, etag

    def do_HEAD(self) -> None:  # noqa: N802
        info = self._path_info()
        if info is None:
            self._send_empty(404)
            return
        path, size, etag = info
        range_hdr = self.headers.get("Range")
        if range_hdr:
            r = _parse_range(range_hdr, size)
            if r is None:
                self._send_416(size, etag)
                return
            start, end = r
            length = end - start + 1
            range_crc = _crc32_range(path, start, length)
            if range_crc is None:
                self._send_empty(500)
                return
            self._send_headers(
                status=206,
                length=length,
                etag=etag,
                content_range=f"bytes {start}-{end}/{size}",
                range_crc=range_crc,
            )
            return
        self._send_headers(status=200, length=size, etag=etag)

    def do_GET(self) -> None:  # noqa: N802
        info = self._path_info()
        if info is None:
            self._send_empty(404)
            return
        path, size, etag = info
        range_hdr = self.headers.get("Range")
        if range_hdr:
            r = _parse_range(range_hdr, size)
            if r is None:
                self._send_416(size, etag)
                return
            start, end = r
            length = end - start + 1
            range_crc = _crc32_range(path, start, length)
            if range_crc is None:
                self._send_empty(500)
                return
            self._send_headers(
                status=206,
                length=length,
                etag=etag,
                content_range=f"bytes {start}-{end}/{size}",
                range_crc=range_crc,
            )
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
        self._send_headers(status=200, length=size, etag=etag)
        with path.open("rb") as f:
            while True:
                chunk = f.read(1 << 20)
                if not chunk:
                    break
                self.wfile.write(chunk)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root_dir", help="Directory to expose")
    parser.add_argument("port", nargs="?", type=int, default=8080)
    parser.add_argument("--host", default="0.0.0.0")
    args = parser.parse_args()

    root = Path(args.root_dir).resolve()
    if not root.is_dir():
        parser.error(f"{root} is not a directory")
    Handler.root_dir = root
    server = http.server.ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"serving {root} on http://{args.host}:{args.port}/", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        return 0
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
