# http_stream

HTTP-backed JSONL stream examples.

The canonical server shape is: any producer writes a JSONL file, and a
separate HTTP file server exposes the directory containing that file. The
server must support:

- HTTP `Range` requests
- `Content-Range`
- `Accept-Ranges: bytes`
- `ETag`
- `X-Content-CRC32` on ranged responses, including `HEAD` + `Range`

The analyzer uses byte ranges for tailing and uses the range CRC to
verify a persisted prefix across restarts without treating append-only
growth as a new stream.

## Canonical server

`file_server.py` binds to `0.0.0.0` by default so another device can
reach it on your machine's LAN address. Use `--host 127.0.0.1` if you
want local-only binding.

Run:

```bash
python3 examples/sources/http_stream/file_server.py /tmp/json_demo 8080
```

Analyzer (separate terminal):

```bash
./target/release/json_analyzer http://127.0.0.1:8080/stream.jsonl
```

This server is intentionally source-agnostic. It just serves files under
the root directory. Its ETag is a cheap stat-based file validator; the
content identity used for resume validation comes from `X-Content-CRC32`
on ranged requests.

## Demo with a Python source

Terminal 1, run an existing source that writes `/tmp/json_demo/stream.jsonl`:

```bash
python3 examples/sources/demo_source/demo_source.py
```

Terminal 2, serve that directory:

```bash
python3 examples/sources/http_stream/file_server.py /tmp/json_demo 8080
```

Terminal 3, point the analyzer at the URL:

```bash
./target/release/json_analyzer http://127.0.0.1:8080/stream.jsonl
```

You can swap in any other writer that appends JSONL to a file in
`/tmp/json_demo` or another served directory.

## Nested files

The server supports JSONL files in subdirectories under the configured
root. For example, serving `/tmp/json_demo` exposes
`/tmp/json_demo/high-volume/stream.jsonl` as:

```bash
http://127.0.0.1:8080/high-volume/stream.jsonl
```

See `examples/sources/high_volume` for a high-volume producer demo that
uses this layout.

## One-process smoke test

`http_stream.py` is still useful as a quick smoke test. It combines a
writer and the HTTP server in one process:

```bash
python3 examples/sources/http_stream/http_stream.py
```

On exit (Ctrl-C), it stops and removes `/tmp/json_demo/`.
