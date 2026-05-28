# http_stream

Simulated HTTP-backed JSONL stream. Runs a tiny HTTP file server and a
continuous writer in one process so the analyzer can be pointed at an
URL instead of a local path.

The server supports HTTP/1.1 Range requests (so the client only fetches
new bytes per poll) and emits a CRC32 content-hash `ETag` on every
response (cached by `mtime_ns + size + inode` so it only recomputes when
the file actually changes).

Run:

```bash
python3 examples/sources/http_stream/http_stream.py
```

Analyzer (separate terminal):

```bash
./target/release/json-analyzer http://127.0.0.1:8080/stream.jsonl
```

On exit (Ctrl-C), the server stops and `/tmp/json_demo/` is removed.

### Deploying for real

The Python source in `http_stream.py` is fine for a demo or a small
internal endpoint. For higher throughput or single-binary deployment, the
project ships an equivalent Rust server:

```bash
cargo run --release --bin stream_server -- /var/log/json-streams 8080
```

Same wire protocol — Range, CRC32 ETag, 206/200/416. The analyzer can't
tell them apart. See `src/bin/stream_server.rs`.
