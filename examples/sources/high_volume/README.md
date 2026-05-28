# high_volume

High-volume JSONL writer. Drops a large backlog of typed events
(`metric`/`trace`/`request`/`job`/`error`) into a file, then keeps
appending at a slow steady-state rate so the analyzer has both a real
initial-load workload and a continuous tail.

The writer is transport-agnostic: it produces a local file. Feed that
file to the analyzer directly, or expose it over HTTP and point the
analyzer at the URL.

## Run the writer

```bash
python3 examples/sources/high_volume/high_volume_writer.py \
  --output /tmp/json_demo/stream.jsonl \
  --initial-events 100000 \
  --steady-rate 10 \
  --batch-size 250 \
  --truncate
```

## Consume the file directly

```bash
target/release/json_analyzer /tmp/json_demo/stream.jsonl
```

## Consume over HTTP

In a separate terminal, expose `/tmp/json_demo/` with the canonical HTTP
file server:

```bash
python3 examples/sources/http_stream/file_server.py /tmp/json_demo 8080
```

Then point the analyzer at the URL:

```bash
target/release/json_analyzer http://127.0.0.1:8080/stream.jsonl
```

`file_server.py` serves any file under the configured root and supports
`Range`, `Content-Range`, `ETag`, and `X-Content-CRC32` — the same
protocol the analyzer expects from any HTTP source.
