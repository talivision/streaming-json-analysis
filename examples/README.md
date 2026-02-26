# Examples

## Layout

- `generators/`
  - `generate_large_jsonl.py` - produce a large JSONL file corpus.
  - `generate_large_directory.py` - produce a large directory corpus (JSON files).
- `sources/`
  - `demo_source/demo_source.py` - interactive demo source writing JSONL to `/tmp/json_demo/stream.jsonl`.
  - `directory_stream/stream_directory_writer.py` - continuous directory writer for directory-mode testing.

## Quick Commands

Generate a 180k directory corpus:

```bash
python3 examples/generators/generate_large_directory.py \
  --output-dir /tmp/json_demo/dir-180k \
  --events 180000 \
  --events-per-file 1
```

Run analyzer on a directory (requires `--offline`):

```bash
./target/release/json-analyzer --directory /tmp/json_demo/dir-180k --offline
```

Run interactive JSONL demo source:

```bash
python3 examples/sources/demo_source/demo_source.py
```

Run continuous directory stream writer:

```bash
python3 examples/sources/directory_stream/stream_directory_writer.py \
  --output-dir /tmp/json_demo/stream-dir \
  --rate 500 \
  --events-per-file 1 \
  --clean
```
