# directory_stream

Continuous directory writer for testing directory ingestion.

Run writer:

```bash
python3 examples/sources/directory_stream/stream_directory_writer.py \
  --output-dir /tmp/json_demo/stream-dir \
  --rate 500 \
  --events-per-file 1 \
  --clean
```

Analyze output:

```bash
./target/release/json-analyzer --directory /tmp/json_demo/stream-dir --offline
```
