# http_stream

HTTP-backed JSONL stream examples.

## Recommended setup

Write your producer however you like — its only job is to append JSONL
lines to a file. Then expose the containing directory with the bundled
`file_server.py`. The analyzer reads the URL.

```bash
# Terminal 1: your producer writes to /tmp/json_demo/stream.jsonl
python3 examples/sources/demo_source/demo_source.py

# Terminal 2: serve the directory
python3 examples/sources/http_stream/file_server.py /tmp/json_demo 8080

# Terminal 3: analyzer
./target/release/json_analyzer http://127.0.0.1:8080/stream.jsonl
```