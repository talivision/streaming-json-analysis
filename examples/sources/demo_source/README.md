# demo_source

Interactive JSONL stream demo.
The writer appends one complete JSON object per line and terminates every object with `\n`.

Run:

```bash
python3 examples/sources/demo_source/demo_source.py
```

Analyzer (separate terminal):

```bash
./target/release/json-analyzer /tmp/json_demo/stream.jsonl
```
