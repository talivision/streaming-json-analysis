# jsonl_synth

Generate a synthetic JSONL fixture with a realistic mix of typed events
(heartbeats, logins, clicks, purchases, errors) and varied scalar values.
Useful for HTTP-stream perf testing and any benchmark where you want
the analyzer's anomaly path to actually do work rather than seeing
uniform `type_NNN` shapes.

Run:

```bash
python3 examples/generators/jsonl_synth/generate_jsonl_synth.py
```

Default output:

- `/tmp/json_demo/synth.jsonl` (50 MB)

Analyze:

```bash
./target/release/json-analyzer --jsonl /tmp/json_demo/synth.jsonl --offline
```

### Other sizes

```bash
# 5 MB
python3 examples/generators/jsonl_synth/generate_jsonl_synth.py \
    --output /tmp/json_demo/synth-5mb.jsonl --bytes 5000000

# 500 MB
python3 examples/generators/jsonl_synth/generate_jsonl_synth.py \
    --output /tmp/json_demo/synth-500mb.jsonl --bytes 500000000
```
