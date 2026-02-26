# large_jsonl

Generate a large JSONL corpus with defaults:

```bash
python3 examples/generators/large_jsonl/generate_large_jsonl.py
```

Default output:
- `/tmp/json_demo/large-180k-128types.jsonl`

Analyze:

```bash
./target/release/json-analyzer --jsonl /tmp/json_demo/large-180k-128types.jsonl --offline
```
