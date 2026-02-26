# large_directory

Generate a large directory corpus with defaults:

```bash
python3 examples/generators/large_directory/generate_large_directory.py
```

Default output:
- `/tmp/json_demo/dir-180k`

Analyze (directory mode is offline-only):

```bash
./target/release/json-analyzer --directory /tmp/json_demo/dir-180k --offline
```
