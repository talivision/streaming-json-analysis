# Partial Write Tester

This demo source writes JSONL continuously and intentionally emits partial lines before finishing them.
It uses live 13-digit epoch-millisecond `_timestamp` values so it can be used directly in live mode.
Every complete object is terminated by `\n`; partial objects are intentionally withheld until that newline is written.

It exists to validate the stream reader behavior for:

- complete lines being ingested immediately
- incomplete trailing lines being retained and retried on the next poll
- whitespace-only trailing lines being ignored at EOF

## Usage

Write to a temporary JSONL file:

```bash
python3 examples/sources/partial_writes/partial_write_tester.py /tmp/partial-write-demo.jsonl
```

Or slow it down further:

```bash
python3 examples/sources/partial_writes/partial_write_tester.py /tmp/partial-write-demo.jsonl --delay 2
```

Then point the analyzer at that file in another terminal:

```bash
cargo run -- /tmp/partial-write-demo.jsonl
```

Stop the writer with `Ctrl+C`.

## Expected Behavior

- Complete objects should appear immediately.
- A partially written object should not be ingested until its newline arrives.
- A whitespace-only tail should be ignored once EOF is reached.
- The writer should continue producing this pattern until stopped.
