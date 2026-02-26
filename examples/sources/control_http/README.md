# Control HTTP Example

This example triggers action-period control commands over HTTP.

## Start the analyzer with control API enabled

```bash
cargo run --release -- --jsonl /tmp/json_demo/stream.jsonl --control-http 127.0.0.1:8080
```

## Trigger a single command

```bash
python3 examples/sources/control_http/control_client.py --base http://127.0.0.1:8080 --start --label api-demo
python3 examples/sources/control_http/control_client.py --base http://127.0.0.1:8080 --status
python3 examples/sources/control_http/control_client.py --base http://127.0.0.1:8080 --stop
```
