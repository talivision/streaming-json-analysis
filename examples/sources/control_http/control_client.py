#!/usr/bin/env python3
import argparse
import json
import urllib.error
import urllib.request


def call(method: str, url: str, payload=None):
    body = None
    headers = {}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=body, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=3) as resp:
            text = resp.read().decode("utf-8", errors="replace")
            return resp.status, text
    except urllib.error.HTTPError as err:
        text = err.read().decode("utf-8", errors="replace")
        return err.code, text


def main():
    parser = argparse.ArgumentParser(description="Demo control HTTP start/stop/status calls")
    parser.add_argument("--base", default="http://127.0.0.1:8080", help="Base control URL")
    parser.add_argument("--start", action="store_true", help="Send only POST /action/start")
    parser.add_argument("--stop", action="store_true", help="Send only POST /action/stop")
    parser.add_argument("--status", action="store_true", help="Send only GET /action/status")
    parser.add_argument("--label", default="api-demo", help="Label for --start")
    args = parser.parse_args()

    base = args.base.rstrip("/")
    selected = [args.start, args.stop, args.status]
    selected_count = sum(1 for flag in selected if flag)
    if selected_count > 1:
        parser.error("choose only one of --start, --stop, --status")
    if selected_count == 0:
        parser.error("one action is required: --start or --stop or --status")

    if args.start:
        steps = [("POST", f"{base}/action/start", {"label": args.label})]
    elif args.stop:
        steps = [("POST", f"{base}/action/stop", None)]
    else:
        steps = [("GET", f"{base}/action/status", None)]
    for method, url, payload in steps:
        status, body = call(method, url, payload)
        print(f"{method} {url} -> {status}")
        print(body)
        print("-" * 60)


if __name__ == "__main__":
    main()
