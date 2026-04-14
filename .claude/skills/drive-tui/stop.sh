#!/usr/bin/env bash
# Usage: stop.sh [session-name]
SESSION="${1:-tui-test}"

if tmux has-session -t "$SESSION" 2>/dev/null; then
  tmux kill-session -t "$SESSION"
  echo "Killed session '$SESSION'"
else
  echo "No session '$SESSION' found"
fi
