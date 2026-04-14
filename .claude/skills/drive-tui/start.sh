#!/usr/bin/env bash
# Usage: start.sh [session-name] [-- extra cargo args]
# Starts the TUI app in a detached tmux session.

SESSION="${1:-tui-test}"

if tmux has-session -t "$SESSION" 2>/dev/null; then
  echo "Session '$SESSION' already exists. Kill it first with stop.sh or attach with:"
  echo "  tmux attach -t $SESSION"
  exit 1
fi

# Determine terminal size: default 120x40 unless overridden
COLS="${TUI_COLS:-120}"
ROWS="${TUI_ROWS:-40}"

STREAM_ARG="${TUI_STREAM:+-- $TUI_STREAM}"
tmux new-session -d -s "$SESSION" -x "$COLS" -y "$ROWS" "cargo run $STREAM_ARG 2>/tmp/tui-stderr.log"

echo "Started session '$SESSION' (${COLS}x${ROWS})"
echo "Stderr log: /tmp/tui-stderr.log"
echo ""
echo "Wait a moment for the app to initialise, then capture with:"
echo "  ./capture.py $SESSION"
