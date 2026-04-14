#!/usr/bin/env bash
# Usage: send_keys.sh [session] <keys...>
# Sends keystrokes to the tmux session. Keys are passed as separate args,
# each sent as one tmux send-keys call (so special names like 'Enter',
# 'Up', 'Down', 'Escape', 'BSpace' work correctly).
#
# Examples:
#   send_keys.sh tui-test j j j
#   send_keys.sh tui-test / f o o Enter
#   send_keys.sh tui-test Escape
#   send_keys.sh tui-test 'C-c'

SESSION="${1:-tui-test}"
shift

if ! tmux has-session -t "$SESSION" 2>/dev/null; then
  echo "ERROR: No tmux session '$SESSION'." >&2
  exit 1
fi

for key in "$@"; do
  tmux send-keys -t "$SESSION" "$key" ""
  sleep 0.05
done

echo "Sent ${#@} key(s) to '$SESSION'"
