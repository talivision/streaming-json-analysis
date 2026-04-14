---
name: drive-tui
description: Manually drive and test the JSON analyzer TUI app via tmux. Sends keystrokes, captures rendered screen state (including colours, highlights, selections, underlines), and diffs before/after snapshots to verify UI behaviour.
allowed-tools: Bash
---

# drive-tui

You are driving the JSON analyzer TUI for manual testing. All helper scripts live in `.claude/skills/drive-tui/`.

## Core tools

| Script | What it does |
|--------|-------------|
| `start.sh [session]` | Launch the app in a tmux session (default: `tui-test`) |
| `stop.sh [session]` | Kill the session |
| `send_keys.sh [session] <key...>` | Send keystrokes (each arg = one key) |
| `capture.py [session] [out.json]` | Dump screen to JSON (text + styled segments + selections + highlights) |
| `diff_snapshots.py before.json after.json` | Diff two snapshots, summarise changes |

## Special key names (pass to send_keys.sh)
`Enter`, `Escape`, `Up`, `Down`, `Left`, `Right`, `BSpace`, `Tab`, `Space`, `C-c`, `C-d`

## Workflow

### Setup
```bash
tmux new-session -d -s tui-test -x 120 -y 40 "cargo run -- --offline /tmp/test.jsonl 2>/tmp/tui-stderr.log"
# wait for compile+start:
for i in $(seq 1 30); do sleep 1; grep -qE "Running" /tmp/tui-stderr.log 2>/dev/null && echo ready && break; done
```

### Basic test pattern
```bash
# 1. Capture baseline
python3 .claude/skills/drive-tui/capture.py tui-test /tmp/before.json

# 2. Perform action(s)
.claude/skills/drive-tui/send_keys.sh tui-test Down Down Down

# 3. Capture result
python3 .claude/skills/drive-tui/capture.py tui-test /tmp/after.json

# 4. Diff
python3 .claude/skills/drive-tui/diff_snapshots.py /tmp/before.json /tmp/after.json
```

### Test data
`/tmp/test.jsonl` — 10 events at various log levels (info/warn/error/debug). Regenerate if missing:
```bash
python3 -c "
import json
events = [
    {'ts':'2026-04-14T10:00:00Z','level':'info', 'msg':'server started',        'port':8080},
    {'ts':'2026-04-14T10:00:01Z','level':'info', 'msg':'request received',      'path':'/api/users','latency_ms':12},
    {'ts':'2026-04-14T10:00:02Z','level':'warn', 'msg':'slow query',            'table':'events','duration_ms':450},
    {'ts':'2026-04-14T10:00:03Z','level':'error','msg':'connection failed',     'host':'db-01','attempts':3},
    {'ts':'2026-04-14T10:00:04Z','level':'info', 'msg':'request received',      'path':'/api/items','latency_ms':8},
    {'ts':'2026-04-14T10:00:05Z','level':'info', 'msg':'cache hit',             'key':'user:42','ttl':300},
    {'ts':'2026-04-14T10:00:06Z','level':'warn', 'msg':'rate limit approaching','client':'api-key-7','usage':0.89},
    {'ts':'2026-04-14T10:00:07Z','level':'info', 'msg':'request received',      'path':'/api/orders','latency_ms':34},
    {'ts':'2026-04-14T10:00:08Z','level':'debug','msg':'gc cycle',              'freed_mb':12.4},
    {'ts':'2026-04-14T10:00:09Z','level':'info', 'msg':'health check ok',       'uptime_s':9},
]
[print(json.dumps(e)) for e in events]
" > /tmp/test.jsonl
```

### Teardown
```bash
tmux kill-session -t tui-test 2>/dev/null; true
```

### Reading the snapshot JSON

**`text_lines`** — plain text of each row. Good for checking content.

**`selections`** — rows that appear selected. Detected via:
- distinct background colour vs the screen modal bg
- reverse-video attribute
- `->` or `=>` arrow marker in the row text (this app uses `->`)

Each entry has: `row`, `bg`, `reverse_video`, `arrow_marker`, `reason`, `text`.

**`highlights`** — styled runs (bold, underline, coloured fg/bg, reverse). Each entry has:
- `row`, `col_start`, `col_end`, `text`
- `fg`, `bg`, `bold`, `underline`, `italics`, `reverse`

Yellow match highlights in this app use `bg=cdcd00` (dark yellow / `Color::Yellow` in ratatui).

**`styled_lines`** — full per-row breakdown into contiguous segments with all style attributes. Use this when you need to inspect a specific region in detail.

### Example assertions to make

**Cursor/selection moved down by N rows:**
Check `selection_changes.moved` in diff output. `to_row - from_row` should equal N.

**Filter applied correctly:**
After typing `/foo Enter`, check `text_lines` — only matching rows visible. Check `highlights` for entries with `bg=cdcd00` (yellow) on the matching text in the JSON preview panel.

**Menu item active:**
Find the expected item text in `selections`. Its `bg` should differ from the dominant screen background.

**Underlined key hints visible:**
Check `highlights` for entries with `underline: true` on the relevant row.

**No visible change when key is invalid:**
`diff_snapshots.py` summary should say "No visible changes detected".

### Known behaviour notes
- Navigation in the Live view uses **arrow keys** (`Up`/`Down`), not `j`/`k`. The `j` key only works in the Types view.
- Yellow match highlights (`bg=cdcd00`) appear in the **right-hand JSON preview panel** of the selected event, not on every row in the event list.
- Pressing `Down` while `follow (f):ON` auto-disables follow mode.
- Filter status shown in bottom bar: `//sub=<term>` for substring filter.

## Test goal

$ARGUMENTS
