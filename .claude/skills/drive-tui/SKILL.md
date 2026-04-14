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
cd .claude/skills/drive-tui
./start.sh
sleep 2   # wait for app to start
```

### Basic test pattern
```bash
# 1. Capture baseline
./capture.py tui-test /tmp/before.json

# 2. Perform action(s)
./send_keys.sh tui-test j j j

# 3. Capture result
./capture.py tui-test /tmp/after.json

# 4. Diff
./diff_snapshots.py /tmp/before.json /tmp/after.json
```

### Reading the snapshot JSON

**`text_lines`** — plain text of each row. Good for checking content.

**`selections`** — rows that appear selected (background colour distinct from the screen modal bg, or reverse-video). Each entry has:
- `row`: row index
- `bg`: the row's background colour
- `text`: plain text of the row
- `reverse_video`: true if selected via reverse-video attribute

**`highlights`** — styled runs (bold, underline, coloured fg/bg, reverse). Each entry has:
- `row`, `col_start`, `col_end`, `text`
- `fg`, `bg`, `bold`, `underline`, `italics`, `reverse`

**`styled_lines`** — full per-row breakdown into contiguous segments with all style attributes. Use this when you need to inspect a specific region in detail.

### Example assertions to make

**Cursor/selection moved down by N rows:**
Check `selection_changes.moved` in diff output. `to_row - from_row` should equal N.

**Filter applied correctly:**
After typing `/foo Enter`, `text_lines` should only contain rows with "foo" (plus header/footer). `highlights` should show yellow-bg or bold runs on the matching text.

**Menu item active:**
Find the expected item text in `selections`. Its `bg` should differ from the dominant screen background.

**Underlined key hints visible:**
Check `highlights` for entries with `underline: true` on the relevant row.

**No visible change when key is invalid:**
`diff_snapshots.py` summary should say "No visible changes detected".

## Teardown
```bash
./stop.sh
```

## Tips
- Always `sleep 0.3` or more after sending keys that trigger data loading or animation before capturing — the TUI may not have re-rendered yet.
- For multi-character input (e.g. filter strings), send each character as a separate arg: `./send_keys.sh tui-test / h e l l o Enter`
- If the screen looks wrong, check `/tmp/tui-stderr.log` for Rust panics.
- The `$ARGUMENTS` passed to this skill are the test goal or scenario to exercise. Plan the steps, run them, and report pass/fail with evidence from the diff output.

## Test goal

$ARGUMENTS
