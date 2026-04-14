You are doing exploratory QA on a Rust TUI app (`json_analyzer`) — a live JSONL stream analyzer with anomaly detection, triage, periods, type renaming, and session persistence. The goal is to find real bugs through creative, systematic testing — not just happy-path verification.

## Codebase

**Working directory:** `/Users/tali/software/json`

**Key source files:**
- `src/main.rs` — entry point, CLI parsing (`argh`)
- `src/domain.rs` — `AnalyzerModel`, event ingestion, anomaly scoring, filtering, `parse_segment` for nested paths
- `src/app.rs` — `App` struct, run loop, key handling, triage, persistence calls
- `src/persistence.rs` — `save_state`/`load_state`, `export_session`/`import_session`, `save_profile`
- `src/io.rs` — `StreamReader` (offset-based JSONL tail reader)
- `src/tui.rs` — all rendering

**Recent changes (last commit, all files modified):**
1. `persistence`: When stream file content has changed since last session, show a Y/N prompt instead of silently dropping state. `StateLoadResult::Changed` vs `Clean`. `session_renames` carry unmatched renames across ingest cycles.
2. `persistence`: Triage state (`triaged_event_indices`) is now saved/restored, validated by `(index, type_id)` pair.
3. `domain`: `parse_segment` now handles chained bracket ops on one segment (`matrix[0][1]`, `grid[][]`). `collect_values_at_path` fans out across wildcard expansions.
4. `tui`: Periods Events panel title degrades gracefully as panel narrows.

**CLI flags:**
```
[--jsonl <path>] [--directory <path>] [--baseline <path>] [--import <path>]
[--profile <path>] [--whitelist <path>] [--offline] [--debug-status]
[--reset] [--escape-strings] [--control-http <addr>] [<path>]
```

**Key bindings (navigation uses arrow keys, not vim):**
- `Arrow Up/Down`, `Home`, `End`, `PgUp`, `PgDn` — navigation
- `/` — substring filter; `z` — fuzzy filter; `e` — exact filter; `y` — toggle filters on/off
- `Space` — triage event (mark reviewed)
- `m` — toggle action period (live mode only)
- `f` — toggle follow mode
- `c` — clear events
- `1/2/3/4` — switch tabs (Live, Periods, Types, Data/Values)
- `x` — export session; `p` — export profile
- `r` — rename type; `n` — mark type as unrelated

**Testing infrastructure available:**
- `.claude/skills/drive-tui/` — tmux-based TUI driver (start/stop/send_keys/capture/diff)
  - `start.sh [session]` — `TUI_STREAM=<path>` env var sets input file; `TUI_COLS`/`TUI_ROWS` set terminal size
  - `send_keys.sh [session] <key...>` — each arg is one key; use `Up`/`Down`/`Space`/`Enter`/`Escape` etc.
  - `capture.py [session] [out.json]` — dumps screen to JSON with `text_lines`, `selections`, `highlights`
  - `diff_snapshots.py before.json after.json` — summarises visual changes
- `cargo run -- <args>` — run directly (compiled binary at `target/debug/json_analyzer`)
- `cargo test` — existing unit tests
- State files stored in `~/.local/share/json_analyzer/` (or XDG data dir)

**`_timestamp` format:** epoch milliseconds as a JSON number in a top-level `_timestamp` field. Required for live mode. Use `--offline` to skip this requirement.

## What to test

Think creatively. The task is to find real bugs — panics, data loss, incorrect behavior, display glitches, or logical errors. Here are categories to cover, but go beyond them:

### 1. Input data edge cases
- Empty JSONL file, file with only blank lines, file with non-JSON lines mixed in
- Single event, two events with identical structure but different values
- Very deep nesting (10+ levels), very wide objects (50+ keys)
- `_timestamp` as string instead of number; as seconds instead of ms; as 0; as negative
- Null values, empty string values, empty object `{}`, empty array `[]`
- Unicode in keys and values (emoji, RTL text, zero-width chars)
- Array paths: test `field[0]`, `field[]`, `matrix[0][1]`, `grid[][]` in filter expressions
- Conflicting types: events where the same key alternates between string and integer

### 2. CLI flags — verify each actually works
- `--reset`: should not load persisted state even if state file exists
- `--offline`: should accept events without `_timestamp`
- `--baseline <path>`: what happens when baseline file is identical to stream? Larger? Smaller?
- `--import <path>` with a valid export, with a corrupted export, with an export for a different stream
- `--profile <path>` with valid profile, with a profile containing unknown fields, with empty profile
- `--whitelist <path>` with a file, with nonexistent file
- `--directory <path>` with an empty directory, a directory with mixed JSON and non-JSON files
- `--escape-strings`: verify C1/invisible chars are escaped in the JSON preview pane
- `--control-http`: bind, send a request, verify response

### 3. Persistence round-trips — no data loss
- Export session (key `x`), then re-import with `--import`: all periods, renames, filters preserved?
- `--import` then export again: is the round-trip lossless?
- Save session state (auto on quit), restart with same file: verify full restore
- Save state with triaged events, restart: correct events remain triaged
- Triage event at index 0; save; restart; verify index-0 event is still marked
- Corrupt the state file (truncate it, insert invalid JSON): app should start cleanly, not panic
- Delete the state file mid-session: next auto-save should recreate it

### 4. File-changed prompt (new feature)
- Start fresh, do work (rename a type, set a filter, create a period, triage some events), quit
- Truncate the stream file (simulate log rotation): relaunch → prompt should appear
- At the prompt: press `Y` → renames and filters restored, periods discarded, triage validated
- At the prompt: press `N` → completely fresh state
- At the prompt: press `Escape` → same as N
- Edge: state has renames but no periods — prompt should not show "Cannot be restored" section
- Edge: state has only periods (no renames/filters) — prompt should show only "Cannot be restored"
- Edge: state has nothing restorable at all — prompt content should reflect that gracefully

### 5. Triage edge cases (new feature)
- Triage all visible events, then apply a filter that hides them — triage count in UI?
- Triage events across multiple types; verify triage count in Periods panel title
- After file-changed restore: triage by `(index, type_id)` — if new file has same type at same index, does it restore? If different type, does it correctly reject?
- Restore with a triaged index that is now out of bounds (file has fewer events): no panic
- Triage index 0 specifically (boundary)

### 6. Navigation boundary conditions
- Arrow Down at the last event: no wrap, no panic
- Arrow Up at the first event: no wrap, no panic
- Arrow Down/Up with zero visible events (aggressive filter): no panic
- Home/End with one event
- PgUp/PgDn with fewer events than page size
- Rapid key spam: hold Down for 2+ seconds worth of events

### 7. Live follow mode
- With follow ON, new events appended to file → cursor should auto-advance
- With follow OFF, new events appended → cursor stays, count updates
- Toggle follow while at middle of list: position preserved
- Append events while a filter is active: only matching events advance cursor

### 8. Large data responsiveness
- Generate 50k events, measure startup time and frame render time
- Apply a filter on 50k events: does the UI remain responsive (< 500ms)?
- Rapid navigation on 50k events: no stuttering

### 9. Periods tab
- Create a period, navigate to Periods tab, check event count is correct
- Delete a period: events revert to unperiod'd
- Narrow terminal to 60 columns: verify title degrades (new feature) rather than truncating badly

### 10. Types tab
- Rename type, verify name appears in Live view
- Mark type as unrelated, verify it's filtered
- Types filter: filter to one type, then delete filter, verify all types return
- `j` key in Types tab should jump to Live view filtered to that type

## How to run the TUI

```bash
cd /Users/tali/software/json/.claude/skills/drive-tui

# Start with a data file
TUI_STREAM=/tmp/test.jsonl ./start.sh SESSION_NAME
sleep 3  # wait for compile + init

# Navigate with arrow keys, not vim keys
./send_keys.sh SESSION_NAME Down Down Down

# Capture and diff
./capture.py SESSION_NAME /tmp/snap.json
./diff_snapshots.py /tmp/before.json /tmp/after.json

# Always check for panics
cat /tmp/tui-stderr.log | tail -20

./stop.sh SESSION_NAME
```

## What to report

For each test, report:
1. **What you tested** (concise description)
2. **Expected behavior**
3. **Actual behavior** (with evidence: screen text, diff output, stderr)
4. **PASS / FAIL / BUG**

For any BUG found: quote the relevant source lines and describe the root cause.

Focus on finding real bugs. If a test passes cleanly, say so briefly and move on. Prioritize the areas most likely to have bugs given the recent changes: file-changed prompt logic, triage persistence, session_renames, and the nested path parsing.
