---
name: regression-test
description: End-to-end regression sweep for the JSON analyzer TUI. Drives one or two real binaries via the drive-tui skill and asserts every user-facing feature still works after a change.
allowed-tools: Bash, Read, Write, Edit
---

# regression-test

Use this when a change touches `app.rs`, `domain.rs`, `persistence.rs`, `tui.rs`, `presence.rs`, `state_watcher.rs`, or the JSONL ingest path. Run end-to-end before merging.

Builds on `.claude/skills/drive-tui/` for tmux + keystroke + capture/diff plumbing. This skill is the *playbook*; drive-tui is the *driver*.

## Prerequisites

```bash
cd /path/to/repo   # must have Cargo.toml at the top
cargo build --release
```

The release build cuts startup latency from ~3s (debug) to <1s, which makes the timing-sensitive sync tests much more reliable.

## Fixtures

Two JSONL fixtures cover almost everything:

```bash
mkdir -p /tmp/regtest
python3 -c "
import json, random, time
random.seed(1); base = int(time.time()*1000)
# Single-shape fixture for filter/rename/period tests
types = ['login','logout','purchase','view','error']
with open('/tmp/regtest/single.jsonl','w') as f:
    for i in range(200):
        f.write(json.dumps({'_timestamp': base + i*50, 'type': random.choice(types),
                            'idx': i, 'payload': {'user': f'u{i%10}', 'amt': random.randint(1,999)}}) + '\n')
# Multi-shape fixture (4 distinct structural types) for merge_groups tests
shapes = [
    lambda i: {'_timestamp': base+i*50, 'event': 'login', 'user': f'u{i%5}'},
    lambda i: {'_timestamp': base+i*50, 'event': 'logout', 'session': f's{i%5}'},
    lambda i: {'_timestamp': base+i*50, 'event': 'purchase', 'amount': i*10},
    lambda i: {'_timestamp': base+i*50, 'event': 'view', 'page': f'p{i%3}'},
]
with open('/tmp/regtest/multi.jsonl','w') as f:
    for i in range(200):
        f.write(json.dumps(shapes[i%4](i)) + '\n')
"
```

## State directory hygiene

Each fixture path resolves to one SHA-keyed state file. **Always clean before each test run** so stale presence/shared files don't poison results:

```bash
python3 -c "
import hashlib, os, glob
for path in ['/tmp/regtest/single.jsonl', '/tmp/regtest/multi.jsonl']:
    h = hashlib.sha256(path.encode()).hexdigest()
    for ext in ('shared.json','local.json','shared.lock'):
        f = os.path.expanduser(f'~/.local/state/json-analyzer/{h}.{ext}')
        if os.path.exists(f): os.remove(f)
    for f in glob.glob(os.path.expanduser(f'~/.local/state/json-analyzer/{h}.presence.*.json')):
        os.remove(f)
"
```

## How to launch a session

```bash
SK=.claude/skills/drive-tui
TUI_STREAM=/tmp/regtest/single.jsonl $SK/start.sh op-a       # tmux session 'op-a' running cargo run
sleep 4                                                       # wait for cold start
$SK/send_keys.sh op-a 3 r                                     # press '3' then 'r'
$SK/capture.py op-a /tmp/regtest/cap.json                     # screen dump as JSON
$SK/stop.sh op-a                                              # tear down
```

For multi-op tests, launch `op-b` (or `op-c`) the same way pointing at the same `TUI_STREAM`. The presence indicator and shared-state watcher pick them up within ~5s.

## Test catalogue

Pass criterion is in each row. Run them in order — later tests assume earlier ones passed.

### A. Single-operator features (no multi-op state involvement)

| ID | What | How to invoke | Pass criterion |
|---|---|---|---|
| A1 | Live tail row count | start solo with `single.jsonl`, capture | `Events  row 200/200  objects 200  types 1` (or appropriate shape count) |
| A2 | `/` substring filter | send `/ l o g i n Enter` | `row N/N  objects 200  types ...` where N matches the live-tail substring count |
| A3 | `z` fuzzy filter | send `z f o o Enter` | row count drops to whatever fuzzy-matches |
| A4 | `=` exact-key filter | navigate to a key with Enter+arrows, `=` to clamp it | row count drops |
| A5 | `t/type filter` | `3 t` then type a name | row count drops |
| A6 | Types tab list | `3` | header `(list)`, all types visible with counts |
| A7 | Types path focus | `3 Enter` | header changes to `(details)`, path list keyed by selected type |
| A8 | Rename a type | `3 r`, backspace existing name, type new, Enter | name updates on the row |
| A9 | Toggle path override | `3 Enter` then Space on a path row | `[AUTO ...]` → `[MANUAL ...]` |
| A10 | Insert period | `2 i` then `10-50 Enter` | new row `[1] #1 action ... rows 10-50` |
| A11 | Edit period range | `2 e` then `5-30 Enter` | row updates |
| A12 | Rename period | `2 r` then `MyLabel Enter` | period label changes |
| A13 | Delete period | `2 d y` | period vanishes |
| A14 | Triage event in period | `2 Enter Space` (assuming period selected, Events focus) | `1/N triaged` in header |
| A15 | Un-triage | second `Space` on same event | back to `N untriaged` |
| A16 | JSON inspector fold | `1 Enter` then navigate to nested key, `f` | sub-keys hide |
| A17 | Set action label | `n` then type label, Enter | header reflects label |
| A18 | Export session (`x`) | `x Enter` accepting default path | `<stream>.session.json` exists, mode 0600 |
| A19 | Export profile (`p`) | `p Enter` accepting default path | `<stream>.profile.json` exists |
| A20 | Select+merge types | `3 s Down s g LABEL Enter` | `LABEL [merged] count=<sum>` row appears |
| A21 | Unmerge | navigate to merged row, `g y` | merged row gone, members reappear separately |

### B. Single-op exit and restart (persistence)

After A20+A21 done, run a session that performs A8, A10, A14, A18, A20, then `q q`.

| ID | What | Pass criterion |
|---|---|---|
| B1 | Renames restored | start fresh, navigate to `3`, the type still shows the custom name |
| B2 | Period restored | `2`, the period row still appears |
| B3 | Triage restored | drill into period events, `N/M triaged` matches what was set |
| B4 | Path overrides restored | navigate the type's path focus, `[MANUAL ...]` still marked |
| B5 | Merge groups restored | the merge group row still shows with the same count |
| B6 | Filters/label restored | the substring filter and action label set before exit are still set |

### C. `--reset` and `--import` and `--baseline` and `--control-http`

| ID | What | How | Pass criterion |
|---|---|---|---|
| C1 | `--reset` ignores shared | with a non-empty shared file on disk, `cargo run -- --reset <stream>` | UI starts blank: no periods, no renames, no merge groups |
| C2 | `--import` loads session export | `cargo run -- --import <session.json>` | UI shows everything that was exported |
| C3 | `--baseline` shows 4 tabs | `cargo run -- --baseline base.jsonl primary.jsonl` | tabs `1 Live │ 2 Periods │ 3 Types │ 4 Baseline` |
| C4 | `--control-http` start/stop | start `--control-http 127.0.0.1:18901`, then `curl -X POST .../action/start -d '{}'` and `.../action/stop` | both return `{"ok":true,...}` |

### D. Multi-operator sync (the headline feature)

Use `multi.jsonl`. Launch `op-a` and `op-b` against the same stream. Sleep 600ms after each mutation before capturing the other side.

| ID | What op-A does | Pass criterion on op-B |
|---|---|---|
| D1 | Rename a type | new name visible within 600ms |
| D2 | Insert a period | period row visible |
| D3 | Edit a period | period row updates |
| D4 | Delete a period | period row gone |
| D5 | Toggle path override | path's `[MANUAL ...]` appears |
| D6 | Triage an event | `1/N triaged` reflected |
| D7 | Un-triage same event | back to untriaged |
| D8 | Merge types `s Down s g LABEL Enter` | `LABEL [merged] count=<sum>` appears on op-b with **non-zero count** (existing events folded in) |
| D9 | Unmerge `g y` on merged row | merged row gone on op-b, members reappear |
| D10 | Set substring filter | op-b's filter is **unchanged** (local-only) |
| D11 | Change action label | op-b's label is **unchanged** (local-only) |
| D12 | Op-A exits cleanly (`q q`) | op-b still has all of op-a's shared mutations |
| D13 | Op-A restarts | op-a sees op-b's intervening mutations on startup |
| D14 | Op-A SIGINT (`C-c`) | shared state from before the last keystroke survives (eager-write); only un-flushed local fields lost |

### E. Presence indicator

| ID | Topology | Pass criterion |
|---|---|---|
| E1 | 1 op | **no** `Connected:` line visible |
| E2 | 2 ops, same user | `Connected: tali (×2)` right-aligned in controls bar |
| E3 | 3 ops, same user | `Connected: tali (×3)` |
| E4 | 1 op + manual stale presence file (`last_heartbeat_secs` from 60s ago) | indicator hidden — stale entry filtered out |
| E5 | Op-A exits | within ~15s op-b's indicator drops back to nothing (heartbeat times out) |

### F. Concurrency / lost-update edge cases

| ID | Scenario | Pass criterion |
|---|---|---|
| F1 | Op-A and op-B both rename **different** types within ~100ms | both renames survive |
| F2 | Op-A and op-B both toggle **different** path overrides | both survive |
| F3 | Op-A deletes period 1, op-B has stale snapshot with period 1 still present, op-B writes | period 1 stays deleted |
| F4 | Op-A un-triages event X, op-B writes with stale (X-still-triaged) snapshot | X stays un-triaged |
| F5 | Op-A merges {T1,T2}, op-B unmerges shortly after | last writer wins (acceptable — both intents are explicit) |

### G. Performance

| ID | What | Pass bar |
|---|---|---|
| G1 | Solo idle CPU over 60s | matches main-branch baseline within ±5% |
| G2 | Mutation-to-visible latency (op-A keypress → op-B capture shows it) | median < 400ms, p99 < 700ms over 10 trials |
| G3 | Ingest under 1000 ev/s with concurrent mutations on op-A | both ops keep up (objects count converges) |

## When something fails

1. **First, check `/tmp/tui-stderr.log`** for Rust panics or warnings.
2. **Read the actual capture** with `python3 -c "import json; s=json.load(open('/tmp/regtest/cap.json')); [print(repr(l[:160])) for l in s['text_lines']]"` — diff_snapshots.py is helpful but can miss subtle state.
3. **Inspect the disk state** at `~/.local/state/json-analyzer/<sha>.shared.json` and `<sha>.local.json` to see what actually got persisted. Mismatch between UI and disk = either a `mark_*_dirty` was missed at the mutation site, or the persist→merge logic dropped the field.
4. **For path-override and merge_groups bugs** specifically: the per-field merge in `persist_shared_state` keys by `path_override_key(...)` / `group_id`. If the key format used to *track* user modifications (`user_toggled_paths`, `user_modified_merge_groups`) doesn't match the key format used to *look up* during merge, every write silently drops the field. Check both sides agree.

## Cleanup

```bash
.claude/skills/drive-tui/stop.sh op-a 2>/dev/null
.claude/skills/drive-tui/stop.sh op-b 2>/dev/null
.claude/skills/drive-tui/stop.sh solo 2>/dev/null
rm -rf /tmp/regtest
# Optional: scrub state dir if your tests created lots of stale presence files
find ~/.local/state/json-analyzer -name "*.presence.*.json" -mmin +60 -delete
```

## $ARGUMENTS

If invoked with `$ARGUMENTS`, treat them as a scope hint:
- `sync` — run sections D, E, F only
- `pre-existing` — run section A only
- `merge` — run A20, A21, D8, D9 only
- empty — full sweep A→G
