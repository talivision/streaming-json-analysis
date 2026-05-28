---
name: regression-test
description: End-to-end regression sweep for the JSON analyzer TUI. Drives the real release binary via the drive-tui skill and asserts every user-facing feature still works after a change.
allowed-tools: Bash, Read, Write, Edit
---

# regression-test

Use this when a change touches `app.rs`, `domain.rs`, `persistence.rs`, `tui.rs`, `io.rs`, or `main.rs`. Run end-to-end before merging.

Builds on `.claude/skills/drive-tui/` for tmux + keystroke + capture/diff plumbing. This skill is the *playbook*; drive-tui is the *driver*.

## Prerequisites

```bash
cd /path/to/repo   # must have Cargo.toml at the top
cargo build --release
```

The release build cuts cold start from ~3s to <1s, which matters for the stutter-capture pattern (see below).

## Fixtures

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

Each fixture path resolves to one SHA-keyed state file (`<sha>.state.json`) plus a swapfile (`<sha>.swap.json`) that's held for the lifetime of the running TUI. **Always clean before each test run** so stale state doesn't poison results:

```bash
python3 -c "
import hashlib, os, glob
state_dir = os.path.expanduser('~/.local/state/json-analyzer')
for path in ['/tmp/regtest/single.jsonl', '/tmp/regtest/multi.jsonl']:
    abs_path = os.path.realpath(path)
    h = hashlib.sha256(abs_path.encode()).hexdigest()
    for ext in ('state.json', 'swap.json'):
        f = os.path.join(state_dir, f'{h}.{ext}')
        if os.path.exists(f): os.remove(f)
# Also nuke any leftover legacy files from older builds
for pat in ('*.shared.json', '*.local.json', '*.shared.lock', '*.presence.*.json'):
    for f in glob.glob(os.path.join(state_dir, pat)):
        os.remove(f)
"
```

## How to launch a session

```bash
SK=.claude/skills/drive-tui
TUI_STREAM=/tmp/regtest/single.jsonl $SK/start.sh solo
sleep 2
$SK/send_keys.sh solo 3 r
$SK/capture.py solo /tmp/regtest/cap.json
$SK/stop.sh solo
```

## The stutter-capture pattern (load-bearing)

**Most of the bugs this skill failed to catch in the past had the same shape: a mutation rendered correctly on the next frame, then a subsequent persist / reload / cache rebuild silently reverted it.** A single capture sees only the first frame and passes.

For any test in Section A or D, after a mutation:

```bash
$SK/send_keys.sh solo m                   # mutate
sleep 0.05
$SK/capture.py solo /tmp/regtest/t0.json  # immediate
sleep 0.5                                  # let any background persist / reload settle
$SK/capture.py solo /tmp/regtest/t1.json  # post-settle
$SK/diff_snapshots.py /tmp/regtest/t0.json /tmp/regtest/t1.json
# Expected: "No visible changes detected" (or only timer-driven changes
# in the status row).
```

A diff between t0 and t1 that shows the mutation reverting is a regression. Apply this around every mutation in Section D especially.

## Test catalogue

Run in order — later tests assume earlier ones passed. Use `single.jsonl` unless noted.

### A. Single-action features

| ID | What | How to invoke | Pass criterion |
|---|---|---|---|
| A1 | Live tail row count | start solo, capture | header shows `objects 200` and one or more `types` row |
| A2 | `/` substring filter | `/ l o g i n Enter` | row count drops; matches highlighted |
| A3 | `z` fuzzy filter | `z f o o Enter` | row count drops |
| A4 | Exact key=value filter | navigate to a value with `Enter` + arrows, press `e` | row count drops to matches |
| A5 | Type-name filter (`t`) | `3 t l o g i n Enter` | only `login` types remain |
| A6 | Types tab list | `3` | header `(list)`, all types visible with counts |
| A7 | Types path focus | `3 Enter` | header changes to `(details)`; path list keyed by selected type |
| A8 | Rename a type to a name | `3 r BSpace BSpace ... LoginEvt Enter` | name updates on the row |
| A9 | Toggle path override | `3 Enter Space` on a path row | `[AUTO ...]` → `[MANUAL ...]` |
| A10 | Insert period | `2 i 1 0 - 5 0 Enter` | new period row appears, rows 10-50 |
| A11 | Edit period range | `2 e 5 - 3 0 Enter` | range updates on selected period |
| A12 | Rename period | `2 r M y L a b e l Enter` | period label updates |
| A13 | Delete period | `2 d y` | period vanishes |
| A14 | Triage event in period | navigate to event in period, `Space` | `1/N triaged` in header |
| A15 | Un-triage | second `Space` on same event | back to `0/N triaged` |
| A16 | JSON inspector fold | `1 Enter`, navigate to nested key, `f` | sub-keys collapse |
| A17 | Set action label | `n` (prefills with current label, default `"action"`), then `BSpace BSpace BSpace BSpace BSpace BSpace` to clear, then `M y L a b e l Enter` | label set to exactly `MyLabel` in state.json (don't forget the BSpace clear or you'll append) |
| A18 | Export session (`x`) | `x Enter` | `<stream>.session.json` exists, mode 0600 |
| A19 | Export profile (`p`) | `p Enter` | `<stream>.profile.json` exists |
| A20 | Select + merge types (use `multi.jsonl`) | `3 s Down s g A u t h Enter` | merged-group row appears with summed count |
| A21 | Unmerge | navigate to merged row, `g y` | merged row gone, members reappear |

**For each of A8, A10, A11, A12, A13, A14, A20, A21: apply the stutter pattern.** Capture immediately and again 500ms later — assert no revert.

### B. Persistence round-trip

Run a session that performs A8, A10, A14, A20, then `q q`. Restart with no `--reset`.

| ID | What | Pass criterion |
|---|---|---|
| B1 | Renames restored | `3` — the type still shows the custom name |
| B2 | Period restored | `2` — the period row still appears |
| B3 | Triage restored | drill into period events, `N/M triaged` matches |
| B4 | Path overrides restored | navigate the type's path focus, `[MANUAL ...]` still marked |
| B5 | Merge groups restored | merged row still shown with same count |
| B6 | Filters/label restored | substring filter and action label still set |
| B7 | **Rename-to-blank persists** | from prior session, rename type via `r BSpace BSpace... Enter` (empty). Restart. Type displays its default (`type-…`) name, NOT the prior name |

### C. CLI flags

| ID | What | How | Pass criterion |
|---|---|---|---|
| C1 | `--reset` ignores state | with non-empty state on disk, `cargo run -- --reset <stream>` | UI starts blank — no periods, renames, or merge groups |
| C2 | `--import` loads session | `cargo run -- --import <session.json>` | UI shows everything that was exported |
| C3 | `--baseline` shows 4 tabs | `cargo run -- --baseline base.jsonl primary.jsonl` | tabs `1 Live │ 2 Periods │ 3 Types │ 4 Baseline` |
| C4 | `--control-http` start/stop | start `--control-http 127.0.0.1:18901`, then `curl -X POST localhost:18901/action/start -d '{}'` and `.../action/stop` | both return `{"ok":true,...}`; status flips in the running TUI |

### D. State-change interactions (the bug-catcher section)

These tests exist because the most damaging bugs we've seen this codebase hit were never single-action failures — they were "do X, then Y, watch Y silently revert." Run each test with the stutter pattern; the regression is a *diff between t0 and t1*, not a static check.

| ID | Sequence | Pass criterion |
|---|---|---|
| D1 | Create 3 periods (A10 × 3), delete the most recent (A13), press `m` | New open period appears AND survives 500ms stutter capture |
| D2 | Rename type to "X" (A8), then rename it to "" (BSpace × N, Enter) | Display name reverts to default `type-…`; no flash back to "X" in stutter capture |
| D3 | Rename type to "X", restart, rename to "" | Same as D2 — exercises the persist-then-restore path |
| D4 | Insert period (A10), no further action | Period still present after 500ms stutter capture |
| D5 | Insert period, delete it, insert a different range with `i` | New period present in stutter capture |
| D6 | Merge types (A20), unmerge (A21), merge the same set again | Second merge sticks across stutter capture |
| D7 | Mark period (`m`), wait for one new event to arrive, mark again | Closed period has end > start; no degenerate span |
| D8 | Press `m` twice rapidly (within ~10ms, send_keys with both args) | `model.periods.len()` ends at 0; no open period; no degenerate closed period |
| D9 | Apply a profile that renames type T, then rename T to "" by hand | Stays blank for the rest of the session (stutter capture). **No restart claim** — see note below. |
| D10 | Toggle path override on (A9), restart, toggle it back to default | Override disappears in stutter capture. **The cycle is tri-state: None → ForcedOff → ForcedOn → None when the path is auto-considered, and None → ForcedOn → None when not. Press Space enough times to land back on None — for an auto-on path that's two Spaces from the restored ForcedOff.** |

**D1 specifically reproduces the `m`-flash bug.** D2/D3 cover rename-to-blank. D4/D5 cover the `i` blink. D8 covers the double-tap. All four were in the bug notes that opened the original session.

**Note on D9 / `--profile` semantics.** Loading `--profile foo.json` is declarative: the profile is authoritative on load. The `user_renamed_types` guard is **same-session only** — it stops the profile from clobbering a deliberate edit you just made, *while you're in the session*. After a clean restart with the same `--profile`, the profile re-applies in full; the user-blanked names will come back. If you want a profile rename to stop applying, remove it from the profile JSON. Do not write a regression test that expects the blank to survive `--profile` on restart.

### E. Swapfile / crash recovery

| ID | Scenario | Pass criterion |
|---|---|---|
| E1 | Launch op-a on `single.jsonl`, then launch op-b on the same path **without `--force`** | op-b exits with non-zero status; stderr contains `E325: ATTENTION` and the pid of op-a |
| E2 | op-a still running, launch op-b with `--force` | op-b starts successfully; `<sha>.swap.json` now records op-b's pid (op-a is now orphaned but harmless) |
| E3 | Plant a stale swapfile (`pid: 0, hostname: <local>`), launch fresh | App starts without warning; swap is silently reclaimed |
| E4 | Op-a running, `kill -9` it (simulate crash), launch fresh | App starts; previous-session swapfile reclaimed (pid not alive); state.json restored normally |
| E5 | Clean exit (`q q`) | Swapfile is removed (`ls ~/.local/state/json-analyzer/<sha>.swap.json` → no such file) |
| E6 | Two instances launched *simultaneously* (no stagger) | Exactly one survives, the other exits with `E325`. Swap.pid matches the survivor. Run 5 trials to surface scheduler-dependent races. |

E6 is the one E1 doesn't cover. The naive `read_swapfile`-then-`atomic_write` implementation passes E1 (sequential) but TOCTOU-races on E6. The correct implementation uses `std::fs::File::try_lock` (OFD-level kernel advisory lock) so two simultaneous opens of the swap path both succeed, but only one wins the lock — the loser sees `TryLockError::WouldBlock` and returns `Held`. Helper:

```bash
SHA=$(python3 -c "import hashlib,os; print(hashlib.sha256(os.path.realpath('/tmp/regtest/single.jsonl').encode()).hexdigest())")
for i in 1 2 3 4 5; do
  rm -f ~/.local/state/json-analyzer/${SHA}.{state,swap}.json
  tmux kill-session -t race_a 2>/dev/null; tmux kill-session -t race_b 2>/dev/null
  tmux new-session -d -s race_a -x 120 -y 40 "target/release/json_analyzer /tmp/regtest/single.jsonl 2>/tmp/race_a-${i}.log" &
  tmux new-session -d -s race_b -x 120 -y 40 "target/release/json_analyzer /tmp/regtest/single.jsonl 2>/tmp/race_b-${i}.log" &
  wait; sleep 1.2
  A=$(tmux has-session -t race_a 2>/dev/null && echo alive || echo dead)
  B=$(tmux has-session -t race_b 2>/dev/null && echo alive || echo dead)
  echo "trial $i: a=$A b=$B"
  tmux kill-session -t race_a 2>/dev/null; tmux kill-session -t race_b 2>/dev/null
done
```

Pass: exactly one `alive`, exactly one `dead`, every trial.

Helper for E3:

```bash
SHA=$(python3 -c "import hashlib,os; print(hashlib.sha256(os.path.realpath('/tmp/regtest/single.jsonl').encode()).hexdigest())")
echo '{"pid":0,"hostname":"'$(hostname)'","stream_path":"/tmp/regtest/single.jsonl","created_at_secs":0}' \
  > ~/.local/state/json-analyzer/${SHA}.swap.json
```

### F. Performance / smoke

| ID | What | Pass bar |
|---|---|---|
| F1 | Solo idle CPU over 60s | matches main-branch baseline within ±5% |
| F2 | Mutation-to-render latency (keypress → next frame shows change) | median < 100ms (no watcher in the loop now; should be a tight render budget) |
| F3 | Ingest 1000 ev/s with concurrent mutations | UI stays responsive, `objects` count keeps climbing without stalls |

## When something fails

1. **First, check `/tmp/tui-stderr.log`** for Rust panics or warnings.
2. **Dump the on-disk state**: `python3 -m json.tool ~/.local/state/json-analyzer/<sha>.state.json | less`. Compare with what the UI showed. Mismatch = either a `mark_dirty` was missed at the mutation site, or `build_state_for_save` is dropping the field. Disk is authoritative.
3. **Inspect the swapfile**: `cat ~/.local/state/json-analyzer/<sha>.swap.json`. While the app is running, the pid here should be the running TUI's pid. After clean exit, the file should be gone.
4. **Stutter regressions specifically**: if t0 shows the mutation and t1 reverts it, something during persist or autosave is rebuilding state. The autosave path runs `persist_state` (full write) every 30s; nothing else should mutate `model.periods` / `model.types` / `triaged_event_indices` between t0 and t1 unless the user did. Look for stray `set_periods` / `apply_renames` calls firing on a timer or watcher.
5. **For rename-to-blank specifically**: `model.renamed_types()` filters out `None`-named types. If a profile rename or `session_renames` re-applies, the type appears renamed again on the next restore. The fix lives in `apply_profile_overrides_to_types` — confirm the `user_renamed_types` guard still gates the profile re-application.

## Cleanup

```bash
.claude/skills/drive-tui/stop.sh solo 2>/dev/null
rm -rf /tmp/regtest
# State files are small; clear them per-fixture rather than blast the whole dir
```

## $ARGUMENTS

If invoked with `$ARGUMENTS`, treat as a scope hint:
- `single` — section A only
- `persist` — sections B and E
- `interactions` — section D (bug-catcher; the highest-ROI section)
- `swap` — section E only
- `cli` — section C only
- empty — full sweep A → F
