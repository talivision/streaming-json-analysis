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
| A22 | Values browser (`v`) round-trip | drill into a key in Live (`1 Enter`, navigate to a key row), press `v` | UI switches to a `Values` modal listing all observed values for that key with counts. `Esc` returns to Live; `Enter` applies the selected value as an exact filter and returns. Regression (df669da): the browser must not jump to Live when opened from Baseline / Periods. |
| A23 | Whitelist mode cycle (`w`) | start with `--whitelist /tmp/regtest/wl.txt` (containing `login` on one line), then press `w`, `w`, `w` | status flips Off → AlwaysShow → OnlyWhitelist → Off; matching events show with orange bg highlight when whitelist is active. With no `--whitelist` arg, `w` is a no-op (status: "no whitelist loaded" or similar). |
| A24 | `--escape-strings` CLI flag | run `cargo run --release -- --escape-strings /tmp/regtest/single.jsonl` and look at any string value containing a literal `\t` or newline in raw bytes | shown as escape sequence rather than the literal byte. Regression check only — verify the flag parses and the binary launches; full byte-escape semantics are unit-tested separately. |

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
| B8 | **Restart re-ingests events from byte 0** | After any session that consumed events (e.g., A8 + A10), `q q`, restart. Header still shows `objects 200` (the full fixture count), not `objects 0` or "N new since last session". `saved_len` + `prefix_hash_hex` are an identity checkpoint for rotation detection only — the reader stays at offset 0 on Clean so the in-memory model rebuilds from the stream. A restart that shows fewer objects than the on-disk file means `verify_resume` is incorrectly seeking the reader forward. |
| B9 | **Canonical-path equivalence** | from `/tmp/regtest/`: run with `./single.jsonl` (relative), rename a type, `q q`. Re-run with `/tmp/regtest/single.jsonl` (absolute). | The rename is restored — relative and absolute forms resolve to the same `<sha>.state.json`. Regression (bd01bdd): previously the SHA was over the literal input string, so `./foo.jsonl` and `/abs/foo.jsonl` were different state files. The fix canonicalizes the path before hashing. |
| B10 | **`q` inside an input buffer doesn't bail** | open any input prompt (e.g. `n` for the action label, or `/` for substring filter), then type `q a q b q Enter` | the buffer accepts the `q` characters; the session does NOT prompt to quit, and the resulting label is `qaqbq`. Regression (bd01bdd): the `q` quit-confirm dance used to run before the input-mode dispatcher, so any `q` keystroke inside an input prompt silently aborted the prompt instead of being captured. |

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
| D11 | **Live cursor unstuck after type filter + Home + Down** | start a feed where many events arrive within one millisecond (use the `examples/sources/high_volume_http` writer or just a synthetic 100k-event fixture with repeating `_timestamp` values). Filter to one type (`3 Enter t Enter`), return to Live (`1`), `Home`, then `Down`, `Down`, `Down`. | Cursor moves down by 3 rows in the visible-event list across captures. Regression (00841dc): `LiveAnchor` keyed by `(ts, type_id)` collapsed when many events shared a timestamp; `find_live_index` always returned the first match, so the post-poll cursor-restore pinned the row to index 0 every iteration. The fix keys the anchor by `event_idx` (a stable position in `model.events`). |
| D12 | Mark / unmark period during sustained ingest | start a writer at ≥1000 ev/s, press `m` once, wait 2s, press `m` again. | No visible UI stutter or stall during either keypress. The whole mark/unmark cycle completes in <100 ms each. Regression (8c16a85): the file-backend poll cadence used to be 10 ms / 100 Hz and would saturate the UI thread with `refresh_live_anomaly_scores` recomputation, making single keystrokes visibly stall on busy streams. Cadence is now throttled to 50 ms (file) / 100 ms (HTTP idle). |

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
| F4 | **Malformed-final-line perf canary** | See script below. Build a 500k-event high-cardinality JSONL ending in an unterminated final line, run the binary against it with stdout/stderr captured, and `time` it through to clean exit. **Goal: confirm a recent build is NOT slower than its parent commit by more than ~10%.** This is the single best catch-all perf test — it exercises ingest, structural hashing, anomaly scoring, path stats, and shutdown-time tail scan in one shot. |
| F5 | Large unterminated final line (>8 KB) emits warning at shutdown | Build a fixture whose last "line" is > 16 KB with no trailing newline (just one giant JSON-ish blob). Launch the binary, let it ingest, `q q`. Stderr must contain `incomplete JSON line remained at shutdown`. Regression: the old 8 KB tail-scan window (e0ee540) missed the warning when the unterminated tail was larger than the scan budget. |
| F6 | High-cardinality ingest is linear, not quadratic | Same 500k fixture as F4 but with one field carrying a unique value per event (`{"_timestamp":..., "_type":"e", "seq": <unique-per-line>}`). Time `cargo run --release -- <fixture>` through to `q q`. **Should finish in seconds, not minutes.** Regression: pre-9da29e7 path-stats recomputation was O(n²) and hung the analyzer past ~100k events. |

**F4 / F6 are the canaries for ingest-loop regressions.** They run in <5s on green builds; any commit that makes them take >2× the parent commit is suspicious.

| F7 | **Stats parity across commits (bit-identical output)** | Build a deterministic mixed-shape fixture (timestamps + types fully fixed), run the binary against it on HEAD and on an older commit, capture the Types tab and the type-detail tab on each, `diff` the captures. | Captures are **byte-identical**. Sufficient validation that any perf-only refactor (e.g. 9da29e7's PathStats numeric-token cache) actually preserved scoring/counting behavior. Regression: any commit that changes path-stats math, anomaly scoring, or type-count rollup will surface as a diff line. See `## Stats parity` below for the helper. |

```bash
# F4 / F5 / F6 fixture generator
python3 -c "
import json, sys
n = int(sys.argv[1])
high_card = sys.argv[2] == 'high'
trail_blob = sys.argv[3] == 'blob'
with open('/tmp/regtest/perf.jsonl', 'w') as f:
    for i in range(n):
        rec = {'_timestamp': 1700000000000 + i, '_type': 'e', 'idx': i}
        if high_card:
            rec['seq'] = f'seq-{i}'
            rec['metric'] = i * 1.0001
        f.write(json.dumps(rec) + '\n')
    if trail_blob:
        # unterminated final blob > 16 KB, deliberately not valid JSON
        f.write('{ \"incomplete\": \"' + ('x' * 17000) + '\"')
    else:
        # plain unterminated final line, valid-ish JSON but no newline
        f.write('{\"_timestamp\":1700000999999,\"_type\":\"e\",\"idx\":' + str(n))
" 500000 high blob

# Time the binary cleanly: open, ingest, quit.
# Pipe 'qq' on stdin via tmux so the app exits as soon as it's idle.
SK=.claude/skills/drive-tui
TUI_STREAM=/tmp/regtest/perf.jsonl $SK/start.sh perf
sleep 0.2
# wait for initial load to finish; poll the screen for 'objects 500000' (allow some slop on unterminated tail = -0 or -1)
for _ in {1..120}; do
  $SK/capture.py perf /tmp/regtest/perf.capture.json >/dev/null
  grep -q 'objects 499999\\|objects 500000' /tmp/regtest/perf.capture.json && break
  sleep 0.25
done
START=$(date +%s%N)
$SK/send_keys.sh perf q q
# Wait for tmux session to die
for _ in {1..40}; do tmux has-session -t perf 2>/dev/null || break; sleep 0.1; done
END=$(date +%s%N)
echo "perf wall-clock: $(( (END-START) / 1000000 )) ms"
grep -i 'incomplete JSON line remained at shutdown' /tmp/tui-stderr.log || \
  echo "FAIL F5: no shutdown warning for unterminated final line"
```

To compare against an older commit, see the **Performance comparison vs older commit** section below.

### G. HTTP source

The analyzer accepts `http://...` and `https://...` in place of a local path. The HTTP backend uses `Range: bytes=<saved_len>-` polling and tracks a prefix-CRC identity (`crc32:<hex>:<len>`) — **not** a whole-file ETag, since the file ETag changes on every append for a live stream and would always look like a rotation. `examples/sources/http_stream/file_server.py` is the canonical example server; it exposes `X-Content-CRC32` on 206 / HEAD responses so the client can verify the prefix without re-downloading it. `http_stream.py` bundles a writer + server in one process for one-shot smoke tests.

Launch the file server for these tests (writer is separate):

```bash
mkdir -p /tmp/json_demo
: > /tmp/json_demo/stream.jsonl
python3 examples/sources/http_stream/file_server.py /tmp/json_demo 8080 &
FS=$!
for _ in {1..40}; do nc -z 127.0.0.1 8080 2>/dev/null && break; sleep 0.05; done
```

Clean up at end of section:

```bash
kill $FS 2>/dev/null
URL_SHA=$(python3 -c "import hashlib; print(hashlib.sha256(b'http://127.0.0.1:8080/stream.jsonl').hexdigest())")
rm -f ~/.local/state/json-analyzer/${URL_SHA}.{state,swap}.json
```

| ID | What | How | Pass criterion |
|---|---|---|---|
| G1 | Events flow over HTTP | `http_stream.py` running; `target/release/json_analyzer http://127.0.0.1:8080/stream.jsonl` | `objects` header > 0 and increases over time; types appear in `3` |
| G2 | Append growth is **not** treated as rotation | While analyzer is running, write a new event to `/tmp/json_demo/stream.jsonl` (or rely on http_stream.py's built-in writer). | UI keeps ingesting smoothly; no "stream changed since last session" prompt appears; `pending_rotation` stays false. Regression: whole-file ETag mismatch on every append would trigger this prompt. |
| G3 | HTTP restart re-ingests from byte 0 | `q q`, relaunch with same URL while server is still running. | Same behaviour as B8 — header shows current `objects` count == events on disk, not 0 and not "since last session". |
| G4 | Annotations persist over HTTP | While running: A8 (rename), A10 (insert period), `q q`. Relaunch same URL. | Renames + periods restored exactly as B1 / B2. Regression: if `mark_dirty` short-circuits on `!source_exists()`, HTTP runs would silently never persist anything. HTTP `source_exists()` must return true regardless of `pending_rotation`. |
| G5 | True rotation IS detected | While analyzer is running and server is serving, truncate the file (`> /tmp/json_demo/stream.jsonl`) then have the writer append new events. | "stream changed" prompt fires; user can choose to keep annotations or discard. |
| G6 | 416 at exact EOF is a no-op, not rotation | Set up: poll catches up to EOF; server returns 416 (range starts past current end). | Analyzer treats it as "no new bytes this poll" and keeps polling. `pending_rotation` does NOT flip to true. Verify by capturing footer / status across several polls during a quiet period. |
| G7 | 206 success clears prior `pending_rotation` | After G5 triggers a rotation flag, append more events so a 206 succeeds. | `pending_rotation` returns to false (observable indirectly: subsequent annotations save correctly per G4). |
| G8 | `X-Content-CRC32` header round-trips | `curl -sI -H 'Range: bytes=0-100' http://127.0.0.1:8080/stream.jsonl` | Response includes `X-Content-CRC32: <8 hex chars>` matching `python3 -c "import zlib; print(format(zlib.crc32(open('/tmp/json_demo/stream.jsonl','rb').read()[:101]),'08x'))"`. Regression: identity verification on the client depends on this header. |

**G2 is the canary for the prefix-vs-whole-file ETag bug.** With a whole-file ETag, every append changes the ETag, every `If-Range` returns 200 with the full body, and the analyzer would see continuous "rotation" while really it's just normal growth. The fix is the prefix CRC identity scheme; G2 catches any regression to whole-file ETag matching.

**G3 is the HTTP analogue of B8.** Same expectation, same justification — saved offset is for identity check only, not seek.

**G4 catches `mark_dirty` short-circuits.** If `source_exists()` returns false for HTTP whenever `pending_rotation` is transiently set (e.g., during a 416 at EOF before G6 was fixed), saves get skipped silently. The fix is to make HTTP `source_exists()` always return true — annotation persistence does not depend on whether the remote endpoint is reachable right this instant.

### H. Drop verification (ground-truth)

The fixture is ground truth. The analyzer's session export (`x`, default path) writes `model.events` to disk as a JSON array — counting that array's length gives an exact ingest count, with order and content checkable for free. Same recipe for file and HTTP backends.

Pre-build a deterministic fixture so each event is self-identifying:

```bash
mkdir -p /tmp/drops
python3 -c "
import json, time
n = 5000
base = int(time.time()*1000)
with open('/tmp/drops/stream.jsonl', 'w') as f:
    for i in range(n):
        f.write(json.dumps({'_timestamp': base + i, '_type': 'e', 'idx': i}) + '\n')
"
EXPECTED=$(wc -l < /tmp/drops/stream.jsonl | tr -d ' ')
```

Helper:

```bash
# usage: run_export <session_name> <stream_arg>
#   stream_arg is either a local path or http://... URL
run_export() {
    local sess="$1" src="$2"
    local SK=.claude/skills/drive-tui
    rm -f ~/.local/state/json-analyzer/*.swap.json
    tmux new-session -d -s "$sess" -x 160 -y 50 \
        "target/release/json_analyzer $src 2>/tmp/${sess}-stderr.log"
    sleep 2   # ingest settle
    $SK/send_keys.sh "$sess" x Enter
    sleep 0.5
    $SK/send_keys.sh "$sess" q q
    sleep 1
    tmux kill-session -t "$sess" 2>/dev/null
    # Default export path is "<stream>.session.json" for file backend.
    # For HTTP backend it's "<url>.session.json" — sanitise to a local
    # filename in the working dir before passing to jq.
}
```

| ID | What | How | Pass criterion |
|---|---|---|---|
| H1 | **File backend, no drops** | `run_export drops_file /tmp/drops/stream.jsonl`, then `jq '.events | length' /tmp/drops/stream.jsonl.session.json` | Count == `$EXPECTED` (5000) |
| H2 | File backend, strict ordering | `jq '[.events[].obj.idx] == [range(0; (.events|length))]' ...session.json` | Returns `true` — every event's `idx` matches its position (no out-of-order ingest, no skipped indices) |
| H3 | File backend, timestamp monotonicity | `jq '[.events[].ts] | (. == sort)' ...session.json` | Returns `true` |
| H4 | **HTTP backend, no drops** | Start a server (Python or Rust per Section G) against `/tmp/drops/`, run `run_export drops_http http://127.0.0.1:8080/stream.jsonl`, jq the count. **Static fixture — do not use http_stream.py's bundled writer, we need a fixed count.** | Count == `$EXPECTED` |
| H5 | HTTP backend, ordering + monotonicity | Same `jq` checks as H2, H3 against the HTTP-derived export | Both return `true` |
| H6 | No double-ingest on HTTP poll boundary | Spawn a live writer at 100 ev/s for 30s alongside the HTTP server. Run `run_export drops_http_live ...`. Compare jq count to `wc -l` of the file at quit time. | `events_count == wc -l` exactly. A count *greater* than line count means the partial-tail re-fetch glued lines twice — regression on the `HttpStreamReader` partial-line fix |
| H7 | Drops on rotation are accounted for | Start session, ingest ~1000 events, truncate the file (`> /tmp/drops/stream.jsonl`), append 500 fresh events, accept the "stream changed" prompt with default (discard). Export. | `jq '.events | length'` matches the 500 post-rotation events, not 1500 (no carry-over double-count) |
| H8 | **File / HTTP parity (small)** | With `/tmp/drops/stream.jsonl` (5000 events) static, run `run_export drops_file /tmp/drops/stream.jsonl` then `run_export drops_http http://127.0.0.1:8080/stream.jsonl`. Compare with `jq -S '[.events[] | .obj]' file.session.json > /tmp/drops/file.idx.json` and the same for http; `diff`. | The two `obj`-only projections are **byte-identical**. Both backends must ingest the same bytes into the same in-memory model. Any divergence — fewer events on one side, different ordering, content drift — is a backend-asymmetry bug |
| H9 | **File / HTTP parity (at scale)** | Build a 100_000-event fixture (`n=100000` in the H1 generator), repeat H8 against it. Use the helper below to also report `min/max/missing idx` per side before diffing | Same parity: both exports have exactly 100k events, idx range 0..99999, no gaps. **This is the canary for the `max_lines` offset-skip bug.** With the bug, the HTTP side would tap out at ~180k–200k worth less than the file side (events skipped at every `MAX_LINES_PER_POLL` cap inside a single 16MB response body) |

```bash
# Helper for H8/H9 — surface where parity breaks
diff_parity() {
    local f="$1" h="$2"
    local f_n=$(jq '.events | length' "$f")
    local h_n=$(jq '.events | length' "$h")
    echo "file: $f_n events    http: $h_n events"
    if [ "$f_n" != "$h_n" ]; then
        echo "  count mismatch — see missing idx:"
        jq -r '.events[].obj.idx' "$f" | sort -n > /tmp/.file.idx
        jq -r '.events[].obj.idx' "$h" | sort -n > /tmp/.http.idx
        diff /tmp/.file.idx /tmp/.http.idx | head -20
        return 1
    fi
    jq -S '[.events[] | .obj]' "$f" > /tmp/.file.obj
    jq -S '[.events[] | .obj]' "$h" > /tmp/.http.obj
    if ! diff -q /tmp/.file.obj /tmp/.http.obj > /dev/null; then
        echo "  same count but content differs:"
        diff /tmp/.file.obj /tmp/.http.obj | head -20
        return 1
    fi
    echo "  parity OK"
}
```

**H6 is the canary for the HTTP partial-line bug.** With the original `partial_tail` prepend-and-re-fetch implementation, this regression would produce duplicated or glued lines — `events_count > wc -l` or parse errors mid-stream. The fix is "commit only complete lines from the response body; the next request re-fetches the partial."

**H9 is the canary for the `max_lines` offset-skip bug.** The HTTP `poll_http_chunk` once advanced `self.offset` by the body's last-newline position instead of by `split_and_parse_chunk`'s returned `consumed`. When `MAX_LINES_PER_POLL` (20k) capped parsing mid-body, every line between "last line we parsed" and "last newline in body" was silently skipped. Symptom: HTTP count plateaus far below the file count on the same fixture; file/HTTP exports diverge. The fix is "advance offset by what we *parsed*, not by what we *received*."

**Diagnosing a failed count** — the `idx` field on every event lets you pinpoint exactly which one was dropped:

```bash
# Find missing / duplicated idx values
jq -r '.events[].obj.idx' /tmp/drops/stream.jsonl.session.json | sort -n > /tmp/drops/got.txt
seq 0 4999 > /tmp/drops/expected.txt
diff /tmp/drops/expected.txt /tmp/drops/got.txt | head -20
# A "<" line is a missed event (drop); a ">" line is a phantom (double-ingest).
```

## Stats parity

Verifies F7. A perf-only commit (e.g. caching, vectorisation, parallelism) should not change *what* the analyzer reports — only how fast. To prove that, capture the user-visible stats (Types tab + path-focus tab) from HEAD and from the older commit on the **same** fixture, and diff the captures.

```bash
# Build the older worktree
PRE=60be13e   # or whichever baseline you're validating against
WT=/tmp/regtest/wt-stats
git worktree add "$WT" "$PRE" && ( cd "$WT" && cargo build --release ) || exit 1

# Deterministic fixture — fixed timestamps, 4 shapes, 5000 events
python3 -c "
import json
shapes = [
    lambda i: {'_timestamp': 1700000000000 + i*100, '_type': 'login',    'user': f'u{i%5}'},
    lambda i: {'_timestamp': 1700000000000 + i*100, '_type': 'logout',   'session': f's{i%5}'},
    lambda i: {'_timestamp': 1700000000000 + i*100, '_type': 'purchase', 'amount': i*10},
    lambda i: {'_timestamp': 1700000000000 + i*100, '_type': 'view',     'page': f'p{i%3}'},
]
with open('/tmp/regtest/stats.jsonl', 'w') as f:
    for i in range(5000):
        f.write(json.dumps(shapes[i%4](i)) + '\n')
"

run_stats_capture() {
    local label="$1" bin="$2"
    local sess="stats-${label}"
    python3 -c "
import hashlib, os
sd = os.path.expanduser('~/.local/state/json-analyzer')
abs_path = os.path.realpath('/tmp/regtest/stats.jsonl')
h = hashlib.sha256(abs_path.encode()).hexdigest()
for ext in ('state.json', 'swap.json'):
    f = os.path.join(sd, f'{h}.{ext}')
    if os.path.exists(f): os.remove(f)
"
    tmux kill-session -t "$sess" 2>/dev/null
    tmux new-session -d -s "$sess" -x 160 -y 50 "$bin /tmp/regtest/stats.jsonl 2>/dev/null"
    sleep 2
    tmux send-keys -t "$sess" 3                                    # Types tab
    sleep 0.5
    tmux capture-pane -t "$sess" -p > /tmp/regtest/stats-${label}-types.txt
    tmux send-keys -t "$sess" Enter                                # path-focus on first type
    sleep 0.5
    tmux capture-pane -t "$sess" -p > /tmp/regtest/stats-${label}-details.txt
    tmux send-keys -t "$sess" q q
    for _ in {1..40}; do tmux has-session -t "$sess" 2>/dev/null || break; sleep 0.1; done
}

run_stats_capture head "$(pwd)/target/release/json_analyzer"
run_stats_capture old  "$WT/target/release/json_analyzer"
diff /tmp/regtest/stats-head-types.txt   /tmp/regtest/stats-old-types.txt   || echo "FAIL F7 types diverged"
diff /tmp/regtest/stats-head-details.txt /tmp/regtest/stats-old-details.txt || echo "FAIL F7 details diverged"

git worktree remove --force "$WT"
```

Empty diff output = stats are byte-identical. Any line of `<` / `>` divergence is a regression worth investigating. Captures use raw `tmux capture-pane` text (not the structured JSON capture from drive-tui) because we want deterministic byte-level comparison of the actual on-screen output.

## Performance comparison vs older commit

When a perf bar like F4 / F6 trips, the question is "is this commit *introducing* slowness, or has it always been like this?". Use a git worktree to time the current HEAD against its parent (or any earlier baseline). The malformed-final-line fixture from F4 is ideal — a deterministic clean-exit perf test that exercises ingest, scoring, path stats, and shutdown tail-scan in one shot.

```bash
# Build the parent commit in a separate worktree (no pollution of HEAD's target/)
PARENT=$(git rev-parse HEAD~1)
WT=/tmp/regtest/wt-parent
git worktree add "$WT" "$PARENT"
( cd "$WT" && cargo build --release ) || { echo "parent build failed"; exit 1; }

# Generate fixture once (same path for both runs)
python3 -c "
import json
with open('/tmp/regtest/perf.jsonl', 'w') as f:
    for i in range(500_000):
        f.write(json.dumps({'_timestamp': 1700000000000 + i, '_type': 'e',
                            'idx': i, 'seq': f'seq-{i}', 'metric': i * 1.0001}) + '\n')
    f.write('{ \"incomplete\": \"' + ('x' * 17000) + '\"')   # large unterminated tail
"

# Time each binary. The drive-tui scripts make this reliable: launch in tmux,
# poll for ingest completion, send 'q q', measure wall-clock.
time_binary() {
    local label="$1" bin="$2"
    local sess="perf-${label}"
    rm -f ~/.local/state/json-analyzer/*.swap.json
    tmux new-session -d -s "$sess" -x 160 -y 50 \
        "$bin /tmp/regtest/perf.jsonl 2>/tmp/tui-stderr-${label}.log"
    # Wait for ingest to finish — header text 'objects 499999' or 'objects 500000'
    for _ in {1..240}; do
        tmux capture-pane -p -t "$sess" 2>/dev/null | \
            grep -q 'objects 49999[0-9]\|objects 500000' && break
        sleep 0.25
    done
    local t0=$(date +%s%N)
    tmux send-keys -t "$sess" q q
    for _ in {1..40}; do tmux has-session -t "$sess" 2>/dev/null || break; sleep 0.1; done
    local t1=$(date +%s%N)
    echo "$label: $(( (t1 - t0) / 1000000 )) ms (post-ingest q→exit)"
}

time_binary parent "$WT/target/release/json_analyzer"
time_binary head   "$(pwd)/target/release/json_analyzer"

git worktree remove --force "$WT"
```

Read the warning at shutdown out of `/tmp/tui-stderr-{parent,head}.log` — both should report `incomplete JSON line remained at shutdown`. A HEAD that is >10% slower than parent on this fixture is a regression; bisect from there.

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
- `http` — section G only (HTTP source)
- `drops` — section H only (ground-truth ingest count verification)
- empty — full sweep A → H
