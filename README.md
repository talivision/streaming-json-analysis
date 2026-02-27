# JSON Analyzer

A terminal UI for investigating JSONL streams. Connect it to a stream of telemetry or application events, mark moments of interest as action periods, and it surfaces which event types and field values stand out against the surrounding baseline.

## Install and build

```bash
# macOS/Linux
curl https://sh.rustup.rs -sSf | sh
source "$HOME/.cargo/env"
cargo build --release
```

### Fully static Linux build (MUSL)

MUSL builds use mimalloc to work around the slow musl allocator. This requires a musl-capable C compiler for the mimalloc build script.

On Debian/Ubuntu:

```bash
sudo apt-get install musl-tools
rustup target add x86_64-unknown-linux-musl
CC_x86_64_unknown_linux_musl=musl-gcc cargo build --release --target x86_64-unknown-linux-musl
```

On macOS (via `cargo-zigbuild`, which handles the C cross-compilation automatically):

```bash
cargo zigbuild --release --target x86_64-unknown-linux-musl
```

Optional ARM64 target:

```bash
rustup target add aarch64-unknown-linux-musl
cargo build --release --target aarch64-unknown-linux-musl
```

The output static binary will be written to `target/x86_64-unknown-linux-musl/release/json_analyzer`. This should be run-anywhere on all Linux boxes.

**Known limitation:** MUSL's `memcpy` is slower than glibc's. Negligible for interactive use; may matter at very high ingest throughput.

### Standard Linux build (dynamically linked)

If you are building for your own machine or a system with a compatible glibc:

```bash
cargo build --release
```

This is noticeably faster to compile than the MUSL build, but the binary will not run on older Linux distributions.

### Linux build for old glibc (portable without MUSL)

To target systems with glibc ≥ 2.17 (e.g. CentOS 7, RHEL 7) without requiring a fully static binary, use [`cargo-zigbuild`](https://github.com/messense/cargo-zigbuild):

```bash
pip install ziglang  # or install zig from https://ziglang.org
cargo install cargo-zigbuild
rustup target add x86_64-unknown-linux-gnu
cargo zigbuild --release --target x86_64-unknown-linux-gnu.2.17
```

### Windows build

Native on Windows (MSVC toolchain):

```bash
rustup target add x86_64-pc-windows-msvc
cargo build --release --target x86_64-pc-windows-msvc
```

Cross-compile to Windows GNU from macOS/Linux:

```bash
brew install mingw-w64
rustup target add x86_64-pc-windows-gnu
cargo build --release --target x86_64-pc-windows-gnu
```

---

## Getting started

Point the tool at any JSONL file and it starts reading immediately:

```bash
./target/release/json-analyzer stream.jsonl
```

For static archives where you want to read the whole file at once rather than tail it:

```bash
./target/release/json-analyzer --offline archive.jsonl
```

---

## Writing a source

If you control the event producer, the format is simple — one JSON object per line, flushed immediately. The only required field is **`_timestamp`**: epoch milliseconds as a 13-digit integer.

```python
import json, time

def write_event(f, obj):
    f.write(json.dumps(obj) + "\n")
    f.flush()

with open("/tmp/stream.jsonl", "a") as f:
    seq = 0
    while True:
        write_event(f, {"_timestamp": int(time.time() * 1000), "event": "heartbeat", "seq": seq, "_service": "auth", "_env": "prod"})
        seq += 1
        time.sleep(1)
```

Rules:
- One JSON object per line, no pretty-printing.
- `_timestamp` must be an integer, not a string, and not seconds — 13 digits (milliseconds).
- `_timestamp` must be monotonically non-decreasing across lines — each event must have a timestamp equal to or greater than the one before it.

### Enrichment fields

Any `_`-prefixed field beyond `_timestamp` is treated as enrichment or deployment context and behaves like any other field:

```json
{
  "_timestamp": 1739952000123,
  "_env":       "prod",
  "_service":   "auth",
  "_region":    "us-east-1",
  "event":      "login",
  "user_id":    42
}
```

- Two events that differ only in the *value* of `_env` have the same structural type. Two that differ in whether `_env` is *present* have different types.
- Field values are tracked for uniqueness scoring. If `_env` is almost always `"prod"` in the baseline and a period produces `_env: "staging"`, the anomaly score rises.

Tagging events with `_service`, `_region`, or `_datacenter` gives the anomaly engine more signal without polluting your application fields.

---

## Investigation workflow

### 1. Watch the stream

The default Live view shows incoming events in reverse-chronological order. Each row shows the structural type, event size, and time offset from the first event in view. As types accumulate, the **Types view (`3`)** becomes useful for orienting yourself: what kinds of events exist, how frequently they arrive, and what their fields look like. Press `j` on a type to see a sample event.

Rename unfamiliar types with `r` to give them human-readable labels — these persist across sessions.

### 2. Mark an action period

When something of interest happens — a deployment, a user action, an incident — press **`m`** to open an action period. Press **`m`** again to close it. Label it with **`n`** before closing if you want a name on the period.

Events inside the period are scored against the baseline:
- **Rate anomaly** — is this event type arriving faster or slower than normal?
- **Value uniqueness** — are the field values rare compared to what the baseline has seen?

#### Optional: HTTP control for reproducible marking

The keyboard flow (`m`) is the default and works well in normal interactive use. If you need reproducible, script-driven period boundaries (especially in high-volume environments), you can enable an optional local HTTP control API:

```bash
./target/release/json-analyzer stream.jsonl --control-http 127.0.0.1:8080
```

Endpoints:
- `POST /action/start` (optional JSON body: `{"label":"deploy"}`)
- `POST /action/stop`
- `GET /action/status`

`start`/`stop` are idempotent: repeated calls keep state stable instead of creating duplicate transitions.

### 3. Review the period

Switch to the **Periods view (`2`)** to see closed periods. Select one to browse its events colour-coded by anomaly score. The right pane shows the selected event's JSON with high-scoring paths highlighted.

### 4. Tune the signal

The Types view lets you push down noise from types that aren't relevant to your investigation. Press **`u`** on a type to add it to a negative type filter — it stays visible in the Types view but disappears from event lists. Press **`u`** again to remove it.

Within a type, press `enter` to see the field paths the engine considers for uniqueness scoring. High-cardinality paths (IDs, free text) are auto-excluded by the engine; use `space` to force paths on or off if the heuristic gets it wrong.

Use the filter keys (`k`, `t`, `/`, `z`, `e`) to narrow event lists to what you care about. Filters support `&&`, `||`, and `!` negation, and quoted terms:

```
type:   payment && !healthcheck
exact:  status=error && !env=staging
sub:    "timeout" && !"connection reset"
```

### 5. Whitelist known-related artefacts

If you know certain event types or values will always be related to the activity you're investigating, load a whitelist so they're never accidentally filtered out. A whitelist is a text file with one search term per line:

```bash
./target/release/json-analyzer stream.jsonl --whitelist terms.txt
```

Events matching any term are treated as always-interesting regardless of active filters. Cycle modes with **`w`**:

- **`always-show`** — whitelisted events appear even when filtered out
- **`only-whitelist`** — only whitelisted events are shown
- **`off`** — whitelist loaded but inactive

Matches are highlighted in orange in the JSON preview.

### 6. Save your work

Press **`p`** to export a profile — your configuration (type renames, excluded types, path overrides, whitelist terms) without the events. Reload it next time you open the same stream, or apply it to a different stream of the same kind:

```bash
./target/release/json-analyzer stream.jsonl --profile stream.profile.json
```

Press **`x`** to export a full session snapshot including all events, baseline, and periods. A colleague can open it directly without access to the original stream:

```bash
./target/release/json-analyzer --import session.json
```

Once you've used `x` in a session, the snapshot is re-written automatically on clean exit. If session state conflicts with a loaded profile, you'll be prompted to choose.

---

## How the baseline works

The tool continuously builds a picture of "normal" from events that arrive outside of any action period. When you mark an action period (press `m`), everything the tool has seen up to that point — and everything after the period closes — forms the implicit baseline that the anomaly engine scores against.

This means you don't need to do anything special to establish a baseline: run the tool, let it observe normal operation for a while, then start marking periods.

If you already have a large corpus of known-good events, you can pre-load it to give the engine a better reference from the start:

```bash
./target/release/json-analyzer stream.jsonl --baseline baseline.jsonl
```

This is optional but improves scoring quality, especially for rate anomalies, when the stream is young and the implicit baseline is thin.

---

## Demo

The bundled Python demo source writes background noise plus action-triggered events:

```bash
# Terminal 1
python3 examples/sources/demo-source/demo_source.py

# Terminal 2
./target/release/json-analyzer /tmp/json_demo/stream.jsonl
```

In the source terminal, press `l` (login), `p` (purchase), `s` (search), `c`/`t` (experiment variants) to fire actions. In the analyzer, press `m` to bracket a window around them and watch the anomaly scores appear.

---

## CLI reference

```
json_analyzer [<path>] [--jsonl <path>] [--directory <path>] [--baseline <path>]
              [--import <path>] [--profile <path>] [--whitelist <path>]
              [--offline] [--reset] [--debug-status] [--control-http <addr>]

  <path> / --jsonl    path to input JSONL file (live, tailed)
  --directory         path to a directory of JSON/JSONL files (offline only)
  --baseline          pre-load known-good events from a file or directory
  --import            open a previously exported session snapshot
  --profile           apply a source profile on startup
  --whitelist         load whitelist terms from a file (one per line)
  --offline           read file once without tailing; _timestamp not required
  --reset             start without loading persisted session state from disk
  --debug-status      show internal status line details continuously
  --control-http      optional control API bind address (e.g. 127.0.0.1:8080)
```

If your events are spread across many files, use `--directory`. Files are read in parallel and sorted by `_timestamp` before ingestion. Directory mode is offline-only — it reads the files once and does not tail for new additions. It is slower and less reliable than a single JSONL file, as ordering depends entirely on `_timestamp` being present and correct. Where you have control over the source, prefer writing to a single append-only JSONL file with monotonically increasing `_timestamp` values.

---

## Keybindings

### Global

| Key | Action |
|-----|--------|
| `q` (×2) | Quit |
| `h` / `?` | Toggle help overlay |
| `1` `2` `3` `4` | Live / Periods / Types / Baseline |
| `m` | Open / close action period |
| `n` | Set action label |
| `x` | Export session |
| `p` | Export profile |

### Filters

| Key | Filter |
|-----|--------|
| `k` | Keys |
| `t` | Type |
| `/` | Substring |
| `z` | Fuzzy |
| `e` | Exact `path=value` |
| `c` | Clear all |
| `y` | Suspend / restore |
| `w` | Cycle whitelist mode |

### Live view

| Key | Action |
|-----|--------|
| `↑` / `↓` | Select event |
| `f` | Toggle follow |
| `→` / `enter` | Focus key picker |
| `esc` / `←` | Back to event list |
| (key-focused) `k` | Set key filter |
| (key-focused) `t` | Jump to type |
| (key-focused) `v` | Browse all unique values for this key |
| (value-focused) `e` | Set exact filter |

### Values view

| Key | Action |
|-----|--------|
| `↑` / `↓` | Select value |
| `enter` / `e` | Apply as exact filter and return to Live |
| `esc` | Return to Live without filtering |

### Types view

| Key | Action |
|-----|--------|
| `↑` / `↓` | Select type |
| `r` | Rename |
| `j` | Preview sample event |
| `t` | Filter to type and jump to Live |
| `u` | Toggle negative type filter |
| `/` | Search type list |
| `enter` / `→` | Focus path list |
| (path-focused) `space` | Toggle path on / off |

### Periods view

| Key | Action |
|-----|--------|
| `↑` / `↓` | Select period |
| `enter` / `→` | Browse period events |
| `del` | Delete period |

### Object inspector

| Key | Action |
|-----|--------|
| `↑` / `↓` | Select key |
| `k` | Set key filter |
| `t` | Jump to type |
| `esc` | Close |

---

## Anomaly scoring

See [stats.md](stats.md) for full details. Brief summary:

- **Rate anomaly** — compares event frequency during the action window against the baseline rate for that type. Falls back to inter-arrival time when the period is short or sparsely sampled.
- **Value uniqueness** — scores each scalar field path: how rare is this value compared to baseline?
- Both scores are in [0, 1]. Display uses `sqrt(score)` to keep mid-range anomalies visible.
