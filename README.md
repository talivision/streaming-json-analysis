# JSON Analyzer (Rust + Ratatui)

This app is now a Rust terminal UI focused on fast, practical anomaly discovery.

## What It Does

- Structural typing only: type IDs are SHA-256 hashes of normalized JSON shape.
- Basic analytics only:
  - rate anomaly
  - value uniqueness anomaly
- Type Explorer:
  - shows uniqueness paths considered per type
  - lets you force paths on/off to normalize noisy fields
- Data Explorer:
  - explore collected data with filters on keys, type ID, fuzzy text, exact key=value
- Known unrelated controls:
  - suppress entire types

Removed by design: palette controls, long-tail analysis, semantic/fuzzy typing.

## Install Rust

macOS/Linux (rustup):

```bash
curl https://sh.rustup.rs -sSf | sh
source "$HOME/.cargo/env"
```

## Build

```bash
cargo build --release
```

## Run

By default it tails `/tmp/json_demo/stream.jsonl`:

```bash
cargo run --release
```

Or pass a stream path:

```bash
cargo run --release -- /path/to/stream.jsonl
```

Optionally preload a known-clean baseline corpus:

```bash
cargo run --release -- /path/to/stream.jsonl --baseline /path/to/clean-baseline.jsonl
```

Live mode requires each JSON object to include root `_timestamp` as epoch milliseconds
(13-digit integer, e.g. `1739952000123`).
If missing, the analyzer exits fast with an unsupported-input error.

For offline analysis of files without `_timestamp`:

```bash
cargo run --release -- --offline /path/to/stream.jsonl
```

## Demo Source

Run the built-in Python demo source (no separate trigger script needed):

```bash
python3 demo_source.py
```

In the source terminal, use single-key triggers:

- `l` login
- `p` purchase
- `s` search
- `c` experiment_control
- `t` experiment_treatment
- `h` source_like_heartbeat
- `m` source_like_metric
- `?` help
- `q` quit

## Keybindings

- `q`: quit
- `1/2/3/4`: switch modes (`Live`, `Periods`, `Types`, `Baseline`)
- `m`: start/stop action period
- `n`: set current action label

Event-list modes (`Periods`, `Baseline`):

- `enter`: open object inspector
- `g`: toggle rate boundary display (`point` / `interval`)
- `k`: edit key filter
- `t`: edit type filter
- `/`: edit fuzzy filter
- `e`: edit exact filter (`path=value`)
- `c`: clear event filters
- `y`: toggle event filters off/on (restore previous set)

Live mode:

- `up/down`: select event row
- `right` or `enter`: focus inline key picker in the selected object pane
- `esc` or `left`: return focus to event rows (resumes follow if it was on)
- while key-focused: `up/down` select key, `right` focus value, `enter` or `k` toggle key filter, `t` jump to type
- while value-focused: `enter` or `e` toggle exact `path=value` filter in place, `left` back to key focus
- follow mode is paused while key-focused and resumes when key focus exits

Periods mode:

- `up/down`: move between action periods
- `enter` or `right`: focus events for the selected period
- while event-focused: `up/down` select event, `enter` open object inspector, `left` return to period list

Types mode:

- `up/down`: select type
- `t`: apply selected type as event filter and jump to Live mode
- after `t`, `esc` in Live returns to Types
- `enter` or `right`: focus uniqueness paths for selected type
- `left`: return from path focus to type list
- while path-focused: `up/down` select path, `space` toggle include/exclude
- `u`: mark selected type as known unrelated
- `/`: filter type list
- `r`: rename selected type

Object inspector:

- `up/down`: select key
- `k`: apply selected key as event filter in the current mode
- `t`: jump to the selected event's type in Types mode
- `esc`: close inspector

Type names are shown in all event lists as either:
- `type-<hash8>` (default)
- `<custom name> (type-<hash8>)` (renamed)
