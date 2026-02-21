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
  - suppress specific correlation pairs
- Correlations auto-refresh on a throttled cadence with smoothing.

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
- `1/2/3/4`: switch modes (`Live`, `Periods`, `Types`, `Data`)
- `m`: start/stop action period
- `n`: set current action label

All event-list modes (`Live`, `Periods`, `Data`):

- `enter`: open object inspector
- `a`: cycle anomaly display (`snapshot` -> `snapshot+live` -> `live`)
- `k`: edit key filter
- `t`: edit type filter
- `/`: edit fuzzy filter
- `e`: edit exact filter (`path=value`)
- `c`: clear event filters
- `y`: toggle event filters off/on (restore previous set)

Live mode:

- `up/down`: select event row
- `x`: mark selected correlation as known unrelated

Periods mode:

- `up/down`: move between action periods
- `left/right`: move through events for the selected period

Types mode:

- `up/down`: select type
- `t`: apply selected type as event filter and jump to Data mode
- `left/right`: select uniqueness path
- `space`: toggle path on/off for uniqueness analysis
- `u`: mark selected type as known unrelated
- `/`: filter type list
- `r`: rename selected type

Object inspector:

- `up/down`: select key
- `k`: apply selected key as event filter and jump to Data mode
- `t`: jump to the selected event's type in Types mode
- `esc`: close inspector

Type names are shown in all event lists and correlations as either:
- `type-<hash8>` (default)
- `<custom name> (type-<hash8>)` (renamed)
