# Architecture (Rust)

## Core Direction

- UI: `ratatui` + `crossterm`
- Typing: structural hash only (normalized JSON shape -> short SHA-256)
- Analytics: only
  - rate anomaly
  - value uniqueness anomaly

## Modules

- `src/domain.rs`
  - shape extraction + structural hash
  - event ingestion and in-memory storage
  - per-type path/value stats
  - uniqueness-path toggles (normalization controls)
  - anomaly scoring
  - action periods
  - known-unrelated suppression
  - filtering for data explorer
- `src/io.rs`
  - incremental JSONL tail reader (offset-based)
- `src/tui.rs`
  - Live / Periods / Type Explorer / Data Explorer rendering
  - object inspector overlay
  - highlighting strategy and discoverable key hints
- `src/app.rs`
  - input handling, mode switching, filter editing, orchestration loop

## UI Modes

1. Live
- recent events with anomaly highlighting
- action state

2. Periods
- all closed action periods in one list
- period-by-period event browsing with active filters

3. Type Explorer
- discovered types
- uniqueness paths considered per type
- per-path on/off controls
- type list filtering and type rename
- mark whole type as known unrelated

4. Data Explorer
- query collected data by keys, type id, fuzzy text, exact path=value

## Object Inspector

- Open from any event list.
- Shows selected event plus extracted key paths.
- Key-driven navigation:
  - apply selected key as filter and jump to Data view
  - jump to the event's type in Type Explorer

## Performance Notes

- bounded event buffer (`VecDeque`, capped)
- incremental stream tailing
- anomaly windows updated in O(types active in window)
