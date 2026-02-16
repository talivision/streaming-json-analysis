"""
demo_analyzer.py — Interactive Stream Analyzer (Textual TUI)

This is the analyst's tool. It watches a stream file written by the
data source, fingerprints objects, and helps discover correlations
between the analyst's actions and objects in the stream.

The analyst's workflow:
  1. Start the source (demo_source.py) — it writes to /tmp/json_demo/
  2. Start the analyzer — baseline recording begins automatically
  3. Wait a bit for the baseline to build (observe "normal" traffic)
  4. Press 'b' to lock in the baseline
  5. Perform an action on the system (e.g., run trigger.py login)
  6. Press 'm' to mark the moment — timestamp is captured INSTANTLY
  7. Press 'l' to set/change the label for marks (applies to future marks)
  8. Repeat steps 5-6 several times for the same action
  9. Press 'c' to compute correlations and see confidence scores
  10. Use the scores — plus your own judgment — to decide what's causal

Key UX decisions:
  - 'm' marks INSTANTLY — no dialog, no typing delay. The timestamp
    is the moment you press the key. Labels are set separately with 'l'.
  - Baseline records automatically from startup. You just press 'b'
    when you're ready to start acting.
  - The analyzer ADVISES, it doesn't decide. Confidence scores help
    focus attention, but the analyst makes the final call.

Usage:
  python demo_analyzer.py                              # default path
  python demo_analyzer.py --path /tmp/json_demo        # custom path

Requires: pip install textual
"""

import asyncio
import json
import math
import os
import time
import argparse
from typing import Any, Optional, Callable
from collections import defaultdict

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Header, Footer, RichLog, Static, Input
from textual import on, work

from rich.text import Text

from engine import TypeRegistry, BaselineModel, CorrelationEngine


# ============================================================================
# TUI APPLICATION
# ============================================================================

# Default path where the source writes its stream
DEFAULT_STREAM_DIR = "/tmp/json_demo"


def strength_band(confidence: float) -> tuple[str, str]:
    """
    Map numeric confidence to a concise label + Rich style.
    """
    if confidence >= 0.8:
        return "STRONG", "bold green"
    if confidence >= 0.5:
        return "MODERATE", "bold cyan"
    if confidence >= 0.2:
        return "WEAK", "bold yellow"
    return "NOISE", "bold red"


class InspectionModal(ModalScreen):
    """
    A modal screen to inspect detailed correlation results.
    Overlays the main stream view so results don't get lost in the scroll.
    """
    
    BINDINGS = [
        Binding("escape", "dismiss", "Close"),
        Binding("i", "dismiss", "Close"),
        Binding("enter", "open_raw_view", "Open Raw", priority=True),
        Binding("b", "back_to_list", "Back", priority=True),
    ]

    CSS = """
    InspectionModal {
        align: center middle;
    }
    .modal-container {
        width: 85%;
        height: 85%;
        border: solid $accent;
        background: $surface;
        padding: 1 2;
    }
    .header {
        width: 100%;
        text-align: center;
        background: $accent;
        color: $text;
        text-style: bold;
        padding: 1;
        margin-bottom: 1;
    }
    """

    def __init__(
        self,
        correlation: CorrelationEngine,
        registry: TypeRegistry,
        confidence_cutoff: float,
    ):
        super().__init__()
        self.correlation = correlation
        self.registry = registry
        self.confidence_cutoff = confidence_cutoff

    def compose(self) -> ComposeResult:
        with Vertical(classes="modal-container"):
            yield Static(
                "INSPECTION",
                classes="header",
                id="inspect-header",
            )
            yield RichLog(id="report", markup=True, highlight=True)

    def on_mount(self):
        self._mode = "select"  # "select" or "raw"
        self._selected_index = 0
        self._candidates: list[dict] = []
        report = self.query_one("#report", RichLog)
        report.can_focus = False
        self._build_candidates()
        self.render_select_view()

    def on_key(self, event):
        # Selection mode: arrows choose candidate only (no scrolling).
        if self._mode == "select":
            if event.key == "up":
                event.stop()
                self.action_cursor_up()
            elif event.key == "down":
                event.stop()
                self.action_cursor_down()

    def _build_candidates(self):
        self._candidates = []
        for label in self.correlation.action_labels():
            closed = self.correlation.closed_periods(label)
            period_order = {p.id: i + 1 for i, p in enumerate(closed)}
            for result in self.correlation.correlations(label):
                if result["confidence"] <= self.confidence_cutoff:
                    continue
                rows, _ = self.correlation.raw_observations(
                    action_label=label,
                    type_id=result["type_id"],
                    limit=0,
                )
                linked_period_ids = sorted({int(r["period_id"]) for r in rows})
                has_repeats = len(closed) > 1
                linked_marks: list[str] = []
                for pid in linked_period_ids:
                    idx = period_order.get(pid)
                    if idx is None:
                        continue
                    if has_repeats:
                        linked_marks.append(f"{label} ({self._ordinal(idx)})")
                    else:
                        linked_marks.append(label)
                self._candidates.append(
                    {
                        "label": label,
                        "result": result,
                        "linked_marks": linked_marks,
                    }
                )
        self._candidates.sort(key=lambda c: c["result"]["confidence"], reverse=True)

    def _selected_candidate(self) -> Optional[dict]:
        if not self._candidates:
            return None
        self._selected_index = max(0, min(self._selected_index, len(self._candidates) - 1))
        return self._candidates[self._selected_index]

    @staticmethod
    def _ordinal(n: int) -> str:
        if 10 <= (n % 100) <= 20:
            suffix = "th"
        else:
            suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
        return f"{n}{suffix}"

    def render_select_view(self):
        header = self.query_one("#inspect-header", Static)
        header.update("INSPECTION: Select with ↑/↓, Enter opens raw objects, Esc closes")
        log = self.query_one("#report", RichLog)
        log.can_focus = False
        log.clear()

        if not self._candidates:
            log.write("No actions marked yet.")
            return

        log.write(
            Text(
                f"Candidates: {len(self._candidates)} "
                f"(confidence > {self.confidence_cutoff:.2f})\n",
                style="bold",
            )
        )
        legend = Text("Legend: ")
        legend.append("[STRONG]", style="bold green")
        legend.append(" ")
        legend.append("[MODERATE]", style="bold cyan")
        legend.append(" ")
        legend.append("[WEAK]", style="bold yellow")
        legend.append(" ")
        legend.append("[NOISE]\n", style="bold red")
        log.write(legend)

        for i, candidate in enumerate(self._candidates):
            label = candidate["label"]
            result = candidate["result"]
            obj_type = self.registry.get(result["type_id"])
            type_name = obj_type.display_name if obj_type else result["type_id"][:8]
            strength, strength_style = strength_band(result["confidence"])
            prefix = "► " if i == self._selected_index else "  "
            line = Text(prefix)
            line.append(f"[{strength}]", style=strength_style)
            linked_marks = candidate.get("linked_marks") or []
            linked_display = ", ".join(linked_marks) if linked_marks else "-"
            line.append(
                f" [{label}] {type_name}  conf={result['confidence']:.2f}  "
                f"{result['appearances']}/{result['trials']}  ~{result['avg_latency_ms']:.0f}ms  "
                f"marks: {linked_display}"
            )
            if i == self._selected_index:
                line.stylize("bold white")
            log.write(line)

        selected = self._selected_candidate()
        if selected is None:
            return
        label = selected["label"]
        result = selected["result"]
        type_id = result["type_id"]
        obj_type = self.registry.get(type_id)
        type_name = obj_type.display_name if obj_type else type_id[:8]

        log.write(Text("\n" + "-" * 60, style="dim"))
        log.write(Text(f"Selected: [{label}] {type_name}", style="bold underline"))
        selected_marks = selected.get("linked_marks") or []
        selected_marks_display = ", ".join(selected_marks) if selected_marks else "-"
        log.write(
            Text(
                f"Confidence: {result['confidence']:.2f} ({result['assessment']})\n"
                f"Stats: {result['appearances']}/{result['trials']} trials, "
                f"latency ~{result['avg_latency_ms']:.0f}ms, "
                f"baseline {result['baseline_rate']:.2f}/sec\n"
                f"Linked marks: {selected_marks_display}"
            )
        )

        example = obj_type.example if obj_type else {}
        log.write(Text("\nExample object:", style="bold cyan"))
        formatted = json.dumps(example, indent=2)
        indented = "\n".join("  " + ln for ln in formatted.splitlines())
        log.write(Text(indented, style="cyan"))

        delayed = self.correlation.delayed_correlations(label)[:3]
        if delayed:
            log.write(Text("\nTop delayed candidates (post-window):", style="bold magenta"))
            for r in delayed:
                delayed_type = self.registry.get(r["type_id"])
                delayed_name = delayed_type.display_name if delayed_type else r["type_id"][:8]
                log.write(
                    Text(
                        f"  [{label}] {delayed_name}  conf={r['confidence']:.2f}  "
                        f"{r['appearances']}/{r['trials']}  ~{r['avg_latency_ms']:.0f}ms after end"
                    )
                )

    def action_cursor_up(self):
        if self._mode != "select" or not self._candidates:
            return
        self._selected_index = (self._selected_index - 1) % len(self._candidates)
        self.render_select_view()

    def action_cursor_down(self):
        if self._mode != "select" or not self._candidates:
            return
        self._selected_index = (self._selected_index + 1) % len(self._candidates)
        self.render_select_view()

    def action_open_raw_view(self):
        if self._mode != "select":
            return
        selected = self._selected_candidate()
        if selected is None:
            return

        self._mode = "raw"
        header = self.query_one("#inspect-header", Static)
        header.update("INSPECTION: Raw objects (scroll normally; press 'b' to go back)")
        log = self.query_one("#report", RichLog)
        log.can_focus = True
        log.clear()

        label = selected["label"]
        result = selected["result"]
        type_id = result["type_id"]
        obj_type = self.registry.get(type_id)
        type_name = obj_type.display_name if obj_type else type_id[:8]

        raw_rows, raw_total = self.correlation.raw_observations(
            action_label=label,
            type_id=type_id,
            limit=0,  # show all; user can scroll
        )

        log.write(Text(f"[{label}] {type_name}", style="bold yellow"))
        log.write(Text(f"Raw objects: {raw_total}\n", style="bold"))
        if not raw_rows:
            log.write(Text("(no raw objects captured for this candidate)"))
            return

        for row in raw_rows:
            ts = time.strftime("%H:%M:%S", time.localtime(row["timestamp"]))
            ms = int((row["timestamp"] % 1) * 1000)
            log.write(
                Text(
                    f"period #{row['period_id']} @ {ts}.{ms:03d} "
                    f"(+{row['latency_ms']:.0f}ms from start; "
                    f"+{row['phase_latency_ms']:.0f}ms in {row['phase']} phase)"
                )
            )
            formatted = json.dumps(row["obj"], indent=2)
            indented = "\n".join("  " + ln for ln in formatted.splitlines())
            log.write(Text(indented, style="cyan"))
            log.write("")

    def action_back_to_list(self):
        if self._mode != "raw":
            return
        self._mode = "select"
        self.render_select_view()


class TypesExplorerModal(ModalScreen):
    """
    Dedicated browser for discovered types and their recent raw objects.
    """

    BINDINGS = [
        Binding("escape", "dismiss", "Close"),
        Binding("t", "dismiss", "Close"),
        Binding("enter", "open_raw_view", "Open Raw", priority=True),
        Binding("b", "back_to_list", "Back", priority=True),
    ]

    CSS = """
    TypesExplorerModal {
        align: center middle;
    }
    .modal-container {
        width: 85%;
        height: 85%;
        border: solid $accent;
        background: $surface;
        padding: 1 2;
    }
    .header {
        width: 100%;
        text-align: center;
        background: $accent;
        color: $text;
        text-style: bold;
        padding: 1;
        margin-bottom: 1;
    }
    """

    def __init__(
        self,
        registry: TypeRegistry,
        fetch_recent_raw: Callable[[str], tuple[list[dict[str, Any]], int]],
    ):
        super().__init__()
        self.registry = registry
        self.fetch_recent_raw = fetch_recent_raw

    def compose(self) -> ComposeResult:
        with Vertical(classes="modal-container"):
            yield Static("TYPES EXPLORER", classes="header", id="types-header")
            yield RichLog(id="types-report", markup=True, highlight=True)

    def on_mount(self):
        self._mode = "select"  # "select" or "raw"
        self._selected_index = 0
        self._types = self.registry.all_types()
        log = self.query_one("#types-report", RichLog)
        log.can_focus = False
        self.render_select_view()

    def on_key(self, event):
        if self._mode == "select":
            if event.key == "up":
                event.stop()
                self.action_cursor_up()
            elif event.key == "down":
                event.stop()
                self.action_cursor_down()

    def _selected_type(self):
        if not self._types:
            return None
        self._selected_index = max(0, min(self._selected_index, len(self._types) - 1))
        return self._types[self._selected_index]

    def render_select_view(self):
        self._types = self.registry.all_types()
        header = self.query_one("#types-header", Static)
        header.update("TYPES EXPLORER: Select with ↑/↓, Enter opens recent raw objects")
        log = self.query_one("#types-report", RichLog)
        log.can_focus = False
        log.clear()

        if not self._types:
            log.write("No types discovered yet.")
            return

        log.write(Text(f"Discovered types: {len(self._types)}\n", style="bold"))
        for i, obj_type in enumerate(self._types):
            prefix = "►" if i == self._selected_index else " "
            sig_count = len(obj_type.semantic_signature)
            line = Text(
                f"{prefix} {obj_type.display_name}  count={obj_type.count:,}  sig={sig_count}"
            )
            if i == self._selected_index:
                line.stylize("bold yellow")
            log.write(line)

        selected = self._selected_type()
        if selected is None:
            return

        log.write(Text("\n" + "-" * 60, style="dim"))
        log.write(Text(f"Selected: {selected.display_name}", style="bold underline"))
        log.write(Text(f"Type ID: {selected.type_id}"))
        shape_keys: list[str] = []
        if isinstance(selected.shape, dict):
            shape_keys = sorted(selected.shape.keys())
        if shape_keys:
            preview = ", ".join(shape_keys[:10])
            if len(shape_keys) > 10:
                preview += f", +{len(shape_keys) - 10} more"
            log.write(Text(f"Top-level keys: {preview}"))

        log.write(Text("\nExample object:", style="bold cyan"))
        formatted = json.dumps(selected.example or {}, indent=2)
        indented = "\n".join("  " + ln for ln in formatted.splitlines())
        log.write(Text(indented, style="cyan"))

    def action_cursor_up(self):
        if self._mode != "select" or not self._types:
            return
        self._selected_index = (self._selected_index - 1) % len(self._types)
        self.render_select_view()

    def action_cursor_down(self):
        if self._mode != "select" or not self._types:
            return
        self._selected_index = (self._selected_index + 1) % len(self._types)
        self.render_select_view()

    def action_open_raw_view(self):
        if self._mode != "select":
            return
        selected = self._selected_type()
        if selected is None:
            return

        self._mode = "raw"
        header = self.query_one("#types-header", Static)
        header.update("TYPES EXPLORER: Raw objects (scroll; press 'b' to go back)")
        log = self.query_one("#types-report", RichLog)
        log.can_focus = True
        log.clear()

        rows, total_seen = self.fetch_recent_raw(selected.type_id)
        log.write(Text(f"{selected.display_name}", style="bold yellow"))
        log.write(
            Text(
                f"Recent raw objects shown: {len(rows)} / {total_seen} captured for this type\n",
                style="bold",
            )
        )

        if not rows:
            log.write(Text("(no raw objects captured yet for this type)"))
            return

        for row in rows:
            ts = time.strftime("%H:%M:%S", time.localtime(row["timestamp"]))
            ms = int((row["timestamp"] % 1) * 1000)
            log.write(Text(f"@ {ts}.{ms:03d}"))
            formatted = json.dumps(row["obj"], indent=2)
            indented = "\n".join("  " + ln for ln in formatted.splitlines())
            log.write(Text(indented, style="cyan"))
            log.write("")

    def action_back_to_list(self):
        if self._mode != "raw":
            return
        self._mode = "select"
        self.render_select_view()


class AnalyzerApp(App):
    """
    The main analyzer TUI.

    Layout:
    ┌─────────────────────────────────────┬────────────────────────┐
    │ Stream (scrolling log of objects)   │ Status                 │
    │                                     │ Types (auto-discovered)│
    │                                     │ Correlations           │
    ├─────────────────────────────────────┤                        │
    │ Label input (shown when pressing l) │                        │
    └─────────────────────────────────────┴────────────────────────┘
    """

    CSS = """
    #main-panel {
        width: 2fr;
    }
    #sidebar {
        width: 1fr;
        border-left: solid $accent;
        padding: 0 1;
    }
    #stream {
        height: 1fr;
    }
    #label-input {
        /* Hidden by default, shown when analyst presses 'l' */
        display: none;
        dock: bottom;
    }
    #status-panel {
        height: auto;
        max-height: 8;
        padding: 0 0 1 0;
        color: $text;
    }
    #types-panel {
        height: 1fr;
        padding: 0 0 1 0;
    }
    #correlations-panel {
        height: 1fr;
    }
    """

    BINDINGS = [
        Binding("m", "toggle_action", "Toggle Action", show=True),
        Binding("l", "set_label", "Label", show=True),
        Binding("c", "correlate", "Correlate", show=True),
        Binding("i", "inspect_correlations", "Inspect Results", show=True),
        Binding("t", "explore_types", "Explore Types", show=True),
        Binding("escape", "cancel_input", "Cancel", show=False),
    ]

    def __init__(
        self,
        stream_dir: str = DEFAULT_STREAM_DIR,
        similarity_threshold: float = 0.85,
        semantic_overlap_threshold: float = 0.50,
        inspect_confidence_cutoff: float = 0.20,
        post_window_sec: float = 0.0,
        replay_file: Optional[str] = None,
        replay_speed: float = 0.0,
        marks: Optional[list[dict[str, Any]]] = None,
    ):
        super().__init__()
        self.stream_dir = stream_dir
        self.stream_path = os.path.join(stream_dir, "stream.jsonl")
        self.replay_file = replay_file
        self.replay_speed = replay_speed
        self.marks = marks or []
        self.is_replay = replay_file is not None

        # Core analysis components (see engine.py for details)
        self.registry = TypeRegistry(
            similarity_threshold=similarity_threshold,
            semantic_overlap_threshold=semantic_overlap_threshold,
        )
        # Baseline auto-starts — records from the moment the analyzer launches
        self.baseline = BaselineModel()
        self.correlation = CorrelationEngine(self.baseline, post_window_sec=post_window_sec)
        self.inspect_confidence_cutoff = inspect_confidence_cutoff
        self.post_window_sec = post_window_sec

        # The current action label. Set with 'l', used by 'm'.
        self.current_label = "action"

        # UI state
        self.object_count = 0
        self._rate_counter = 0
        self._rate_timestamp = time.time()
        self.current_rate = 0.0
        self._keyset_counts: dict[tuple[str, ...], int] = defaultdict(int)
        self._shape_counts: dict[str, int] = defaultdict(int)
        # Recent raw objects per discovered type for the Types Explorer.
        self._raw_by_type: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self._raw_cap_per_type = 120
        # Per-type value profiling for "novel value within common type" coloring.
        # type_id -> path -> {"total": int, "values": {value_token: count}}
        self._type_value_stats: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)

    # --- Layout ---

    def compose(self) -> ComposeResult:
        yield Header()
        with Horizontal():
            with Vertical(id="main-panel"):
                yield RichLog(id="stream", max_lines=500, highlight=True, markup=True)
                yield Input(
                    id="label-input",
                    placeholder="New label for action periods (e.g. 'login')...",
                )
            with Vertical(id="sidebar"):
                yield Static("Waiting for stream...", id="status-panel")
                yield Static("No types discovered yet", id="types-panel")
                yield Static(
                    "No correlations yet\n\n"
                    "Workflow:\n"
                    " 1. Wait for baseline\n"
                    " 2. Press [m] to START action\n"
                    " 3. Press [m] to END action\n"
                    " 4. Repeat (updates live)\n"
                    " 5. Press [i] to inspect",
                    id="correlations-panel",
                )
        yield Footer()

    @staticmethod
    def _signature_preview(signature: set[str], max_items: int = 2) -> str:
        """Compact, deterministic preview of semantic value hints."""
        if not signature:
            return "-"
        items = sorted(signature)
        preview = items[:max_items]
        if len(items) > max_items:
            preview.append(f"+{len(items) - max_items} more")
        return ", ".join(preview)

    @staticmethod
    def _collect_value_candidates(value: Any, path: str = "") -> dict[str, str]:
        """
        Collect categorical scalar candidates for value-novelty detection.
        """
        out: dict[str, str] = {}
        if isinstance(value, dict):
            for key, child in value.items():
                child_path = f"{path}.{key}" if path else key
                out.update(AnalyzerApp._collect_value_candidates(child, child_path))
        elif isinstance(value, list):
            for item in value[:3]:
                out.update(AnalyzerApp._collect_value_candidates(item, f"{path}[]"))
        elif isinstance(value, str):
            cleaned = value.strip()
            if cleaned and len(cleaned) <= 64:
                out[path] = f"s:{cleaned}"
        elif isinstance(value, bool):
            out[path] = f"b:{value}"
        return out

    def _value_novelty_score_within_type(self, type_id: str, type_count: int, obj: dict) -> float:
        """
        Return novelty strength (0..1) for rare/new categorical values within a type.
        1.0 = brand-new/very rare value, 0.0 = common value.
        """
        candidates = self._collect_value_candidates(obj)
        if not candidates:
            return 0.0

        type_stats = self._type_value_stats[type_id]
        best_score = 0.0

        for path, token in candidates.items():
            stats = type_stats.get(path)
            if stats is None:
                stats = {"total": 0, "values": defaultdict(int)}
                type_stats[path] = stats

            total = int(stats["total"])
            values = stats["values"]
            distinct = len(values)
            value_count_before = int(values.get(token, 0))

            # Path-level gating: only score fields that look stable/categorical.
            if total >= 10 and distinct >= 2:
                unique_ratio = distinct / total if total else 1.0
                if distinct <= 32 and unique_ratio <= 0.80:
                    # Frequency of this value before current event.
                    freq = (value_count_before / total) if total else 0.0
                    # Rare-value band: <=10% of path observations.
                    score = 1.0 - min(1.0, freq / 0.10)
                    if score > best_score:
                        best_score = score

            stats["total"] += 1
            values[token] += 1

        # Keep first-seen/new types bright green; apply orange novelty only
        # after type has enough support to be considered "common".
        if type_count < 20:
            return 0.0
        return best_score

    # --- Lifecycle ---

    def on_mount(self):
        """Called when the app starts. Kick off the file watcher."""
        if self.marks:
            for mark in self.marks:
                self.correlation.add_period(
                    label=mark["name"],
                    start=mark["ts_start"],
                    end=mark["ts_end"],
                )
        self.watch_stream()

    # --- File Watcher ---

    @work(thread=True)
    def watch_stream(self):
        """
        Worker thread that tails the stream file.

        Polls the file for new lines every 50ms. This is the file-based
        equivalent of a TCP stream reader — simple, no dependencies,
        and works with any JSONL-producing source.
        """
        if self.is_replay and self.replay_file:
            self._replay_stream()
            return

        while True:
            # Wait for the file to appear (or reappear after source restart).
            while not os.path.exists(self.stream_path):
                self.call_from_thread(
                    self.update_status_panel,
                    f"Waiting for {self.stream_path}...\n"
                    f"Start demo_source.py first.",
                )
                time.sleep(0.5)

            self.call_from_thread(self.update_status_panel, "Connected — baseline recording")

            try:
                with open(self.stream_path, "r") as f:
                    current_inode = os.fstat(f.fileno()).st_ino

                    while True:
                        line = f.readline()
                        if line:
                            line = line.strip()
                            if line:
                                try:
                                    obj = json.loads(line)
                                    self.call_from_thread(self.process_object, obj)
                                except json.JSONDecodeError:
                                    continue
                            continue

                        # No new data — check if file was replaced/deleted.
                        if not os.path.exists(self.stream_path):
                            break
                        try:
                            path_inode = os.stat(self.stream_path).st_ino
                        except FileNotFoundError:
                            break
                        if path_inode != current_inode:
                            # Source rotated/recreated the stream file.
                            break

                        # Still the same file; keep polling.
                        time.sleep(0.05)
            except FileNotFoundError:
                # Race: file disappeared between exists() and open().
                time.sleep(0.1)

    def _read_replay_events(self) -> list[tuple[float, dict]]:
        """
        Read replay events from JSONL or JSON array.
        Supported entry forms:
          {"ts": <float>, "obj": {...}}
          {"timestamp": <float>, "obj": {...}}
          {...}  # plain object (synthetic incremental timestamps assigned)
        """
        if not self.replay_file:
            return []
        path = self.replay_file
        events: list[tuple[float, dict]] = []
        synthetic_ts = 0.0

        with open(path, "r") as f:
            content = f.read().strip()

        if not content:
            return events

        def _append_event(entry: Any):
            nonlocal synthetic_ts
            if not isinstance(entry, dict):
                return
            if "obj" in entry and isinstance(entry["obj"], dict):
                ts_raw = entry.get("ts", entry.get("timestamp"))
                if isinstance(ts_raw, (int, float)):
                    events.append((float(ts_raw), entry["obj"]))
                    return
                events.append((synthetic_ts, entry["obj"]))
                synthetic_ts += 1.0
                return

            ts_raw = entry.get("ts", entry.get("timestamp"))
            if isinstance(ts_raw, (int, float)):
                events.append((float(ts_raw), entry))
                return

            events.append((synthetic_ts, entry))
            synthetic_ts += 1.0

        # Try JSON array first.
        try:
            parsed = json.loads(content)
            if isinstance(parsed, list):
                for entry in parsed:
                    _append_event(entry)
            elif isinstance(parsed, dict):
                _append_event(parsed)
            else:
                # Fall back to JSONL parsing below.
                raise ValueError("Unsupported JSON root")
        except Exception:
            # JSONL fallback
            for line in content.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                _append_event(entry)

        events.sort(key=lambda e: e[0])
        return events

    def _replay_stream(self):
        """
        Replay mode: read pre-captured events and process in timestamp order.
        """
        events = self._read_replay_events()
        self.call_from_thread(
            self.update_status_panel,
            f"Replay mode: loaded {len(events)} events from {self.replay_file}",
        )
        if not events:
            return

        prev_ts: Optional[float] = None
        for ts, obj in events:
            if self.replay_speed > 0 and prev_ts is not None:
                delay = max(0.0, (ts - prev_ts) / self.replay_speed)
                if delay > 0:
                    time.sleep(delay)
            prev_ts = ts
            self.call_from_thread(self.process_object, obj, ts, True)

    def get_frequency_color(
        self,
        rarity_count: int,
        max_count: int,
        keyset_count: int,
        value_novelty_score: float = 0.0,
    ) -> str:
        """
        Calculate a color from rarity and keyset novelty.

        - Unique/near-unique objects stay bright green.
        - Rare keysets become orange (structural novelty cue).
        - Common recurring items fade toward grey.

        Uses a logarithmic scale because counts follow a power law.
        """
        if rarity_count <= 1:
            return "#00FF00"  # Neon Green (New/Unique)
        if value_novelty_score > 0:
            # Bright orange (very rare/new value) -> dull orange (less rare).
            bright = (255, 153, 0)
            dull = (138, 106, 58)
            val = min(max(value_novelty_score, 0.0), 1.0)
            r = int(dull[0] + val * (bright[0] - dull[0]))
            g = int(dull[1] + val * (bright[1] - dull[1]))
            b = int(dull[2] + val * (bright[2] - dull[2]))
            return f"#{r:02x}{g:02x}{b:02x}"
        if keyset_count <= 2:
            return "#FF9900"  # Orange (rare keyset / shape novelty)
        
        # Logarithmic normalization
        # log(1) = 0, log(max) = 1.0
        try:
            val = math.log(rarity_count) / math.log(max_count + 1)
        except ValueError:
            val = 0.0
            
        val = min(max(val, 0.0), 1.0)

        # Interpolate between Green (#00FF00) and Grey (#555555)
        # R: 0 -> 85 (0x55)
        # G: 255 -> 85 (0x55)
        # B: 0 -> 85 (0x55)
        
        r = int(0 + val * 85)
        g = int(255 - val * 170)
        b = int(0 + val * 85)
        
        return f"#{r:02x}{g:02x}{b:02x}"

    # --- Object Processing ---

    def process_object(self, obj: dict, observed_ts: Optional[float] = None, replay_mode: bool = False):
        """
        Handle a single object from the stream.

        Every object flows through here: fingerprinted, counted, checked
        against baseline, and tested for correlation with marked actions.
        """
        now = observed_ts if observed_ts is not None else time.time()
        self.object_count += 1
        self._rate_counter += 1

        # Update rate estimate (smoothed over 1-second windows)
        elapsed = now - self._rate_timestamp
        if elapsed >= 1.0:
            self.current_rate = self._rate_counter / elapsed
            self._rate_counter = 0
            self._rate_timestamp = now

        # --- Fingerprint and register ---
        type_id, is_new = self.registry.register(obj, now)
        obj_type = self.registry.get(type_id)

        # --- Feed to baseline and correlation engine ---
        if replay_mode:
            if not self.correlation.is_in_period(now):
                self.baseline.record(type_id)
            self.correlation.observe_at(type_id, now, raw_obj=obj)
        else:
            self.baseline.record(type_id)
            self.correlation.observe(type_id, now, raw_obj=obj)

        # --- Display in stream ---
        if obj_type and obj_type.ignored:
            return

        # Store rolling recent raw objects for type exploration UI.
        self._raw_by_type[type_id].append({"timestamp": now, "obj": obj})
        if len(self._raw_by_type[type_id]) > self._raw_cap_per_type:
            self._raw_by_type[type_id] = self._raw_by_type[type_id][-self._raw_cap_per_type:]

        stream = self.query_one("#stream", RichLog)
        ts_str = time.strftime("%H:%M:%S", time.localtime(now))
        ms = int((now % 1) * 1000)
        ts_display = f"{ts_str}.{ms:03d}"

        type_name = obj_type.display_name if obj_type else type_id[:8]
        sig_preview = (
            self._signature_preview(obj_type.semantic_signature, max_items=1)
            if obj_type else "-"
        )

        obj_summary = json.dumps(obj, separators=(",", ":"))
        if len(obj_summary) > 80:
            obj_summary = obj_summary[:77] + "..."

        # Color coding by frequency (sliding scale)
        # Find the max count currently in the registry for normalization
        # Color by structural family frequency, not only subtype frequency.
        shape_sig = json.dumps(obj_type.shape if obj_type else {}, sort_keys=True)
        self._shape_counts[shape_sig] += 1
        shape_count = self._shape_counts[shape_sig]
        max_count = max(self._shape_counts.values(), default=1)

        count = obj_type.count if obj_type else 1
        key_sig = tuple(sorted(obj.keys()))
        self._keyset_counts[key_sig] += 1
        keyset_count = self._keyset_counts[key_sig]
        value_novelty_score = self._value_novelty_score_within_type(type_id, count, obj)

        rarity_count = max(count, shape_count)
        color = self.get_frequency_color(
            rarity_count,
            max_count,
            keyset_count,
            value_novelty_score=value_novelty_score,
        )
        style = f"bold {color}" if is_new or count < 5 else color
        marker = "★" if is_new else " "

        sig_suffix = f"  sig:{sig_preview}" if is_new and sig_preview != "-" else ""
        line = Text(f"{ts_display} {marker} [{type_name}] {obj_summary}{sig_suffix}")
        line.stylize(style)

        stream.write(line)

        # Update sidebar periodically
        if self.object_count % 20 == 0:
            self.update_status_panel()
            self.update_types_panel()
            self.update_correlations_panel()

    # --- Sidebar Panels ---

    def update_status_panel(self, message: str | None = None):
        """Update the status panel in the sidebar."""
        panel = self.query_one("#status-panel", Static)

        if message and not self.object_count:
            panel.update(message)
            return

        # Baseline status
        if self.baseline.is_paused:
            baseline_str = "PAUSED (Action in progress)"
        else:
            baseline_str = (
                f"Recording ({self.baseline.duration:.0f}s, "
                f"{self.baseline.total_rate():.0f}/sec)"
            )
        mode_str = "REPLAY" if self.is_replay else "LIVE"

        # Summarize periods
        labels = self.correlation.action_labels()
        if labels:
            action_parts = []
            for lbl in labels:
                count = self.correlation.period_count(lbl)
                action_parts.append(f"{lbl} x{count}")
            marks_str = ", ".join(action_parts)
        else:
            marks_str = "none (press 'm')"

        # Current action status
        if self.correlation.is_in_action:
            action_status = f"ACTION: {self.correlation.active_period.label.upper()}"
        else:
            action_status = "IDLE (Baseline recording)"

        text = (
            f"STATUS\n"
            f"Objects: {self.object_count:,}  "
            f"Rate: {self.current_rate:.0f}/sec\n"
            f"Mode: {mode_str}\n"
            f"Baseline: {baseline_str}\n"
            f"Periods: {marks_str}\n"
            f"State: {action_status}"
        )
        panel.update(text)

    def update_types_panel(self):
        """Update the discovered types panel."""
        panel = self.query_one("#types-panel", Static)
        types = self.registry.all_types()[:12]

        if not types:
            panel.update("No types discovered yet")
            return

        total = len(self.registry.types)
        lines = [f"TYPES ({total} discovered)\n"]
        for t in types:
            marker = " ★" if t.count < 10 else ""
            novel = ""
            if self.baseline.is_ready and not self.baseline.is_known_type(t.type_id):
                novel = " [NEW]"
            lines.append(f"  {t.display_name}: {t.count:,}{marker}{novel}")
            lines.append(f"    sig: {self._signature_preview(t.semantic_signature)}")
        lines.append("\n[t] Explore all types")

        panel.update("\n".join(lines))

    def update_correlations_panel(self):
        """Compute and display correlations for all marked actions."""
        panel = self.query_one("#correlations-panel", Static)
        labels = self.correlation.action_labels()

        if not labels:
            panel.update(
                "No correlations yet\n\n"
                "Mark some actions first:\n"
                "  [l] set label\n"
                "  [m] START action\n"
                "  [m] END action\n"
                "  [i] inspect"
            )
            return

        text = Text("CORRELATIONS\n", style="bold")
        text.append("Legend: ")
        text.append("[STRONG]", style="bold green")
        text.append(" ")
        text.append("[MODERATE]", style="bold cyan")
        text.append(" ")
        text.append("[WEAK]", style="bold yellow")
        text.append(" ")
        text.append("[NOISE]\n", style="bold red")

        if not self.baseline.is_ready:
            text.append("(baseline building... need >10s)\n", style="dim")

        for label in labels:
            n = self.correlation.period_count(label)
            results = self.correlation.correlations(label)
            delayed = self.correlation.delayed_correlations(label)
            text.append(f'"{label}" ({n} trials):\n', style="bold")

            if not results:
                text.append("  No candidates found\n\n")
                continue

            for r in results[:5]:
                type_obj = self.registry.get(r["type_id"])
                type_name = type_obj.display_name if type_obj else r["type_id"][:8]
                band_label, band_style = strength_band(r["confidence"])

                conf = r["confidence"]
                filled = int(conf * 10)
                bar = "█" * filled + "░" * (10 - filled)

                text.append("  ")
                text.append(f"[{band_label}] ", style=band_style)
                text.append(f"{type_name}:\n")
                text.append(
                    f"    {bar} {conf:.2f}  "
                    f"({r['appearances']}/{r['trials']}, "
                    f"~{r['avg_latency_ms']:.0f}ms)\n"
                    f"    {r['assessment']}\n"
                )
            if delayed:
                text.append("  Delayed (post-window):\n", style="bold magenta")
                for r in delayed[:3]:
                    delayed_obj = self.registry.get(r["type_id"])
                    delayed_name = delayed_obj.display_name if delayed_obj else r["type_id"][:8]
                    band_label, band_style = strength_band(r["confidence"])
                    text.append("    ")
                    text.append(f"[{band_label}] ", style=band_style)
                    text.append(
                        f"{delayed_name}: {r['confidence']:.2f}  "
                        f"({r['appearances']}/{r['trials']}, ~{r['avg_latency_ms']:.0f}ms)\n"
                    )
            text.append("\n")

        panel.update(text)

    # --- Action Handlers ---

    def action_toggle_action(self):
        """
        Toggle action period (start or stop).
        INSTANT response — timestamp is recorded immediately.
        """
        period, started = self.correlation.toggle(self.current_label)
        
        stream = self.query_one("#stream", RichLog)
        ts_str = time.strftime("%H:%M:%S", time.localtime(period.start))
        ms = int((period.start % 1) * 1000)

        if started:
            text = Text(
                f"{'─' * 12} ACTION START #{period.id}: "
                f"\"{period.label}\" @ {ts_str}.{ms:03d} "
                f"{'─' * 12}"
            )
            text.stylize("bold green")
        else:
            end_ts = period.end or time.time()
            end_str = time.strftime("%H:%M:%S", time.localtime(end_ts))
            end_ms = int((end_ts % 1) * 1000)
            duration = end_ts - period.start
            text = Text(
                f"{'─' * 12} ACTION END #{period.id}: "
                f"\"{period.label}\" @ {end_str}.{end_ms:03d} "
                f"({duration:.1f}s) "
                f"{'─' * 12}"
            )
            text.stylize("bold red")

        stream.write(text)
        self.update_status_panel()

    def action_set_label(self):
        """
        Show the label input. Whatever the analyst types becomes the
        label used for future action periods.
        """
        inp = self.query_one("#label-input", Input)
        inp.display = True
        inp.value = self.current_label
        inp.focus()

    @on(Input.Submitted, "#label-input")
    def on_label_submitted(self, event: Input.Submitted):
        """Set the current label from the input field."""
        label = event.value.strip()
        inp = self.query_one("#label-input", Input)
        inp.display = False

        if label:
            self.current_label = label
            # Also update the active period label if one is open
            if self.correlation.is_in_action:
                self.correlation.active_period.label = label

            stream = self.query_one("#stream", RichLog)
            text = Text(f"{'─' * 12} Label set to: \"{label}\" {'─' * 12}")
            text.stylize("bold cyan")
            stream.write(text)

        self.update_status_panel()

    def action_correlate(self):
        """Compute and display correlation results."""
        self.update_correlations_panel()

    def action_inspect_correlations(self):
        """
        Open a modal screen with detailed correlation results.
        """
        self.push_screen(
            InspectionModal(
                self.correlation,
                self.registry,
                confidence_cutoff=self.inspect_confidence_cutoff,
            )
        )

    def _recent_raw_for_type(self, type_id: str) -> tuple[list[dict[str, Any]], int]:
        rows = self._raw_by_type.get(type_id, [])
        return rows, len(rows)

    def action_explore_types(self):
        """
        Open a modal for browsing discovered types and their recent raw objects.
        """
        self.push_screen(
            TypesExplorerModal(
                self.registry,
                self._recent_raw_for_type,
            )
        )

    def action_cancel_input(self):
        """Hide the label input (Escape key)."""
        inp = self.query_one("#label-input", Input)
        if inp.display:
            inp.display = False


# ============================================================================
# MAIN
# ============================================================================


def main():
    parser = argparse.ArgumentParser(description="JSON Stream Analyzer")
    parser.add_argument(
        "--path",
        default=DEFAULT_STREAM_DIR,
        help=f"Stream directory (default: {DEFAULT_STREAM_DIR})",
    )
    parser.add_argument(
        "--similarity-threshold",
        type=float,
        default=0.85,
        help="Structural similarity threshold for fuzzy type merges (default: 0.85)",
    )
    parser.add_argument(
        "--semantic-overlap-threshold",
        type=float,
        default=0.50,
        help="Minimum semantic signature overlap required for merges (default: 0.50)",
    )
    parser.add_argument(
        "--inspect-confidence-cutoff",
        type=float,
        default=0.20,
        help="Minimum confidence shown in Inspect candidate list (default: 0.20)",
    )
    parser.add_argument(
        "--post-window-sec",
        type=float,
        default=0.0,
        help="Seconds after action end to include delayed observations (default: 0.0)",
    )
    parser.add_argument(
        "--replay-file",
        help="Replay events from JSONL/JSON file instead of tailing live stream",
    )
    parser.add_argument(
        "--replay-speed",
        type=float,
        default=0.0,
        help="Replay speed multiplier. 0 = as fast as possible (default: 0.0)",
    )
    parser.add_argument(
        "--marks-file",
        help="JSON file with replay action periods: [{\"ts_start\":...,\"ts_end\":...,\"name\":\"...\"}]",
    )
    args = parser.parse_args()

    marks: list[dict[str, Any]] = []
    if args.marks_file:
        with open(args.marks_file, "r") as f:
            marks_raw = json.load(f)
        if not isinstance(marks_raw, list):
            raise ValueError("marks-file must be a JSON list")
        for item in marks_raw:
            if not isinstance(item, dict):
                continue
            if not {"ts_start", "ts_end", "name"} <= set(item.keys()):
                continue
            ts_start = float(item["ts_start"])
            ts_end = float(item["ts_end"])
            name = str(item["name"]).strip() or "action"
            if ts_end < ts_start:
                continue
            marks.append({"ts_start": ts_start, "ts_end": ts_end, "name": name})

    app = AnalyzerApp(
        stream_dir=args.path,
        similarity_threshold=args.similarity_threshold,
        semantic_overlap_threshold=args.semantic_overlap_threshold,
        inspect_confidence_cutoff=args.inspect_confidence_cutoff,
        post_window_sec=args.post_window_sec,
        replay_file=args.replay_file,
        replay_speed=args.replay_speed,
        marks=marks,
    )
    app.run()


if __name__ == "__main__":
    main()
