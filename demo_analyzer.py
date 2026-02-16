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
from typing import Optional

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
            for result in self.correlation.correlations(label):
                if result["confidence"] <= self.confidence_cutoff:
                    continue
                self._candidates.append({"label": label, "result": result})
        self._candidates.sort(key=lambda c: c["result"]["confidence"], reverse=True)

    def _selected_candidate(self) -> Optional[dict]:
        if not self._candidates:
            return None
        self._selected_index = max(0, min(self._selected_index, len(self._candidates) - 1))
        return self._candidates[self._selected_index]

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

        for i, candidate in enumerate(self._candidates):
            label = candidate["label"]
            result = candidate["result"]
            obj_type = self.registry.get(result["type_id"])
            type_name = obj_type.display_name if obj_type else result["type_id"][:8]
            prefix = "►" if i == self._selected_index else " "
            line = Text(
                f"{prefix} [{label}] {type_name}  conf={result['confidence']:.2f}  "
                f"{result['appearances']}/{result['trials']}  ~{result['avg_latency_ms']:.0f}ms"
            )
            if i == self._selected_index:
                line.stylize("bold yellow")
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
        log.write(
            Text(
                f"Confidence: {result['confidence']:.2f} ({result['assessment']})\n"
                f"Stats: {result['appearances']}/{result['trials']} trials, "
                f"latency ~{result['avg_latency_ms']:.0f}ms, "
                f"baseline {result['baseline_rate']:.2f}/sec"
            )
        )

        if obj_type and obj_type.semantic_signature:
            hints = ", ".join(sorted(obj_type.semantic_signature)[:6])
            if len(obj_type.semantic_signature) > 6:
                hints += f", +{len(obj_type.semantic_signature) - 6} more"
            log.write(Text(f"Semantic hints: {hints}"))
        example = obj_type.example if obj_type else {}
        log.write(Text("\nExample object:", style="bold cyan"))
        formatted = json.dumps(example, indent=2)
        indented = "\n".join("  " + ln for ln in formatted.splitlines())
        log.write(Text(indented, style="cyan"))

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
                    f"(+{row['latency_ms']:.0f}ms)"
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
        Binding("escape", "cancel_input", "Cancel", show=False),
    ]

    def __init__(
        self,
        stream_dir: str = DEFAULT_STREAM_DIR,
        similarity_threshold: float = 0.85,
        semantic_overlap_threshold: float = 0.50,
        inspect_confidence_cutoff: float = 0.20,
    ):
        super().__init__()
        self.stream_dir = stream_dir
        self.stream_path = os.path.join(stream_dir, "stream.jsonl")

        # Core analysis components (see engine.py for details)
        self.registry = TypeRegistry(
            similarity_threshold=similarity_threshold,
            semantic_overlap_threshold=semantic_overlap_threshold,
        )
        # Baseline auto-starts — records from the moment the analyzer launches
        self.baseline = BaselineModel()
        self.correlation = CorrelationEngine(self.baseline)
        self.inspect_confidence_cutoff = inspect_confidence_cutoff

        # The current action label. Set with 'l', used by 'm'.
        self.current_label = "action"

        # UI state
        self.object_count = 0
        self._rate_counter = 0
        self._rate_timestamp = time.time()
        self.current_rate = 0.0

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

    # --- Lifecycle ---

    def on_mount(self):
        """Called when the app starts. Kick off the file watcher."""
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

    def get_frequency_color(self, count: int, max_count: int) -> str:
        """
        Calculate a color on a sliding scale from Bright Green (rare) to Grey (common).
        Uses a logarithmic scale because counts follow a power law.
        """
        if count <= 1:
            return "#00FF00"  # Neon Green (New/Unique)
        
        # Logarithmic normalization
        # log(1) = 0, log(max) = 1.0
        try:
            val = math.log(count) / math.log(max_count + 1)
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

    def process_object(self, obj: dict):
        """
        Handle a single object from the stream.

        Every object flows through here: fingerprinted, counted, checked
        against baseline, and tested for correlation with marked actions.
        """
        now = time.time()
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

        # --- Feed to baseline (auto-records if not yet locked) ---
        self.baseline.record(type_id)

        # --- Feed to correlation engine ---
        self.correlation.observe(type_id, now, raw_obj=obj)

        # --- Display in stream ---
        if obj_type and obj_type.ignored:
            return

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
        max_count = max((t.count for t in self.registry.types.values()), default=1)
        count = obj_type.count if obj_type else 1
        
        color = self.get_frequency_color(count, max_count)
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

        lines = ["CORRELATIONS\n"]

        if not self.baseline.is_ready:
            lines.append("(baseline building... need >10s)\n")

        for label in labels:
            n = self.correlation.period_count(label)
            results = self.correlation.correlations(label)
            lines.append(f'"{label}" ({n} trials):')

            if not results:
                lines.append("  No candidates found")
                lines.append("")
                continue

            for r in results[:5]:
                type_obj = self.registry.get(r["type_id"])
                type_name = type_obj.display_name if type_obj else r["type_id"][:8]

                conf = r["confidence"]
                filled = int(conf * 10)
                bar = "█" * filled + "░" * (10 - filled)

                lines.append(
                    f"  {type_name}:\n"
                    f"    {bar} {conf:.2f}  "
                    f"({r['appearances']}/{r['trials']}, "
                    f"~{r['avg_latency_ms']:.0f}ms)\n"
                    f"    {r['assessment']}"
                )
            lines.append("")

        panel.update("\n".join(lines))

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
    args = parser.parse_args()

    app = AnalyzerApp(
        stream_dir=args.path,
        similarity_threshold=args.similarity_threshold,
        semantic_overlap_threshold=args.semantic_overlap_threshold,
        inspect_confidence_cutoff=args.inspect_confidence_cutoff,
    )
    app.run()


if __name__ == "__main__":
    main()
