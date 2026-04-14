#!/usr/bin/env python3
"""
capture.py — Render the current TUI screen into a structured JSON snapshot.

Usage:
  ./capture.py [session-name]          # prints JSON to stdout
  ./capture.py [session-name] out.json # writes JSON to file

The JSON output contains:
  - text_lines: plain text, one per row
  - styled_lines: per-row list of styled segments (fg, bg, bold, underline, etc.)
  - selections: rows that appear "selected" (distinct bg, reverse-video, or -> arrow)
  - highlights: cells/runs with standout styling
  - size: terminal dimensions used

Implementation notes:
  - pyte 0.8 names the underline attribute 'underscore' (not 'underline')
  - tmux capture-pane -e outputs bare '\n' between rows (no '\r'), so we
    normalise to '\r\n' before feeding pyte, otherwise all rows after the
    first land at the wrong column.
"""

import json
import re
import subprocess
import sys
from collections import Counter
from typing import Any

import pyte


# ---------------------------------------------------------------------------
# Colour normalisation
# ---------------------------------------------------------------------------

ANSI_256_PALETTE = {}

def _build_256_palette():
    basic = [
        "#000000","#800000","#008000","#808000","#000080","#800080","#008080","#c0c0c0",
        "#808080","#ff0000","#00ff00","#ffff00","#0000ff","#ff00ff","#00ffff","#ffffff",
    ]
    for i, h in enumerate(basic):
        ANSI_256_PALETTE[i] = h
    for i in range(216):
        r = (i // 36) * 51
        g = ((i // 6) % 6) * 51
        b = (i % 6) * 51
        ANSI_256_PALETTE[16 + i] = f"#{r:02x}{g:02x}{b:02x}"
    for i in range(24):
        v = 8 + i * 10
        ANSI_256_PALETTE[232 + i] = f"#{v:02x}{v:02x}{v:02x}"

_build_256_palette()


def normalise_colour(c) -> str:
    if c == "default" or c is None:
        return "default"
    if isinstance(c, int):
        return ANSI_256_PALETTE.get(c, f"ansi{c}")
    if isinstance(c, str):
        if c.startswith("#"):
            return c.lower()
        named = {
            "black": "#000000", "red": "#800000", "green": "#008000",
            "brown": "#808000", "blue": "#000080", "magenta": "#800080",
            "cyan": "#008080", "white": "#c0c0c0",
        }
        return named.get(c.lower(), c)
    return str(c)


# ---------------------------------------------------------------------------
# Screen capture via tmux + pyte
# ---------------------------------------------------------------------------

def get_raw_screen(session: str) -> tuple[str, int, int]:
    info = subprocess.run(
        ["tmux", "display-message", "-t", session, "-p", "#{window_width} #{window_height}"],
        capture_output=True, text=True, check=True,
    )
    cols, rows = map(int, info.stdout.strip().split())
    result = subprocess.run(
        ["tmux", "capture-pane", "-t", session, "-p", "-e", "-S", "0"],
        capture_output=True, check=True,
    )
    return result.stdout.decode("utf-8", errors="replace"), cols, rows


def render_screen(raw: str, cols: int, rows: int) -> pyte.Screen:
    """Feed raw ANSI output through pyte.

    tmux capture-pane -e separates rows with bare '\\n' (no '\\r'), so pyte
    never resets the cursor to column 0 between lines. Normalise to '\\r\\n'
    so each row lands in the correct column.
    """
    screen = pyte.Screen(cols, rows)
    stream = pyte.ByteStream(screen)
    normalised = raw.replace("\n", "\r\n").encode("utf-8", errors="replace")
    stream.feed(normalised)
    return screen


# ---------------------------------------------------------------------------
# Snapshot building
# ---------------------------------------------------------------------------

def cell_attrs(char: pyte.screens.Char) -> dict:
    return {
        "fg": normalise_colour(char.fg),
        "bg": normalise_colour(char.bg),
        "bold":          char.bold,
        "italics":       char.italics,
        "underline":     char.underscore,   # pyte 0.8 calls it 'underscore'
        "strikethrough": char.strikethrough,
        "reverse":       char.reverse,
    }


def build_styled_lines(screen: pyte.Screen) -> list[dict]:
    styled_lines = []
    for row_idx in range(screen.lines):
        row = screen.buffer[row_idx]
        segments = []
        current_text = ""
        current_attrs: dict | None = None

        for col_idx in range(screen.columns):
            char = row[col_idx]
            attrs = cell_attrs(char)
            glyph = char.data if char.data else " "

            if attrs == current_attrs:
                current_text += glyph
            else:
                if current_attrs is not None:
                    if current_text.strip() or current_attrs["bg"] != "default":
                        segments.append({"text": current_text, **current_attrs})
                current_text = glyph
                current_attrs = attrs

        if current_attrs is not None:
            if current_text.strip() or current_attrs["bg"] != "default":
                segments.append({"text": current_text, **current_attrs})

        styled_lines.append({"row": row_idx, "segments": segments})
    return styled_lines


def detect_selections(screen: pyte.Screen, styled_lines: list[dict]) -> list[dict]:
    """Identify rows that look 'selected'. Three detection strategies:
      1. Distinct background colour vs the screen modal background.
      2. Reverse-video attribute on any cell in the row.
      3. An explicit arrow marker ('-> ' or '=> ') in the row text.
    """
    bg_counts: Counter = Counter()
    for row_idx in range(screen.lines):
        row = screen.buffer[row_idx]
        for col_idx in range(screen.columns):
            char = row[col_idx]
            if char.data and char.data != " ":
                bg_counts[normalise_colour(char.bg)] += 1

    dominant_bg = bg_counts.most_common(1)[0][0] if bg_counts else "default"

    selections = []
    for row_idx in range(screen.lines):
        row = screen.buffer[row_idx]
        row_bgs: Counter = Counter()
        has_reverse = False

        for col_idx in range(screen.columns):
            char = row[col_idx]
            if char.data and char.data.strip():
                row_bgs[normalise_colour(char.bg)] += 1
                if char.reverse:
                    has_reverse = True

        plain = "".join(
            screen.buffer[row_idx][c].data or " "
            for c in range(screen.columns)
        ).rstrip()

        row_dominant_bg = row_bgs.most_common(1)[0][0] if row_bgs else dominant_bg
        has_arrow = bool(re.search(r'->\s|=>\s', plain))

        if row_dominant_bg != dominant_bg or has_reverse or has_arrow:
            reason = []
            if row_dominant_bg != dominant_bg:
                reason.append("distinct_bg")
            if has_reverse:
                reason.append("reverse_video")
            if has_arrow:
                reason.append("arrow_marker")

            selections.append({
                "row": row_idx,
                "bg": row_dominant_bg,
                "dominant_screen_bg": dominant_bg,
                "reverse_video": has_reverse,
                "arrow_marker": has_arrow,
                "reason": reason,
                "text": plain,
            })

    return selections


def detect_highlights(screen: pyte.Screen) -> list[dict]:
    """Find runs of cells with notable styling (bold, underline, colour, reverse)."""
    highlights = []
    for row_idx in range(screen.lines):
        row = screen.buffer[row_idx]
        run_start = None
        run_text = ""
        run_attrs: dict | None = None

        for col_idx in range(screen.columns):
            char = row[col_idx]
            if not char.data or not char.data.strip():
                if run_start is not None:
                    if _is_highlight(run_attrs):
                        highlights.append({
                            "row": row_idx,
                            "col_start": run_start,
                            "col_end": col_idx - 1,
                            "text": run_text,
                            **run_attrs,
                        })
                    run_start = None
                    run_text = ""
                    run_attrs = None
                continue

            attrs = cell_attrs(char)
            if attrs == run_attrs:
                run_text += char.data
            else:
                if run_start is not None and _is_highlight(run_attrs):
                    highlights.append({
                        "row": row_idx,
                        "col_start": run_start,
                        "col_end": col_idx - 1,
                        "text": run_text,
                        **run_attrs,
                    })
                run_start = col_idx
                run_text = char.data
                run_attrs = attrs

        if run_start is not None and _is_highlight(run_attrs):
            highlights.append({
                "row": row_idx,
                "col_start": run_start,
                "col_end": screen.columns - 1,
                "text": run_text,
                **run_attrs,
            })

    return highlights


def _is_highlight(attrs: dict | None) -> bool:
    if attrs is None:
        return False
    return (
        attrs["bold"]
        or attrs["underline"]
        or attrs["italics"]
        or attrs["strikethrough"]
        or attrs["reverse"]
        or attrs["fg"] not in ("default", "#c0c0c0", "#ffffff", "#000000")
        or attrs["bg"] not in ("default", "#000000")
    )


def build_snapshot(session: str) -> dict[str, Any]:
    raw, cols, rows = get_raw_screen(session)
    screen = render_screen(raw, cols, rows)

    text_lines = []
    for row_idx in range(screen.lines):
        row = screen.buffer[row_idx]
        line = "".join(row[c].data or " " for c in range(screen.columns)).rstrip()
        text_lines.append(line)

    styled_lines = build_styled_lines(screen)
    selections = detect_selections(screen, styled_lines)
    highlights = detect_highlights(screen)

    return {
        "session": session,
        "size": {"cols": cols, "rows": rows},
        "text_lines": text_lines,
        "styled_lines": styled_lines,
        "selections": selections,
        "highlights": highlights,
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    session = sys.argv[1] if len(sys.argv) > 1 else "tui-test"
    out_file = sys.argv[2] if len(sys.argv) > 2 else None

    if subprocess.run(["tmux", "has-session", "-t", session], capture_output=True).returncode != 0:
        print(f"ERROR: No tmux session '{session}'.", file=sys.stderr)
        sys.exit(1)

    snapshot = build_snapshot(session)
    output = json.dumps(snapshot, indent=2)

    if out_file:
        with open(out_file, "w") as f:
            f.write(output)
        print(f"Snapshot written to {out_file}", file=sys.stderr)
    else:
        print(output)
