#!/usr/bin/env python3
"""
diff_snapshots.py — Compare two capture.py snapshots and summarise what changed.

Usage:
  ./diff_snapshots.py before.json after.json

Output is JSON with:
  - text_changes: rows whose plain text changed
  - selection_changes: rows that gained/lost/moved selection
  - highlight_changes: highlights that appeared or disappeared
  - summary: human-readable list of change descriptions
"""

import json
import sys


def load(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def diff_text(before: list[str], after: list[str]) -> list[dict]:
    changes = []
    for i, (b, a) in enumerate(zip(before, after)):
        if b != a:
            changes.append({"row": i, "before": b.rstrip(), "after": a.rstrip()})
    for i in range(len(before), len(after)):
        changes.append({"row": i, "before": "", "after": after[i].rstrip()})
    for i in range(len(after), len(before)):
        changes.append({"row": i, "before": before[i].rstrip(), "after": ""})
    return changes


def rows_set(selections: list[dict]) -> dict[int, dict]:
    return {s["row"]: s for s in selections}


def diff_selections(before_sels: list[dict], after_sels: list[dict]) -> dict:
    b = rows_set(before_sels)
    a = rows_set(after_sels)
    gained = [s for row, s in a.items() if row not in b]
    lost   = [s for row, s in b.items() if row not in a]
    moved  = []
    if len(gained) == 1 and len(lost) == 1:
        moved = [{"from_row": lost[0]["row"], "from_text": lost[0]["text"],
                  "to_row":   gained[0]["row"], "to_text":   gained[0]["text"]}]
        gained = []
        lost = []
    return {"gained": gained, "lost": lost, "moved": moved}


def key_highlights(highlights: list[dict]) -> set[str]:
    return {f"{h['row']}:{h['col_start']}:{h['text']}" for h in highlights}


def diff_highlights(before_hl: list[dict], after_hl: list[dict]) -> dict:
    bk = key_highlights(before_hl)
    ak = key_highlights(after_hl)
    appeared   = [h for h in after_hl  if f"{h['row']}:{h['col_start']}:{h['text']}" not in bk]
    disappeared = [h for h in before_hl if f"{h['row']}:{h['col_start']}:{h['text']}" not in ak]
    return {"appeared": appeared, "disappeared": disappeared}


def build_summary(text_changes, sel_diff, hl_diff) -> list[str]:
    s = []
    if text_changes:
        s.append(f"{len(text_changes)} row(s) changed text content")
    if sel_diff["moved"]:
        m = sel_diff["moved"][0]
        s.append(f"Selection moved: row {m['from_row']} \"{m['from_text'].strip()[:40]}\" "
                 f"→ row {m['to_row']} \"{m['to_text'].strip()[:40]}\"")
    if sel_diff["gained"]:
        for g in sel_diff["gained"]:
            s.append(f"New selection at row {g['row']}: \"{g['text'].strip()[:60]}\"")
    if sel_diff["lost"]:
        for l in sel_diff["lost"]:
            s.append(f"Selection lost at row {l['row']}: \"{l['text'].strip()[:60]}\"")
    if hl_diff["appeared"]:
        s.append(f"{len(hl_diff['appeared'])} new highlight(s) appeared")
        for h in hl_diff["appeared"][:5]:
            s.append(f"  + row {h['row']} col {h['col_start']}: \"{h['text'].strip()[:40]}\" "
                     f"(bold={h['bold']}, underline={h['underline']}, fg={h['fg']}, bg={h['bg']})")
    if hl_diff["disappeared"]:
        s.append(f"{len(hl_diff['disappeared'])} highlight(s) disappeared")
    if not s:
        s.append("No visible changes detected")
    return s


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: diff_snapshots.py before.json after.json", file=sys.stderr)
        sys.exit(1)

    before = load(sys.argv[1])
    after  = load(sys.argv[2])

    text_changes = diff_text(before["text_lines"], after["text_lines"])
    sel_diff     = diff_selections(before["selections"], after["selections"])
    hl_diff      = diff_highlights(before["highlights"], after["highlights"])
    summary      = build_summary(text_changes, sel_diff, hl_diff)

    print(json.dumps({
        "text_changes":      text_changes,
        "selection_changes": sel_diff,
        "highlight_changes": hl_diff,
        "summary":           summary,
    }, indent=2))
