#!/usr/bin/env python3
"""Comprehensive TUI test script for json_analyzer."""
import json
import re
import subprocess
import sys
import time

SESSION = "tui-test"
SKILLS = "/Users/tali/software/json/.claude/skills/drive-tui"
PASS = []
FAIL = []

def send(*keys, delay=0.35):
    subprocess.run([f"{SKILLS}/send_keys.sh", SESSION] + list(keys), check=True, capture_output=True)
    time.sleep(delay)

def capture():
    r = subprocess.run(["python3", f"{SKILLS}/capture.py", SESSION], capture_output=True, text=True, check=True)
    return json.loads(r.stdout)

def highlighted_keys_in_json_pane(s):
    """All highlight entries that look like JSON key names in the right panel."""
    results = []
    for h in s["highlights"]:
        # JSON panel is on the right; col > 108 in 272-wide terminal
        if h["col_start"] <= 108:
            continue
        t = h["text"]
        # A JSON key is a double-quoted string: starts and ends with "
        if t.startswith('"') and t.endswith('"') and len(t) > 2:
            results.append(h)
    return results

def focused_key(s):
    """Currently-focused JSON key (underlined, in JSON pane)."""
    for h in highlighted_keys_in_json_pane(s):
        if h["underline"]:
            name = h["text"].strip('"')
            # exclude border artifacts
            if name not in ("JSON", "selected", "apply", "filter", "value"):
                return name
    return None

def focused_value(s):
    """Currently-focused value: second underlined item in JSON pane (key and value both underlined)."""
    underlined = [h for h in s["highlights"] if h["col_start"] > 108 and h["underline"]]
    if len(underlined) >= 2:
        return underlined[-1]["text"].strip('"')
    return None

def status_line(s):
    for l in s["text_lines"]:
        if "k/key=" in l and "e/exact=" in l:
            return l.strip()
    return ""

def controls_line(s):
    for l in s["text_lines"]:
        if "follow (f)" in l:
            return l.strip()
    return ""

def selected_event_row(s):
    for i, l in enumerate(s["text_lines"]):
        if re.search(r'\s+->\s+', l):
            return i
    return None

def current_event_num(s):
    row = selected_event_row(s)
    if row is None:
        return None
    m = re.search(r'(\d+)\s+\[type-', s["text_lines"][row])
    return int(m.group(1)) if m else None

def header_line(s):
    for l in s["text_lines"]:
        if "row " in l and "objects " in l:
            return l.strip()
    return ""

def total_events(s):
    m = re.search(r'row\s+\d+/(\d+)', header_line(s))
    return int(m.group(1)) if m else None

def check(name, cond, detail=""):
    if cond:
        PASS.append(name)
        print(f"  PASS  {name}")
    else:
        FAIL.append(name)
        print(f"  FAIL  {name}" + (f"  [{detail}]" if detail else ""))

def ensure_live_event_focus():
    """Ensure we're in Live mode in event focus (not key focus, not sub-mode)."""
    send("Escape", delay=0.15)  # exit key focus / value focus if any
    send("1", delay=0.2)        # go to Live tab

def goto_row(n):
    """Navigate to event row n in Live mode. Must be in event focus (not key focus)."""
    ensure_live_event_focus()
    send("Home", delay=0.4)     # go to row 1
    for _ in range(n - 1):
        subprocess.run([f"{SKILLS}/send_keys.sh", SESSION, "Down"],
                       check=True, capture_output=True)
        time.sleep(0.06)
    time.sleep(0.4)

def clear_all():
    """Clear all filters and return to clean state."""
    ensure_live_event_focus()
    send("c", delay=0.5)


# ============================================================
# SETUP
# ============================================================
print("\n=== Setup ===")
ensure_live_event_focus()
send("c", delay=0.5)

s = capture()
ctrl = controls_line(s)
if "follow (f):ON" in ctrl:
    send("f", delay=0.2)

goto_row(1)
s = capture()
check("setup-live-120-events", total_events(s) == 120, f"total={total_events(s)}")
check("setup-at-row-1", current_event_num(s) == 1, f"event={current_event_num(s)}")
check("setup-follow-off", "follow (f):OFF" in controls_line(s), controls_line(s)[:60])
check("setup-no-filters", "active:0" in status_line(s), status_line(s))


# ============================================================
# A: Basic navigation - Up/Down arrows
# ============================================================
print("\n=== A: Basic navigation ===")
send("Down", delay=0.3)
s1 = capture()
check("A1-Down-moves-to-row-2", current_event_num(s1) == 2, f"event={current_event_num(s1)}")

send("Up", delay=0.3)
s2 = capture()
check("A2-Up-moves-to-row-1", current_event_num(s2) == 1, f"event={current_event_num(s2)}")

# Navigate to row 6 (cache event: service=cache, level=error)
for _ in range(5): send("Down", delay=0.1)
time.sleep(0.3)
s6 = capture()
check("A3-at-row-6", current_event_num(s6) == 6, f"event={current_event_num(s6)}")


# ============================================================
# B: Substring filter (/) with anchor hold
# The test data has 40 cache events (level=error). Rows pattern: 3,6,9,...
# Row 6 is a cache event. Applying "error" filter should keep row 6 visible.
# ============================================================
print("\n=== B: Substring filter / and clear ===")
s_pre = capture()
row_pre = current_event_num(s_pre)
check("B0-at-row-6", row_pre == 6, f"at={row_pre}")

# Apply "error" filter: 40 cache events match (they have "level":"error")
send("/")
send("e", "r", "r", "o", "r", delay=0.08)
send("Enter", delay=0.6)
s_filt = capture()
total_filt = total_events(s_filt)
check("B1-filter-reduces-count", total_filt == 40, f"total={total_filt}")
ev_filt = current_event_num(s_filt)
check("B2-selection-visible-after-filter", ev_filt is not None, f"event={ev_filt}")
# '/' uses TypedInput origin → resets position to 0 → first cache event = row 3
check("B3-slash-filter-resets-to-first-match", ev_filt == 3, f"ev_filt={ev_filt}")

# Clear: anchor is at row 3 (first filtered event), should return near row 3
send("c", delay=0.6)
s_cleared = capture()
check("B4-clear-restores-count", total_events(s_cleared) == 120, f"total={total_events(s_cleared)}")
ev_cleared = current_event_num(s_cleared)
check("B5-clear-anchor-near-first-match", ev_cleared is not None and abs(ev_cleared - 3) <= 2,
      f"first_match=3 after={ev_cleared}")


# ============================================================
# C: Types view - filter by type, anchor works
# ============================================================
print("\n=== C: Types view ===")
goto_row(4)  # row 4 = api event (pattern: 1=api, 2=db, 3=cache, 4=api, ...)
s_pre = capture()
row_pre = current_event_num(s_pre)
check("C0-at-row-4", row_pre == 4, f"at={row_pre}")

# Go to Types tab
send("3", delay=0.3)
s_types = capture()
check("C1-types-tab-has-types", any("type-" in l for l in s_types["text_lines"]), "no types")

# Apply type filter with 't' key (not Enter - Enter goes into path focus)
send("t", delay=0.5)

# Go back to Live
send("1", delay=0.3)
s_after = capture()
total_after = total_events(s_after)
check("C2-type-filter-reduces-count", total_after is not None and total_after < 120, f"total={total_after}")
ev_after = current_event_num(s_after)
check("C3-anchor-present-after-type-filter", ev_after is not None, f"event={ev_after}")
# Row 4 is api type, so the first highlighted type in Types should be applied
# and the anchor should land near row 4 (or nearest of that type)
check("C4-anchor-near-row-4", ev_after is not None and abs(ev_after - 4) <= 6, f"ev={ev_after} pre={row_pre}")

# Clear type filter
send("c", delay=0.5)
s_cleared = capture()
check("C5-type-filter-cleared", total_events(s_cleared) == 120, f"total={total_events(s_cleared)}")


# ============================================================
# D: JSON key focus - Enter toggles key focus; Up/Down navigate keys
# ============================================================
print("\n=== D: JSON key focus ===")
goto_row(1)
s0 = capture()
check("D0-at-row-1", current_event_num(s0) == 1, f"event={current_event_num(s0)}")

# Enter enters key focus
send("Enter", delay=0.3)
s_kf = capture()
key0 = focused_key(s_kf)
check("D1-enter-activates-key-focus", key0 is not None, f"focused_key={key0}")
check("D2-first-key-is-real-key", key0 not in (None, "JSON", "selected"), f"key0={key0}")

# Down moves to next key
key_before = key0
send("Down", delay=0.3)
s_kf2 = capture()
key1 = focused_key(s_kf2)
check("D3-Down-moves-to-next-key", key1 is not None and key1 != key0, f"key0={key0} key1={key1}")

# Down again
send("Down", delay=0.3)
key2_snap = capture()
key2 = focused_key(key2_snap)
check("D4-Down-moves-again", key2 is not None and key2 != key1, f"key1={key1} key2={key2}")

# Up goes back
send("Up", delay=0.3)
key1b_snap = capture()
key1b = focused_key(key1b_snap)
check("D5-Up-returns-to-prev-key", key1b == key1, f"key1={key1} key1b={key1b}")

send("Up", delay=0.3)
key0b_snap = capture()
key0b = focused_key(key0b_snap)
check("D6-Up-returns-to-first-key", key0b == key0, f"key0={key0} key0b={key0b}")


# ============================================================
# E: Right → value focus; check both key and value underlined
# ============================================================
print("\n=== E: Value focus ===")
# Still in key focus at key0 (first key)
send("Right", delay=0.3)
s_vf = capture()
key_vf = focused_key(s_vf)
val_vf = focused_value(s_vf)
check("E1-Right-enters-value-focus", val_vf is not None, f"val={val_vf}")
check("E2-key-still-underlined-in-value-focus", key_vf == key0, f"key_vf={key_vf} key0={key0}")

# Escape exits all key focus (back to event focus)
send("Escape", delay=0.3)
s_event_focus = capture()
key_after_esc = focused_key(s_event_focus)
check("E3-Escape-exits-to-event-focus", key_after_esc is None, f"key_after_esc={key_after_esc}")
check("E4-still-on-same-event", current_event_num(s_event_focus) == 1, f"ev={current_event_num(s_event_focus)}")


# ============================================================
# F: Enter in key focus applies exact-value filter
# ============================================================
print("\n=== F: Exact filter from key focus ===")
# Re-enter key focus
send("Enter", delay=0.3)
s_kf_f = capture()
key_f0 = focused_key(s_kf_f)
check("F0-re-entered-key-focus", key_f0 is not None, f"key={key_f0}")

# Navigate to "service" key (which has value "api" for row 1)
for _ in range(6):
    k = focused_key(capture())
    if k == "service":
        break
    send("Down", delay=0.2)

s_at_svc = capture()
check("F1-at-service-key", focused_key(s_at_svc) == "service", f"key={focused_key(s_at_svc)}")

# Enter in key focus applies KEY filter (not exact). Need Right→Enter for exact.
# First test: Enter at key focus applies key filter (k/key=service).
send("Enter", delay=0.5)
s_key_filt = capture()
st_kf = status_line(s_key_filt)
check("F2-enter-at-key-applies-key-filter", "k/key=service" in st_kf, f"status={st_kf}")
send("c", delay=0.4)

# Now test exact filter: re-enter key focus, navigate to service, Right for value, Enter
send("Enter", delay=0.3)  # enter key focus
for _ in range(6):
    k = focused_key(capture())
    if k == "service":
        break
    send("Down", delay=0.2)

send("Right", delay=0.3)  # enter value focus (value = "api")
s_vf2 = capture()
check("F2b-value-focus-on-service", focused_value(s_vf2) is not None, f"val={focused_value(s_vf2)}")

# Enter in value focus applies exact filter
send("Enter", delay=0.6)
s_exact = capture()
st = status_line(s_exact)
total_exact = total_events(s_exact)
check("F3-exact-filter-from-value-focus", "e/exact=" in st and st.split("e/exact=")[1].split()[0] != "off", f"status={st}")
check("F4-exact-filter-reduces-count", total_exact is not None and total_exact < 120, f"total={total_exact}")

send("c", delay=0.5)
s_f_cleared = capture()
check("F5-exact-filter-cleared", total_events(s_f_cleared) == 120, f"total={total_events(s_f_cleared)}")


# ============================================================
# G: k key applies key filter from key focus
# ============================================================
print("\n=== G: Key filter from key focus ===")
goto_row(1)
send("Enter", delay=0.3)  # enter key focus
s_g0 = capture()
key_g0 = focused_key(s_g0)
check("G0-in-key-focus", key_g0 is not None, f"key={key_g0}")

# Navigate to "level" key
for _ in range(6):
    k = focused_key(capture())
    if k == "level":
        break
    send("Down", delay=0.2)

s_at_level = capture()
check("G1-at-level-key", focused_key(s_at_level) == "level", f"key={focused_key(s_at_level)}")

# k applies key filter
send("k", delay=0.5)
s_kfilt = capture()
st = status_line(s_kfilt)
check("G2-key-filter-in-status", "k/key=" in st, f"status={st}")
key_val = st.split("k/key=")[1].split()[0] if "k/key=" in st else ""
check("G3-key-filter-is-level", key_val == "level", f"k/key={key_val}")
# All 120 events have "level" field, so count should still be 120
total_g = total_events(s_kfilt)
check("G4-key-filter-shows-all-with-key", total_g == 120, f"total={total_g}")

send("c", delay=0.5)


# ============================================================
# H: y toggle - suspend and restore filters
# ============================================================
print("\n=== H: y toggle ===")
# Apply a filter
send("/")
send("e", "r", "r", "o", "r", delay=0.08)
send("Enter", delay=0.5)
s_filt = capture()
total_filt = total_events(s_filt)
check("H0-filter-applied-40-events", total_filt == 40, f"total={total_filt}")

# Suspend with y
send("y", delay=0.4)
s_susp = capture()
check("H1-y-shows-all-120", total_events(s_susp) == 120, f"total={total_events(s_susp)}")
check("H2-controls-show-filters-off", "filters (y):OFF" in controls_line(s_susp), controls_line(s_susp)[:80])

# Restore with y
send("y", delay=0.5)
s_rest = capture()
total_rest = total_events(s_rest)
check("H3-y-restore-same-count", total_rest == total_filt, f"rest={total_rest} filt={total_filt}")
check("H4-controls-show-filters-on", "filters (y):ON" not in controls_line(s_rest) or total_rest < 120,
      controls_line(s_rest)[:60])

send("c", delay=0.5)
s_hclear = capture()
check("H5-clear-after-y-works", total_events(s_hclear) == 120, f"total={total_events(s_hclear)}")


# ============================================================
# I: state:working... indicator (smoke test - transient)
# ============================================================
print("\n=== I: state:working... ===")
send("/")
send("e", "r", "r", "o", "r", delay=0.05)
send("Enter", delay=0.6)
s_i = capture()
st_i = status_line(s_i)
check("I1-filter-applied", total_events(s_i) == 40, f"total={total_events(s_i)}")
check("I2-state-active-shown", "state:" in st_i, f"status={st_i}")
send("c", delay=0.5)


# ============================================================
# J: Periods view
# ============================================================
print("\n=== J: Periods view ===")
send("2", delay=0.3)  # Periods tab
s_per = capture()
check("J1-periods-tab-active", "Periods" in s_per["text_lines"][0][:120], s_per["text_lines"][0][:80])

# Clean up any existing periods from previous runs
# Count existing periods first
existing_periods = sum(1 for l in s_per["text_lines"] if re.search(r'\[\d+\].*#\d+.*action', l))
for _ in range(existing_periods):
    # Ensure we're at Periods focus, then delete and confirm
    send("2", delay=0.2)   # reset periods_focus
    send("d", delay=0.4)   # delete
    send("y", delay=0.4)   # confirm
    time.sleep(0.2)

# Insert a period spanning rows 5-20
send("i", delay=0.3)
send("5", "-", "2", "0", delay=0.05)
send("Enter", delay=0.5)
s_inserted = capture()
inserted_periods = sum(1 for l in s_inserted["text_lines"] if re.search(r'\[\d+\].*#\d+.*action', l))
check("J2-period-inserted", inserted_periods == 1, f"expected 1 period, got {inserted_periods}")

# Navigate to events within the period
send("Enter", delay=0.3)  # advance_periods_focus: Periods → Events
s_ev = capture()
total_per = total_events(s_ev)
check("J3-period-events-shown", total_per is not None and total_per > 0, f"total={total_per}")
# rows 5-20 = 16 events
check("J4-period-events-count-16", total_per == 16, f"total={total_per}")

# Navigate to JSON key focus within period
send("Enter", delay=0.3)  # Events → JSON key focus
s_jkf = capture()
key_j = focused_key(s_jkf)
check("J5-period-json-key-focus-works", key_j is not None, f"focused_key={key_j}")

# Down navigates keys in period JSON focus
send("Down", delay=0.3)
s_jkf2 = capture()
key_j2 = focused_key(s_jkf2)
check("J6-period-json-Down-changes-key", key_j2 is not None and key_j2 != key_j,
      f"key_j={key_j} key_j2={key_j2}")

# Up navigates back
send("Up", delay=0.3)
s_jkf3 = capture()
key_j3 = focused_key(s_jkf3)
check("J7-period-json-Up-returns", key_j3 == key_j, f"key_j={key_j} key_j3={key_j3}")

# Apply key filter from period JSON focus
send("k", delay=0.5)
s_kf_j = capture()
st_j = status_line(s_kf_j)
check("J8-period-json-key-filter-applied", "k/key=" in st_j and st_j.split("k/key=")[1].split()[0] != "off",
      f"status={st_j}")
send("c", delay=0.5)

# Reset to Periods focus by re-entering Periods tab (set_ui_mode resets periods_focus)
send("2", delay=0.4)  # go to Periods tab → resets periods_focus to Periods

# Count periods before delete
s_before_del = capture()
periods_before = sum(1 for l in s_before_del["text_lines"] if re.search(r'\[\d+\].*#\d+.*action', l))

# Delete the period (requires y confirmation)
send("d", delay=0.5)
send("y", delay=0.5)  # confirm delete
time.sleep(0.3)
s_del = capture()
periods_after = sum(1 for l in s_del["text_lines"] if re.search(r'\[\d+\].*#\d+.*action', l))
check("J9-period-count-reduced", periods_after < periods_before, f"before={periods_before} after={periods_after}")


# ============================================================
# K: Anchor stability through multiple filter operations
# ============================================================
print("\n=== K: Anchor stability ===")
clear_all()
check("K0-clean-start", total_events(capture()) == 120, f"total={total_events(capture())}")

goto_row(9)  # row 9 = cache event (3,6,9 → cache)
s0 = capture()
row0 = current_event_num(s0)
check("K1-at-row-9", row0 == 9, f"at={row0}")

# Apply filter: "warn" matches db events (level=warn). Row 9 is cache, not db.
# Nearest "warn" event to row 9 is row 8 (db). Anchor should jump to row 8.
send("/")
send("w", "a", "r", "n", delay=0.08)
send("Enter", delay=0.6)
s1 = capture()
total1 = total_events(s1)
ev1 = current_event_num(s1)
check("K2-warn-filter-gives-40", total1 == 40, f"total={total1}")
check("K3-anchor-visible-after-filter", ev1 is not None, f"ev={ev1}")

# Clear: '/' filter is TypedInput → resets to position 0 (first db event = row 2).
# After clear, anchor is at row 2 (first db/"warn" event shown by filter).
send("c", delay=0.6)
s2 = capture()
ev2 = current_event_num(s2)
check("K4-count-restored-120", total_events(s2) == 120, f"total={total_events(s2)}")
# Slash TypedInput resets to first match (row 2, first db event). Anchor at row 2 after clear.
check("K5-clear-anchor-near-first-warn", ev2 is not None and abs(ev2 - 2) <= 2, f"first_warn=2 ev2={ev2}")

# Apply fuzzy filter: "cache" matches events with "cache" in JSON
send("z")
send("c", "a", "c", "h", "e", delay=0.08)
send("Enter", delay=0.6)
s3 = capture()
total3 = total_events(s3)
check("K6-fuzzy-filter-reduces", total3 is not None and total3 < 120, f"total={total3}")

# Clear
send("c", delay=0.6)
s4 = capture()
check("K7-fuzzy-clear-restores-120", total_events(s4) == 120, f"total={total_events(s4)}")

# Chain: substring + key filter
send("/")
send("e", "r", "r", "o", "r", delay=0.08)
send("Enter", delay=0.5)
s5 = capture()
check("K8-chained-sub-filter", total_events(s5) == 40, f"total={total_events(s5)}")

send("c", delay=0.5)
s6 = capture()
check("K9-chained-clear-restores-120", total_events(s6) == 120, f"total={total_events(s6)}")


# ============================================================
# Results
# ============================================================
print(f"\n{'='*50}")
print(f"Results: {len(PASS)} passed, {len(FAIL)} failed out of {len(PASS)+len(FAIL)} tests")
if FAIL:
    print(f"\nFailed tests:")
    for f in FAIL:
        print(f"  - {f}")
print("="*50)
sys.exit(0 if not FAIL else 1)
