#!/usr/bin/env python3
"""Generate a JSONL corpus covering many JSON shapes and sizes for UI regression testing.

Each scenario is a named family of events that stresses a specific rendering or
filtering path in the TUI.  Run this once to produce a file you can load with
the app and manually (or drive-tui) verify that nothing panics or misrenders.

Scenarios
---------
empty_values          – {}, [], "", 0, null, false at every position
scalar_array          – top-level payload arrays of pure scalars
mixed_array           – arrays mixing objects, scalars, null, bool, nested arrays
deep_nesting          – objects nested 15+ levels deep
wide_object           – single object with 150+ keys
sparse_schema         – same structural "type" but most keys absent on any given event
long_strings          – string values of 500–5000 chars; also very long key names
unicode_content       – emoji, RTL text, zero-width chars, combining chars in values
numeric_extremes      – very large ints, negative, floats near MAX, tiny floats
array_of_arrays       – nested array-of-array structures (triggered recent bugs)
homogenous_long_array – large array of identical objects (96+ items)
heterogeneous_array   – array where each element has a different shape
top_level_null_fields – many null values scattered through a complex object
schema_drift          – events that are structurally similar but with one key added/removed per event
single_key            – events with exactly one key: edge for type identity
type_collision        – two structurally identical shapes with different value types
"""

import argparse
import json
import random
import time
from pathlib import Path

RNG = random.Random(42)

BASE_MS = int(time.time() * 1000)
_seq = 0


def ts() -> int:
    global _seq
    _seq += 1
    return BASE_MS + _seq


def emit(scenario: str, payload: object, extra: dict | None = None) -> dict:
    obj: dict = {"_timestamp": ts(), "scenario": scenario}
    if extra:
        obj.update(extra)
    obj["data"] = payload
    return obj


# ---------------------------------------------------------------------------
# Scenario builders
# ---------------------------------------------------------------------------

def scenario_empty_values() -> list[dict]:
    events = []
    for shape, val in [
        ("empty_object", {}),
        ("empty_array", []),
        ("empty_string", ""),
        ("zero_int", 0),
        ("zero_float", 0.0),
        ("null_top", None),
        ("false_val", False),
        ("true_val", True),
    ]:
        events.append(emit("empty_values", val, {"shape": shape}))
    # Nested empties
    events.append(emit("empty_values", {"a": {}, "b": [], "c": None, "d": ""}, {"shape": "nested_empties"}))
    events.append(emit("empty_values", [[], {}, None, "", 0, False], {"shape": "array_of_empties"}))
    return events


def scenario_scalar_array() -> list[dict]:
    events = []
    # All-int array of various sizes
    for n in [1, 2, 5, 10, 50, 200]:
        events.append(emit("scalar_array", list(range(n)), {"subtype": "ints", "n": n}))
    # All-string
    for n in [1, 5, 20]:
        events.append(emit("scalar_array", [f"item-{i}" for i in range(n)], {"subtype": "strings", "n": n}))
    # All-null
    events.append(emit("scalar_array", [None] * 10, {"subtype": "nulls"}))
    # All-bool
    events.append(emit("scalar_array", [i % 2 == 0 for i in range(8)], {"subtype": "bools"}))
    # Mixed scalars
    events.append(emit("scalar_array", [1, "two", 3.0, None, True, False, 0, ""], {"subtype": "mixed"}))
    return events


def scenario_mixed_array() -> list[dict]:
    events = []
    # Array where each element is a different type
    events.append(emit("mixed_array", [
        1, "hello", None, True, False, 3.14,
        {"nested": "object"},
        [1, 2, 3],
        {"deep": {"x": 1}},
        [],
    ], {"subtype": "every_type"}))
    # Array of objects with different keys per element
    events.append(emit("mixed_array", [
        {"a": 1},
        {"b": 2},
        {"a": 1, "b": 2},
        {"c": [1, 2]},
        {},
        {"a": None},
    ], {"subtype": "objects_varying_keys"}))
    # Array containing arrays (nested)
    events.append(emit("mixed_array", [
        [1, 2],
        [3, [4, 5]],
        [[6], [7, [8]]],
    ], {"subtype": "arrays_in_array"}))
    # Long mixed array
    items = []
    for i in range(40):
        if i % 4 == 0:
            items.append({"id": i, "v": i * 2})
        elif i % 4 == 1:
            items.append(f"str-{i}")
        elif i % 4 == 2:
            items.append(i)
        else:
            items.append(None)
    events.append(emit("mixed_array", items, {"subtype": "long_mixed_40"}))
    return events


def _nest(depth: int, val: object = "leaf") -> dict:
    if depth <= 0:
        return {"value": val}
    return {"level": depth, "child": _nest(depth - 1, val)}


def scenario_deep_nesting() -> list[dict]:
    events = []
    for depth in [5, 10, 15, 20]:
        events.append(emit("deep_nesting", _nest(depth), {"depth": depth}))
    # Array-deep nesting
    def nest_array(depth: int) -> list:
        if depth <= 0:
            return [42]
        return [nest_array(depth - 1)]
    for depth in [3, 6, 10]:
        events.append(emit("deep_nesting", nest_array(depth), {"depth": depth, "kind": "array"}))
    # Mixed obj+array deep nesting
    events.append(emit("deep_nesting", {
        "a": {"b": [{"c": {"d": [{"e": {"f": "deep"}}]}}]}
    }, {"kind": "mixed_path"}))
    return events


def scenario_wide_object() -> list[dict]:
    events = []
    for n in [10, 50, 100, 150]:
        obj = {f"key_{i:04d}": i for i in range(n)}
        events.append(emit("wide_object", obj, {"n_keys": n}))
    # Wide with varied value types
    obj = {}
    for i in range(80):
        if i % 4 == 0:
            obj[f"int_key_{i}"] = i
        elif i % 4 == 1:
            obj[f"str_key_{i}"] = f"value-{i}"
        elif i % 4 == 2:
            obj[f"bool_key_{i}"] = i % 2 == 0
        else:
            obj[f"null_key_{i}"] = None
    events.append(emit("wide_object", obj, {"n_keys": 80, "subtype": "mixed_types"}))
    return events


def scenario_sparse_schema() -> list[dict]:
    """Same 20-key 'type' but each event only sets a random subset of keys."""
    all_keys = [f"field_{i:02d}" for i in range(20)]
    events = []
    for event_idx in range(25):
        k = RNG.randint(1, len(all_keys))
        chosen = RNG.sample(all_keys, k)
        obj = {key: event_idx * 10 + i for i, key in enumerate(chosen)}
        events.append(emit("sparse_schema", obj, {"event_idx": event_idx, "n_present": k}))
    return events


def scenario_long_strings() -> list[dict]:
    events = []
    for length in [100, 500, 1000, 2000, 5000]:
        s = "abcdefghij" * (length // 10) + "x" * (length % 10)
        events.append(emit("long_strings", {"value": s, "length": length}, {"subtype": "repeated"}))
    # Long string with spaces (wraps differently in TUI)
    for length in [200, 800]:
        s = " ".join(["word"] * (length // 5))
        events.append(emit("long_strings", {"value": s}, {"subtype": "spaced", "approx_len": length}))
    # Long key names
    events.append(emit("long_strings", {
        "a_very_long_key_name_that_exceeds_normal_display_width_x" * 2: "short_val",
        "normal_key": "normal_val",
    }, {"subtype": "long_key_name"}))
    # Deeply nested long strings
    events.append(emit("long_strings", {
        "outer": {"inner": {"deepest": "Z" * 500}}
    }, {"subtype": "nested_long"}))
    return events


def scenario_unicode_content() -> list[dict]:
    events = []
    # Emoji
    events.append(emit("unicode", {
        "emoji": "🎉🔥💯🚀✨🎸🌈🦄👾🤖",
        "flag": "🇦🇺🇯🇵🇺🇸",
        "family": "👨‍👩‍👧‍👦",
    }))
    # RTL text
    events.append(emit("unicode", {
        "arabic": "مرحبا بالعالم",
        "hebrew": "שלום עולם",
        "mixed": "Hello مرحبا World",
    }))
    # Zero-width and combining
    events.append(emit("unicode", {
        "zwsp": "word\u200bwith\u200bzero\u200bwidth",
        "combining": "e\u0301 a\u0300 n\u0303",
        "bom": "\ufeffstart",
    }))
    # Escaped/control chars in strings
    events.append(emit("unicode", {
        "tab": "a\tb",
        "newline_escaped": "a\\nb",
        "backslash": "a\\\\b",
        "quote": 'a"b',
    }))
    # Non-ASCII key names
    events.append(emit("unicode", {
        "café": 1,
        "naïve": 2,
        "Ünîcödé": 3,
    }))
    # Very long unicode string
    events.append(emit("unicode", {
        "long_emoji": "🔥" * 100,
        "long_cjk": "日本語テスト" * 50,
    }))
    return events


def scenario_numeric_extremes() -> list[dict]:
    events = []
    values = {
        "max_safe_int": 2**53 - 1,
        "min_safe_int": -(2**53 - 1),
        "large_int": 10**15,
        "negative": -999999,
        "zero": 0,
        "one": 1,
        "neg_one": -1,
        "small_float": 1e-308,
        "large_float": 1e+308,
        "neg_float": -1.23456789e+100,
        "precision_float": 1.0000000000000002,
        "integer_float": 1.0,
    }
    events.append(emit("numeric_extremes", values))
    # Array of mixed numerics
    events.append(emit("numeric_extremes", [
        0, 1, -1, 2**31, -(2**31), 1e10, -1e10, 1e-10, 1.5, -1.5
    ], {"subtype": "array"}))
    # Nested numerics
    events.append(emit("numeric_extremes", {
        "a": {"b": {"c": 2**53}},
        "list": [1e300, -1e300, 0.0, 1.0],
    }, {"subtype": "nested"}))
    return events


def scenario_array_of_arrays() -> list[dict]:
    """Specifically stress the array-of-arrays paths that triggered recent bugs."""
    events = []
    # 2D
    events.append(emit("array_of_arrays", [[1, 2, 3], [4, 5, 6], [7, 8, 9]], {"dim": "2d_3x3"}))
    # Jagged
    events.append(emit("array_of_arrays", [[1], [1, 2], [1, 2, 3], [], [1, 2, 3, 4]], {"dim": "jagged"}))
    # 3D
    events.append(emit("array_of_arrays", [[[1, 2], [3, 4]], [[5, 6], [7, 8]]], {"dim": "3d_2x2x2"}))
    # Mixed depth
    events.append(emit("array_of_arrays", [1, [2, 3], [[4, 5], 6], [[[7]]]], {"dim": "mixed_depth"}))
    # Array containing objects that contain arrays
    events.append(emit("array_of_arrays", [
        {"id": 0, "tags": ["a", "b"], "scores": [1.0, 2.0]},
        {"id": 1, "tags": [], "scores": [3.0]},
        {"id": 2, "tags": ["c"], "scores": []},
    ], {"dim": "objects_with_arrays"}))
    # Large 2D
    matrix = [[i * 10 + j for j in range(8)] for i in range(12)]
    events.append(emit("array_of_arrays", matrix, {"dim": "2d_12x8"}))
    # Empty inner arrays
    events.append(emit("array_of_arrays", [[], [], []], {"dim": "all_empty"}))
    events.append(emit("array_of_arrays", [[], [1], [], [2, 3], []], {"dim": "sparse_inner"}))
    # Array of arrays of arrays of scalars
    events.append(emit("array_of_arrays", [[[i + j + k for k in range(3)] for j in range(3)] for i in range(3)], {"dim": "3d_3x3x3"}))
    return events


def scenario_homogenous_long_array() -> list[dict]:
    """Large arrays of identical-shape objects (stresses array preview scrolling)."""
    events = []
    for n in [20, 50, 100, 200]:
        items = [{"id": i, "value": f"item-{i}", "score": i * 0.5, "active": i % 2 == 0} for i in range(n)]
        events.append(emit("homogenous_long_array", items, {"n": n}))
    # Items with nested objects
    items = [{"id": i, "meta": {"group": i % 5, "rank": i}, "tags": [f"t{i % 3}"]} for i in range(60)]
    events.append(emit("homogenous_long_array", items, {"n": 60, "subtype": "nested"}))
    return events


def scenario_heterogeneous_array() -> list[dict]:
    """Array where each element has a different structure."""
    events = []
    items = [
        {"type": "a", "x": 1},
        {"type": "b", "x": 1, "y": 2},
        {"type": "c", "x": 1, "y": 2, "z": 3},
        {"type": "d", "nested": {"p": 1}},
        {"type": "e", "list": [1, 2]},
        {"type": "f"},
        1,
        "string",
        None,
        [1, 2, 3],
    ]
    events.append(emit("heterogeneous_array", items, {"n": len(items)}))
    # Growing schema within array
    growing = [{f"k{j}": j for j in range(i + 1)} for i in range(10)]
    events.append(emit("heterogeneous_array", growing, {"subtype": "growing_schema"}))
    return events


def scenario_top_level_null_fields() -> list[dict]:
    events = []
    # Object where most fields are null
    obj = {f"field_{i:02d}": (None if i % 3 != 0 else i) for i in range(30)}
    events.append(emit("null_heavy", obj, {"subtype": "mostly_null"}))
    # All null
    events.append(emit("null_heavy", {f"k{i}": None for i in range(20)}, {"subtype": "all_null"}))
    # Nullable nested
    events.append(emit("null_heavy", {
        "a": None,
        "b": {"x": None, "y": None},
        "c": [None, None, 1, None],
        "d": {"deep": {"deeper": None}},
    }, {"subtype": "nested_nulls"}))
    return events


def scenario_schema_drift() -> list[dict]:
    """Events that share a base shape but gain/lose one key per event."""
    events = []
    base = {"id": 0, "name": "base", "score": 0.0}
    extra_keys = [f"ext_{i}" for i in range(12)]
    # Add one key per event
    for i, key in enumerate(extra_keys):
        obj = dict(base)
        obj["id"] = i
        obj[key] = f"val-{i}"
        events.append(emit("schema_drift", obj, {"kind": "add", "key": key}))
    # Remove one key per event (start with all, peel off one by one)
    all_keys_obj = dict(base)
    for k in extra_keys:
        all_keys_obj[k] = f"full-{k}"
    for i, key in enumerate(extra_keys):
        obj = dict(all_keys_obj)
        del obj[key]
        obj["id"] = 100 + i
        events.append(emit("schema_drift", obj, {"kind": "remove", "key": key}))
    return events


def scenario_single_key() -> list[dict]:
    events = []
    for key, val in [
        ("x", 1),
        ("x", "string"),
        ("x", None),
        ("x", []),
        ("x", {}),
        ("x", [1, 2]),
        ("x", {"nested": True}),
        ("different_key", 42),
    ]:
        events.append(emit("single_key", {key: val}))
    return events


def scenario_type_collision() -> list[dict]:
    """Same structural shape but different value types — tests type identity logic."""
    events = []
    # Same keys, same structure, but values alternate between int/string/null
    for i in range(10):
        if i % 3 == 0:
            obj = {"a": i, "b": i * 2, "c": True}
        elif i % 3 == 1:
            obj = {"a": f"str-{i}", "b": f"str-{i*2}", "c": False}
        else:
            obj = {"a": None, "b": None, "c": None}
        events.append(emit("type_collision", obj, {"variant": i % 3}))
    return events


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

SCENARIOS = [
    scenario_empty_values,
    scenario_scalar_array,
    scenario_mixed_array,
    scenario_deep_nesting,
    scenario_wide_object,
    scenario_sparse_schema,
    scenario_long_strings,
    scenario_unicode_content,
    scenario_numeric_extremes,
    scenario_array_of_arrays,
    scenario_homogenous_long_array,
    scenario_heterogeneous_array,
    scenario_top_level_null_fields,
    scenario_schema_drift,
    scenario_single_key,
    scenario_type_collision,
]


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate shape corpus JSONL for UI regression testing")
    parser.add_argument(
        "--output",
        default="/tmp/json_demo/shape-corpus.jsonl",
        help="output JSONL path (default: /tmp/json_demo/shape-corpus.jsonl)",
    )
    parser.add_argument(
        "--scenarios",
        nargs="*",
        metavar="SCENARIO",
        help="run only these scenario names (default: all)",
    )
    args = parser.parse_args()

    scenario_map = {fn.__name__.removeprefix("scenario_"): fn for fn in SCENARIOS}

    if args.scenarios:
        unknown = set(args.scenarios) - set(scenario_map)
        if unknown:
            raise SystemExit(f"unknown scenarios: {sorted(unknown)}\navailable: {sorted(scenario_map)}")
        chosen = [(name, scenario_map[name]) for name in args.scenarios]
    else:
        chosen = list(scenario_map.items())

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    total = 0
    with out_path.open("w", encoding="utf-8") as f:
        for name, fn in chosen:
            events = fn()
            for ev in events:
                f.write(json.dumps(ev, ensure_ascii=False, separators=(",", ":")))
                f.write("\n")
            print(f"  {name}: {len(events)} events")
            total += len(events)

    print(f"\nwrote {total} events to {out_path}")
    print(f"\navailable scenarios: {', '.join(scenario_map)}")


if __name__ == "__main__":
    main()
