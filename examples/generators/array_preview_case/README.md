# array_preview_case

Generate JSONL with long arrays of mostly similar objects, sparse late-appearing keys, and deliberately long string fields that wrap in the preview pane.
It also includes a mixed-type `scalar_items` array for testing scalar array selection and exact-match behavior.

```bash
python3 examples/generators/array_preview_case/generate_array_preview_case.py
```

To exaggerate wrapping drift:

```bash
python3 examples/generators/array_preview_case/generate_array_preview_case.py --wrap-len 320
```

Default output:

- `/tmp/json_demo/array-preview-case.jsonl`

Useful for testing:

- preview scrolling through repeated array objects
- selection stability for indexed array entries
- discovery of keys that appear late in long arrays
- wrapped-string preview centering drift while moving deeper into the object

```bash
./target/release/json-analyzer --jsonl /tmp/json_demo/array-preview-case.jsonl --offline
```

Suggested manual check:

- Narrow your terminal so `wrapped_note`, `wrap_probe.header`, and `wrap_probe.footer` visibly wrap.
- Enter JSON focus and move down through deeper `payload.items[n].wrapped_note` and later fields.
- If the bug is present, the selected row will drift lower and lower instead of staying near mid-screen.
