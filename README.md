# Example Usage

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Live Demo

Terminal 1:

```bash
python demo_source.py
```

Terminal 2:

```bash
python demo_analyzer.py
```

Terminal 3 (send actions):

```bash
python trigger.py login
python trigger.py purchase
python trigger.py search
```

## Replay Mode

```bash
python demo_analyzer.py \
  --replay-file /path/to/events.jsonl \
  --marks-file replay_marks.example.json
```
