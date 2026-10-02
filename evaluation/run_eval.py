"""Score the detector against the labelled evaluation set.

    python evaluation/run_eval.py                       # rules only
    python evaluation/run_eval.py --scorer detoxify     # rules + model
    python evaluation/run_eval.py --errors              # list every miss / false alarm
    python evaluation/run_eval.py --out evaluation/results/rules.json

Metrics (each message is labelled harmful or safe):
  catch rate        harmful messages flagged
  sent to review    harmful messages at least sent to a human (flagged or review)
  false alarms      safe messages flagged
  review load       safe messages sent to review (not wrong, but costs moderator time)
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT.parent))

from modstream.detector import Detector, load_scorer  # noqa: E402


def load(path: Path) -> list[dict]:
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def pct(n: int, d: int) -> str:
    return f"{100 * n / d:5.1f}%" if d else "    - "


def evaluate(detector: Detector, rows: list[dict]) -> dict:
    started = time.perf_counter()
    results = detector.analyze_many([r["text"] for r in rows])
    elapsed = time.perf_counter() - started

    by_tag: dict[str, dict] = defaultdict(lambda: {"label": None, "n": 0, "flagged": 0, "review": 0, "safe": 0})
    errors = []
    for row, res in zip(rows, results, strict=True):
        t = by_tag[row["tag"]]
        t["label"] = row["label"]
        t["n"] += 1
        t[res.verdict] += 1
        wrong = (row["label"] == "harmful" and res.verdict != "flagged") or (
            row["label"] == "safe" and res.verdict != "safe"
        )
        if wrong:
            errors.append({**row, "verdict": res.verdict, "matched": [m.term for m in res.matches],
                           "reasons": res.reasons})

    harmful = [r for r in zip(rows, results, strict=True) if r[0]["label"] == "harmful"]
    safe = [r for r in zip(rows, results, strict=True) if r[0]["label"] == "safe"]
    count = lambda pairs, *verdicts: sum(res.verdict in verdicts for _, res in pairs)  # noqa: E731
    return {
        "engine": detector.engine,
        "messages": len(rows),
        "harmful": len(harmful),
        "safe": len(safe),
        "catch_rate": count(harmful, "flagged") / len(harmful),
        "sent_to_review": count(harmful, "flagged", "review") / len(harmful),
        "false_alarm_rate": count(safe, "flagged") / len(safe),
        "review_load": count(safe, "review") / len(safe),
        "ms_per_message": round(1000 * elapsed / len(rows), 3),
        "by_tag": dict(by_tag),
        "errors": errors,
    }


def report(m: dict, show_errors: bool) -> None:
    print(f"\nEngine: {m['engine']}   ({m['messages']} messages: {m['harmful']} harmful, {m['safe']} safe)")
    print(f"  catch rate      {100 * m['catch_rate']:5.1f}%   harmful messages flagged")
    print(f"  sent to review  {100 * m['sent_to_review']:5.1f}%   harmful messages flagged or sent to review")
    print(f"  false alarms    {100 * m['false_alarm_rate']:5.1f}%   safe messages flagged")
    print(f"  review load     {100 * m['review_load']:5.1f}%   safe messages sent to review")
    print(f"  speed           {m['ms_per_message']} ms per message\n")

    print(f"  {'category':<22} {'label':<8} {'n':>3}  {'flagged':>8} {'review':>8} {'safe':>8}")
    for tag, t in m["by_tag"].items():
        print(f"  {tag:<22} {t['label']:<8} {t['n']:>3}  {pct(t['flagged'], t['n']):>8} "
              f"{pct(t['review'], t['n']):>8} {pct(t['safe'], t['n']):>8}")

    if show_errors:
        print(f"\nMistakes ({len(m['errors'])}):")
        for e in m["errors"]:
            kind = "MISSED " if e["label"] == "harmful" else "FALSE  "
            print(f"  {kind} [{e['tag']}] {e['verdict']:<8} {e['text']!r}  matched={e['matched']}")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dataset", default=str(ROOT / "dataset.jsonl"))
    p.add_argument("--scorer", default="none", choices=["none", "detoxify", "auto"])
    p.add_argument("--errors", action="store_true", help="list every mistake")
    p.add_argument("--out", help="write the full metrics JSON here")
    args = p.parse_args()

    detector = Detector(load_scorer(args.scorer))
    metrics = evaluate(detector, load(Path(args.dataset)))
    report(metrics, args.errors)
    if args.out:
        Path(args.out).write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
