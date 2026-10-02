"""Score the detector against the labelled evaluation set.

    python evaluation/run_eval.py                       # rules only
    python evaluation/run_eval.py --scorer detoxify     # rules vs model vs both, plus speed and memory
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
import os
import sys
import time
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT.parent))

from modstream.detector import Detector, NullScorer, load_scorer  # noqa: E402


def load(path: Path) -> list[dict]:
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def pct(n: int, d: int) -> str:
    return f"{100 * n / d:5.1f}%" if d else "    - "


def score(name: str, rows: list[dict], verdicts: list[str], notes: list[str] | None = None) -> dict:
    """Aggregate verdicts against labels into the headline metrics, per-category table and mistakes."""
    notes = notes or [""] * len(rows)
    by_tag: dict[str, dict] = defaultdict(lambda: {"label": None, "n": 0, "flagged": 0, "review": 0, "safe": 0})
    errors = []
    for row, verdict, note in zip(rows, verdicts, notes, strict=True):
        t = by_tag[row["tag"]]
        t["label"] = row["label"]
        t["n"] += 1
        t[verdict] += 1
        expected = "flagged" if row["label"] == "harmful" else "safe"
        if verdict != expected:
            errors.append({**row, "verdict": verdict, "note": note})

    def rate(label, *wanted):
        pairs = [v for r, v in zip(rows, verdicts, strict=True) if r["label"] == label]
        return sum(v in wanted for v in pairs) / len(pairs)

    return {
        "name": name,
        "messages": len(rows),
        "harmful": sum(r["label"] == "harmful" for r in rows),
        "safe": sum(r["label"] == "safe" for r in rows),
        "catch_rate": rate("harmful", "flagged"),
        "sent_to_review": rate("harmful", "flagged", "review"),
        "false_alarm_rate": rate("safe", "flagged"),
        "review_load": rate("safe", "review"),
        "by_tag": dict(by_tag),
        "errors": errors,
    }


def evaluate(detector: Detector, rows: list[dict]) -> dict:
    """Run the full detector (rules + whatever scorer it has) over the set."""
    started = time.perf_counter()
    results = detector.analyze_many([r["text"] for r in rows])
    elapsed = time.perf_counter() - started
    notes = [f"matched={[m.term for m in res.matches]} model={_top(res.model_scores)}" for res in results]
    metrics = score(detector.engine, rows, [res.verdict for res in results], notes)
    metrics["ms_per_message"] = round(1000 * elapsed / len(rows), 3)
    metrics["results"] = results
    return metrics


def _top(scores: dict | None) -> str:
    if not scores:
        return "-"
    label = max(scores, key=scores.get)
    return f"{label}={scores[label]:.2f}"


def model_only(rows: list[dict], results, flag: float, review: float) -> dict:
    """What the model would decide on its own, ignoring the rules."""
    verdicts = []
    for res in results:
        top = max((res.model_scores or {}).values(), default=0.0)
        verdicts.append("flagged" if top >= flag else "review" if top >= review else "safe")
    notes = [f"model={_top(res.model_scores)}" for res in results]
    return score("model only", rows, verdicts, notes)


def rss_mb() -> float | None:
    try:
        import psutil

        return psutil.Process(os.getpid()).memory_info().rss / 2**20
    except ImportError:
        return None


def benchmark(scorer, texts: list[str]) -> dict:
    """Milliseconds per message for one forward pass at several batch sizes."""
    out = {}
    for size in (1, 8, 32, 64):
        batch = (texts * (size // len(texts) + 1))[:size]
        scorer.score(batch)  # warm
        runs = max(3, 64 // size)
        started = time.perf_counter()
        for _ in range(runs):
            scorer.score(batch)
        out[size] = round(1000 * (time.perf_counter() - started) / (runs * size), 2)
    return out


def comparison(rows_of_metrics: list[dict]) -> None:
    print(f"\n  {'':<18} {'catch rate':>11} {'to review':>10} {'false alarms':>13} {'review load':>12}")
    for m in rows_of_metrics:
        print(f"  {m['name']:<18} {100 * m['catch_rate']:10.1f}% {100 * m['sent_to_review']:9.1f}% "
              f"{100 * m['false_alarm_rate']:12.1f}% {100 * m['review_load']:11.1f}%")


def categories(rows_of_metrics: list[dict]) -> None:
    names = [m["name"] for m in rows_of_metrics]
    print(f"\n  Share flagged per category    {''.join(f'{n:>16}' for n in names)}")
    for tag, t in rows_of_metrics[0]["by_tag"].items():
        cells = "".join(f"{pct(m['by_tag'][tag]['flagged'], t['n']):>16}" for m in rows_of_metrics)
        print(f"  {tag:<22} {t['label']:<7}{cells}")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dataset", default=str(ROOT / "dataset.jsonl"))
    p.add_argument("--scorer", default="none", choices=["none", "detoxify", "auto"])
    p.add_argument("--variant", default="unbiased", choices=["unbiased", "original", "multilingual"])
    p.add_argument("--errors", action="store_true", help="list every mistake of the full detector")
    p.add_argument("--out", help="write the metrics JSON here")
    args = p.parse_args()
    rows = load(Path(args.dataset))
    print(f"{len(rows)} messages: {sum(r['label'] == 'harmful' for r in rows)} harmful, "
          f"{sum(r['label'] == 'safe' for r in rows)} safe")

    rules = evaluate(Detector(NullScorer()), rows)
    rules["name"] = "rules only"
    table, perf = [rules], {"rules_ms_per_message": rules["ms_per_message"]}
    final = rules

    if args.scorer != "none":
        scorer = load_scorer(args.scorer, args.variant)
        before = rss_mb()
        started = time.perf_counter()
        if hasattr(scorer, "load"):
            scorer.load()
        perf["model"] = scorer.name
        perf["load_seconds"] = round(time.perf_counter() - started, 1)
        if before is not None:
            perf["model_memory_mb"] = round(rss_mb() - before)
        detector = Detector(scorer)
        both = evaluate(detector, rows)
        both["name"] = "rules + model"
        model = model_only(rows, both["results"], detector.flag_threshold, detector.review_threshold)
        table, final = [rules, model, both], both
        perf["ms_per_message_by_batch_size"] = benchmark(scorer, [r["text"] for r in rows])
        perf["process_memory_mb"] = round(rss_mb()) if before is not None else None

    comparison(table)
    categories(table)
    print(f"\n  Performance: {json.dumps(perf)}")

    if args.errors:
        print(f"\nMistakes by '{final['name']}' ({len(final['errors'])}):")
        for e in final["errors"]:
            kind = "MISSED" if e["label"] == "harmful" else "FALSE "
            print(f"  {kind} [{e['tag']}] {e['verdict']:<8} {e['text']!r}  {e['note']}")

    if args.out:
        for m in table:
            m.pop("results", None)
        Path(args.out).write_text(json.dumps({"variants": table, "performance": perf}, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
