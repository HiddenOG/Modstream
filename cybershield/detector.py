"""Hybrid harmful-content detector: rule layer + transformer layer + policy.

Pipeline for each text:

1. **Normalise** common character substitutions ("1d10t" -> "idiot") with a
   1:1 translation so match offsets still line up with the original text.
2. **Lexicon** match with word boundaries (no more "made" matching "mad").
3. **Model** scoring via a pluggable ``Scorer`` (Detoxify by default).
4. **Policy** combines both signals into ``safe`` / ``review`` / ``flagged``.
"""

from __future__ import annotations

import importlib.util
import logging
import re
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Protocol

from . import lexicon

log = logging.getLogger(__name__)

# Model label -> unified category shown in the UI and stats.
MODEL_CATEGORY = {
    "toxicity": "toxicity",
    "severe_toxicity": "toxicity",
    "obscene": "profanity",
    "insult": "harassment",
    "identity_attack": "hate",
    "identity_hate": "hate",
    "threat": "threat",
    "sexual_explicit": "profanity",
}

_LEET = str.maketrans("013457@$", "oieastas")


@dataclass(frozen=True)
class Match:
    term: str
    category: str
    tier: str  # "strong" | "contextual"
    start: int
    end: int


@dataclass
class Analysis:
    verdict: str  # "safe" | "review" | "flagged"
    risk: float
    categories: list[str]
    reasons: list[str]
    matches: list[Match]
    model_scores: dict[str, float] | None
    engine: str
    latency_ms: float = 0.0
    chars: int = field(default=0)

    @property
    def flagged(self) -> bool:
        return self.verdict == "flagged"

    def to_dict(self) -> dict:
        # Hand-written: dataclasses.asdict deep-copies recursively and showed up in profiles.
        return {
            "verdict": self.verdict, "risk": self.risk, "categories": self.categories, "reasons": self.reasons,
            "matches": [{"term": m.term, "category": m.category, "tier": m.tier, "start": m.start, "end": m.end}
                        for m in self.matches],
            "model_scores": self.model_scores, "engine": self.engine, "latency_ms": self.latency_ms,
            "chars": self.chars,
        }


# --- Scorers -----------------------------------------------------------------

class Scorer(Protocol):
    name: str

    def score(self, texts: list[str]) -> list[dict[str, float]]: ...


class NullScorer:
    """Lexicon-only mode, used when ML dependencies are not installed."""

    name = "lexicon-only"
    ready = True

    def score(self, texts: list[str]) -> list[dict[str, float]]:
        return [{} for _ in texts]


class DetoxifyScorer:
    """Lazily loads a Detoxify model (a BERT fine-tuned on Jigsaw data)."""

    def __init__(self, variant: str = "original"):
        self.name = f"detoxify-{variant}"
        self.variant = variant
        self._model = None
        self._lock = threading.Lock()

    @property
    def ready(self) -> bool:
        return self._model is not None

    def load(self):
        if self._model is None:
            with self._lock:
                if self._model is None:
                    from detoxify import Detoxify

                    started = time.perf_counter()
                    self._model = Detoxify(self.variant)
                    log.info("Loaded %s in %.1fs", self.name, time.perf_counter() - started)
        return self._model

    def score(self, texts: list[str]) -> list[dict[str, float]]:
        raw = self.load().predict(texts)
        return [
            {label: round(float(values[i]), 4) for label, values in raw.items()}
            for i in range(len(texts))
        ]


def load_scorer(kind: str = "auto") -> Scorer:
    if kind == "none":
        return NullScorer()
    if kind in ("auto", "detoxify"):
        if importlib.util.find_spec("detoxify") is not None:
            return DetoxifyScorer()
        if kind == "detoxify":
            raise RuntimeError("CYBERSHIELD_SCORER=detoxify but detoxify is not installed")
        log.warning("detoxify not installed; running in lexicon-only mode")
        return NullScorer()
    raise ValueError(f"Unknown scorer: {kind!r}")


# --- Lexicon -----------------------------------------------------------------

def _term_pattern(term: str) -> str:
    words = re.split(r"[\s\-]+", term)
    body = r"[\s\-_.]*".join(re.escape(w) for w in words)
    # Allow simple plurals on single words ("idiots", "losers").
    return body + (r"(?:e?s)?" if len(words) == 1 else "")


def _build_matcher() -> tuple[re.Pattern, dict[str, tuple[str, str, str]]]:
    """Compile every term into ONE alternation so each text is scanned once
    (~150 separate regex passes per message was the throughput bottleneck).

    Alternatives are ordered longest-first: at any position the longest term
    wins ("suicide bomber" over "bomb"), and finditer never returns overlaps."""
    rules = {}
    for tier, table in (("strong", lexicon.STRONG), ("contextual", lexicon.CONTEXTUAL)):
        for category, terms in table.items():
            for term in terms:
                rules.setdefault(term, (category, tier))
    ordered = sorted(rules, key=len, reverse=True)
    groups = {f"t{i}": (term, *rules[term]) for i, term in enumerate(ordered)}
    alternation = "|".join(f"(?P<{name}>{_term_pattern(term)})" for name, (term, *_rest) in groups.items())
    return re.compile(rf"(?<![a-z0-9])(?:{alternation})(?![a-z0-9])", re.IGNORECASE), groups


_MATCHER, _GROUPS = _build_matcher()


def find_matches(text: str) -> list[Match]:
    out = []
    for m in _MATCHER.finditer(text.translate(_LEET)):
        term, category, tier = _GROUPS[m.lastgroup]
        out.append(Match(term, category, tier, m.start(), m.end()))
    return out


# --- Detector ----------------------------------------------------------------

class _LRU:
    def __init__(self, size: int):
        self.size, self.data, self.lock = size, OrderedDict(), threading.Lock()

    def get(self, key):
        with self.lock:
            if key in self.data:
                self.data.move_to_end(key)
                return self.data[key]
        return None

    def put(self, key, value):
        with self.lock:
            self.data[key] = value
            self.data.move_to_end(key)
            if len(self.data) > self.size:
                self.data.popitem(last=False)


class Detector:
    def __init__(self, scorer: Scorer, flag_threshold=0.7, review_threshold=0.4, cache_size=2048):
        self.scorer = scorer
        self.flag_threshold = flag_threshold
        self.review_threshold = review_threshold
        self._cache = _LRU(cache_size)

    @property
    def engine(self) -> str:
        return self.scorer.name

    def analyze(self, text: str) -> Analysis:
        return self.analyze_many([text])[0]

    def analyze_many(self, texts: list[str]) -> list[Analysis]:
        started = time.perf_counter()
        scores: list[dict | None] = [self._cache.get(t) for t in texts]
        missing = [i for i, s in enumerate(scores) if s is None]
        if missing:
            fresh = self.scorer.score([texts[i] for i in missing])
            for i, s in zip(missing, fresh, strict=True):
                scores[i] = s
                self._cache.put(texts[i], s)
        per_item_ms = (time.perf_counter() - started) * 1000 / max(len(texts), 1)
        return [self._decide(t, s, per_item_ms) for t, s in zip(texts, scores, strict=True)]

    def _decide(self, text: str, scores: dict, latency_ms: float) -> Analysis:
        matches = find_matches(text)
        strong = [m for m in matches if m.tier == "strong"]
        contextual = [m for m in matches if m.tier == "contextual"]
        model_max = max(scores.values(), default=0.0)

        reasons = []
        if strong:
            reasons.append("Matched high-severity terms: " + ", ".join(sorted({m.term for m in strong})))
        if contextual:
            terms = ", ".join(sorted({m.term for m in contextual}))
            reasons.append(f"Matched context-dependent terms: {terms}")
        if scores and model_max >= self.review_threshold:
            label = max(scores, key=scores.get)
            reasons.append(f"Model scored '{label.replace('_', ' ')}' at {model_max:.0%}")

        if strong or model_max >= self.flag_threshold:
            verdict = "flagged"
        elif contextual and model_max >= self.review_threshold:
            verdict = "flagged"
            reasons.append("Context-dependent term confirmed by the model")
        elif contextual or model_max >= self.review_threshold:
            verdict = "review"
        else:
            verdict = "safe"

        risk = max(model_max, 0.92 if strong else 0.0, 0.45 if contextual else 0.0)
        if verdict == "flagged":
            risk = max(risk, self.flag_threshold)

        categories = {m.category for m in matches}
        categories |= {
            MODEL_CATEGORY.get(label, label)
            for label, value in scores.items()
            if value >= self.review_threshold
        }
        return Analysis(
            verdict=verdict,
            risk=round(min(risk, 1.0), 4),
            categories=sorted(categories),
            reasons=reasons,
            matches=matches,
            model_scores=scores or None,
            engine=self.engine,
            latency_ms=round(latency_ms, 1),
            chars=len(text),
        )

    def warmup(self) -> None:
        if hasattr(self.scorer, "load"):
            self.scorer.load()
            self.scorer.score(["warm-up"])
