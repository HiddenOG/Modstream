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

@dataclass(frozen=True)
class Match:
    term: str  # canonical term ("fuck" for "f*ck", "fuc", "f u c k")
    category: str
    tier: str  # "strong" | "contextual" | "mild"
    start: int  # offsets into the original text
    end: int
    directed: bool = False  # escalated because it was aimed at a person or stacked with another insult


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
            "matches": [{"term": m.term, "category": m.category, "tier": m.tier, "start": m.start, "end": m.end,
                         "directed": m.directed} for m in self.matches],
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
            raise RuntimeError("MODSTREAM_SCORER=detoxify but detoxify is not installed")
        log.warning("detoxify not installed; running in lexicon-only mode")
        return NullScorer()
    raise ValueError(f"Unknown scorer: {kind!r}")


# --- Lexicon -----------------------------------------------------------------

_MASKS = "*#"
_SUFFIXES = r"(?:e?s|ing|in|ed|er|ers|y)?"


def _word_pattern(word: str) -> str:
    """One word, tolerant of stretched letters ("fuuuck") and, for longer words,
    masked interior letters ("f*ck", "f**k")."""
    parts = []
    for i, ch in enumerate(word):
        if not ch.isalnum():
            parts.append(re.escape(ch) + "?")  # optional apostrophe etc.
        elif 0 < i < len(word) - 1 and len(word) >= 4:
            parts.append(f"(?:{re.escape(ch)}|[{re.escape(_MASKS)}])+")
        else:
            parts.append(f"{re.escape(ch)}+")
    return "".join(parts)


def _term_pattern(term: str, category: str) -> str:
    words = re.split(r"[\s\-]+", term)
    body = r"[\s\-_.]*".join(_word_pattern(w) for w in words)
    if len(words) > 1:
        return body
    return body + (_SUFFIXES if category in lexicon.INFLECTED else r"(?:e?s)?")


def _build_matcher() -> tuple[re.Pattern, dict[str, tuple[str, str, str]]]:
    """Compile every term (and known misspelling) into ONE alternation so each
    text is scanned once (~150 separate regex passes per message was the
    throughput bottleneck).

    Alternatives are ordered longest-first: at any position the longest term
    wins ("suicide bomber" over "bomb"), and finditer never returns overlaps."""
    rules: dict[str, tuple[str, str, str]] = {}  # spelling -> (canonical term, category, tier)
    for tier, table in (("strong", lexicon.STRONG), ("contextual", lexicon.CONTEXTUAL), ("mild", lexicon.MILD)):
        for category, terms in table.items():
            for term in terms:
                rules.setdefault(term, (term, category, tier))
    for canonical, spellings in lexicon.VARIANTS.items():
        _, category, tier = rules[canonical]
        for spelling in spellings:
            rules.setdefault(spelling, (canonical, category, tier))
    ordered = sorted(rules, key=len, reverse=True)
    groups = {f"t{i}": rules[spelling] for i, spelling in enumerate(ordered)}
    alternation = "|".join(
        f"(?P<t{i}>{_term_pattern(spelling, rules[spelling][1])})" for i, spelling in enumerate(ordered)
    )
    return re.compile(rf"(?<![a-z0-9])(?:{alternation})(?![a-z0-9])", re.IGNORECASE), groups


_MATCHER, _GROUPS = _build_matcher()

# Runs of 3+ single characters separated by one separator: "f u c k", "k.y.s", "i-d-i-o-t".
_SPACED = re.compile(r"(?<![a-z0-9])(?:[a-z0-9$@!][\s.\-_*]){2,}[a-z0-9$@!](?![a-z0-9])", re.IGNORECASE)
_LEET_ANY = {"0": "o", "1": "i", "3": "e", "4": "a", "5": "s", "7": "t", "@": "a", "$": "s"}
_LEET_INNER = {"!": "i", "|": "i"}  # only between two letters, so "idiot!" stays "idiot!"
_TOKEN = re.compile(r"[a-z0-9']+")
_THREAT = re.compile(
    r"(?<![a-z0-9])(?:" + "|".join(re.escape(s) for s in sorted(lexicon.THREAT_SUBJECTS, key=len, reverse=True))
    + r")(?![a-z0-9])"
)


def normalize(text: str) -> tuple[str, list[int]]:
    """Undo common evasions. Returns the normalized text and, for each of its
    characters, the index of the original character it came from, so matches
    can be highlighted in the text the user actually wrote."""
    drop: set[int] = set()
    for run in _SPACED.finditer(text):
        drop.update(i for i in range(run.start(), run.end()) if (i - run.start()) % 2 == 1)

    chars, index = [], []
    for i, ch in enumerate(text):
        if i not in drop:
            chars.append(ch.lower() if len(ch.lower()) == 1 else ch)
            index.append(i)

    def is_letter(j: int) -> bool:
        return 0 <= j < len(chars) and chars[j].isalpha()

    for j, ch in enumerate(chars):
        if ch in _LEET_ANY and (is_letter(j - 1) or is_letter(j + 1)):
            chars[j] = _LEET_ANY[ch]
        elif ch in _LEET_INNER and is_letter(j - 1) and is_letter(j + 1):
            chars[j] = _LEET_INNER[ch]
    return "".join(chars), index


def _tokens(norm: str) -> list[tuple[str, int, int]]:
    return [(t.group().replace("'", ""), t.start(), t.end()) for t in _TOKEN.finditer(norm)]


def _aimed_at_reader(tokens, first: int, last: int) -> bool:
    """A target word within 3 tokens before or 2 after: "you're so dumb", "fuck you", "your mom is a hoe"."""
    window = range(max(0, first - 3), min(len(tokens), last + 3))
    for k in window:
        if first <= k <= last:
            continue
        word = tokens[k][0]
        if word in lexicon.TARGETS:
            return True
        if word in lexicon.FAMILY and k > 0 and tokens[k - 1][0] in lexicon.OWNERS:
            return True
    return False


def _self_talk(tokens, first: int) -> bool:
    return any(tokens[k][0] in lexicon.SELF for k in range(max(0, first - 3), first))


def _threats(norm: str, tokens) -> list[tuple[int, int]]:
    """<subject> + violent verb within 5 tokens + target within 2 more: "i'm going to find you and hurt you"."""
    spans = []
    for subject in _THREAT.finditer(norm):
        after = [k for k, t in enumerate(tokens) if t[1] >= subject.end()][:6]
        for pos, k in enumerate(after):
            if tokens[k][0] in lexicon.VIOLENT_VERBS:
                for j in after[pos + 1:pos + 3]:
                    if tokens[j][0] in lexicon.TARGETS or tokens[j][0] in lexicon.OWNERS:
                        spans.append((subject.start(), tokens[j][2]))
                        break
                break
    return spans


def find_matches(text: str) -> list[Match]:
    norm, index = normalize(text)
    tokens = _tokens(norm)

    def token_range(start: int, end: int) -> tuple[int, int]:
        inside = [k for k, t in enumerate(tokens) if t[1] < end and t[2] > start]
        return (inside[0], inside[-1]) if inside else (0, -1)

    found = []
    for m in _MATCHER.finditer(norm):
        term, category, tier = _GROUPS[m.lastgroup]
        found.append([term, category, tier, m.start(), m.end(), False])

    # Escalate insults and profanity aimed at a person, or stacked ("stupid bitch", "ugly cow").
    abusive = [(f, *token_range(f[3], f[4])) for f in found if f[1] in ("harassment", "profanity")]
    for f, first, last in abusive:
        if last < first:
            continue
        stacked = any(
            g is not f and (0 < g_first - last <= 2 or 0 < first - g_last <= 2)
            for g, g_first, g_last in abusive
        )
        aimed = _aimed_at_reader(tokens, first, last)
        if f[2] != "strong" and (aimed or stacked):
            f[2], f[5] = "strong", True
        elif f[2] == "strong" and f[1] == "harassment" and not aimed and _self_talk(tokens, first):
            f[2] = "contextual"  # "I'm such an idiot" is self-talk, not bullying

    for start, end in _threats(norm, tokens):
        if not any(f[3] < end and f[4] > start and f[1] == "threat" for f in found):
            found.append(["threat of violence", "threat", "strong", start, end, True])

    found.sort(key=lambda f: f[3])
    return [Match(term, category, tier, index[start], index[end - 1] + 1, directed)
            for term, category, tier, start, end, directed in found]


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
        mild = [m for m in matches if m.tier == "mild"]
        model_max = max(scores.values(), default=0.0)

        def names(ms):
            return ", ".join(sorted({m.term for m in ms}))

        reasons = []
        if directed := [m for m in strong if m.directed]:
            reasons.append(f"Insult or profanity aimed at a person: {names(directed)}")
        if listed := [m for m in strong if not m.directed]:
            reasons.append(f"Matched high-severity terms: {names(listed)}")
        if contextual:
            reasons.append(f"Matched context-dependent terms: {names(contextual)}")
        if mild and not strong and not contextual:
            reasons.append(f"Profanity not aimed at anyone ({names(mild)}): allowed")
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

        risk = max(model_max, 0.92 if strong else 0.0, 0.45 if contextual else 0.0, 0.1 if mild else 0.0)
        if verdict == "flagged":
            risk = max(risk, self.flag_threshold)

        categories = {m.category for m in matches if m.tier != "mild"}
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
