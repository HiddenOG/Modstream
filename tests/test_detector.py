import pytest

from cybershield.detector import Detector, NullScorer, find_matches


def terms(text):
    return [m.term for m in find_matches(text)]


@pytest.mark.parametrize("text", [
    "I made a cake",           # "mad" inside "made"
    "what a skillful player",  # "kill" inside "skillful"
    "classic assignment",      # "ass" inside words
    "the bombastic speech",    # "bomb" inside "bombastic"
])
def test_no_substring_false_positives(text):
    assert terms(text) == []


def test_word_boundaries_and_plurals():
    assert terms("you idiots") == ["idiot"]
    assert terms("LOSER!") == ["loser"]


def test_leetspeak_is_normalised_with_original_offsets():
    text = "ur an 1d10t"
    [m] = find_matches(text)
    assert m.term == "idiot"
    assert text[m.start:m.end] == "1d10t"


def test_multiword_terms_tolerate_spacing():
    assert terms("just  kill-yourself") == ["kill yourself"]


def test_longest_overlapping_match_wins():
    assert terms("a suicide bomber was arrested") == ["suicide bomber"]


def test_strong_term_flags_without_model(detector):
    result = detector.analyze("you are a loser")
    assert result.verdict == "flagged"
    assert "harassment" in result.categories


def test_contextual_term_alone_needs_review(detector):
    assert detector.analyze("that exam was stupid hard").verdict == "review"


def test_contextual_term_confirmed_by_model_flags(detector):
    assert detector.analyze("you are stupid MEH").verdict == "flagged"


def test_model_alone_can_flag(detector):
    result = detector.analyze("TOXIC words with no lexicon hits")
    assert result.verdict == "flagged"
    assert result.model_scores["toxicity"] == 0.95
    assert "harassment" in result.categories  # insult -> harassment


def test_safe_text(detector):
    result = detector.analyze("Great game today, proud of the team!")
    assert result.verdict == "safe"
    assert result.risk < 0.4
    assert result.reasons == []


def test_religious_terms_are_not_flagged(detector):
    assert detector.analyze("She studies sharia law and the meaning of jihad in theology").verdict == "safe"


def test_results_are_cached(detector, scorer):
    detector.analyze("hello there")
    detector.analyze("hello there")
    assert scorer.calls == 1


def test_batch_scores_in_one_call(detector, scorer):
    results = detector.analyze_many(["a", "b", "TOXIC c"])
    assert [r.verdict for r in results] == ["safe", "safe", "flagged"]
    assert scorer.calls == 1


def test_lexicon_only_mode():
    detector = Detector(NullScorer())
    assert detector.analyze("hello").verdict == "safe"
    assert detector.analyze("you idiot").verdict == "flagged"
    assert detector.analyze("hello").model_scores is None
