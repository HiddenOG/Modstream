import pytest

from modstream.detector import Detector, NullScorer, find_matches


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


# --- Targeting, disguises and policy (rules only) -----------------------------------

@pytest.fixture
def rules():
    return Detector(NullScorer())


@pytest.mark.parametrize("text", ["fuck you", "fuck your mother", "ur mom is a hoe", "you dumb bitch",
                                  "screw you", "you're so stupid"])
def test_insults_aimed_at_a_person_are_flagged(rules, text):
    result = rules.analyze(text)
    assert result.verdict == "flagged"
    assert any(m.directed for m in result.matches)


@pytest.mark.parametrize("text", ["this traffic is fucking awful", "holy shit, we won!",
                                  "the hoe is in the garden shed", "what the fuck happened to the wifi"])
def test_profanity_not_aimed_at_anyone_is_allowed(rules, text):
    result = rules.analyze(text)
    assert result.verdict == "safe"
    assert any("not aimed at anyone" in r for r in result.reasons)


@pytest.mark.parametrize("text, shown", [
    ("f*ck you", "f*ck"), ("f**k you", "f**k"), ("fuuuuck you", "fuuuuck"), ("f u c k you", "f u c k"),
    ("fuc your mom", "fuc"), ("phuck you", "phuck"), ("b!tch please, you're nothing", "b!tch"),
])
def test_disguises_are_caught_and_highlighted_in_the_original_text(rules, text, shown):
    result = rules.analyze(text)
    assert result.verdict == "flagged"
    hit = next(m for m in result.matches if m.term == ("bitch" if "b!tch" in text else "fuck"))
    assert text[hit.start:hit.end] == shown


def test_stacked_insults_are_flagged(rules):
    assert rules.analyze("$tupid b1tch").verdict == "flagged"
    assert rules.analyze("ugly cow").verdict == "flagged"


def test_self_talk_is_not_bullying(rules):
    assert rules.analyze("I'm such an idiot, I left my wallet at home").verdict == "review"
    assert rules.analyze("you idiot").verdict == "flagged"


@pytest.mark.parametrize("text", ["i'm going to find you and hurt you", "we're going to bomb your house",
                                  "say that again and i'll stab you"])
def test_threat_pattern(rules, text):
    result = rules.analyze(text)
    assert result.verdict == "flagged" and "threat" in result.categories


def test_threat_pattern_ignores_banter(rules):
    assert rules.analyze("i'll find you a seat").verdict == "safe"


def test_spaced_letters_dont_swallow_normal_text(rules):
    assert rules.analyze("u r a b c and that's fine").verdict == "safe"
    assert rules.analyze("k y s").verdict == "flagged"


def test_evaluation_set_regression_guard():
    """Fails if a rule change makes the detector meaningfully worse on the labelled set."""
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "evaluation"))
    from run_eval import evaluate, load

    dataset = Path(__file__).resolve().parents[1] / "evaluation" / "dataset.jsonl"
    metrics = evaluate(Detector(NullScorer()), load(dataset))
    assert metrics["catch_rate"] >= 0.70, metrics["catch_rate"]
    assert metrics["false_alarm_rate"] <= 0.07, metrics["false_alarm_rate"]
