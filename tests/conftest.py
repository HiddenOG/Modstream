import pytest

from cybershield import create_app
from cybershield.detector import Detector


class FakeScorer:
    """Deterministic stand-in for the transformer model."""

    name = "fake"
    ready = True

    def __init__(self):
        self.calls = 0

    def score(self, texts):
        self.calls += 1
        out = []
        for t in texts:
            if "TOXIC" in t:
                out.append({"toxicity": 0.95, "insult": 0.8})
            elif "MEH" in t:
                out.append({"toxicity": 0.5, "insult": 0.1})
            else:
                out.append({"toxicity": 0.02, "insult": 0.01})
        return out


@pytest.fixture
def scorer():
    return FakeScorer()


@pytest.fixture
def detector(scorer):
    return Detector(scorer)


@pytest.fixture
def app(tmp_path, detector):
    return create_app({
        "TESTING": True,
        "SECRET_KEY": "test",
        "DATABASE": str(tmp_path / "test.db"),
        "UPLOAD_FOLDER": str(tmp_path / "uploads"),
        "DETECTOR": detector,
        "STREAM_MAX_SECONDS": 0,
        "STREAM_POLL_SECONDS": 0,
    })


@pytest.fixture
def client(app):
    return app.test_client()
