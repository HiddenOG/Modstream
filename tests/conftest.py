import asyncio
import os

import pytest
from fastapi.testclient import TestClient

from modstream import create_app
from modstream.config import Settings
from modstream.db import Base, make_engine
from modstream.detector import Detector
from modstream.services import Runtime


class FakeScorer:
    """Deterministic stand-in for the transformer model."""

    name = "fake"
    ready = True

    def __init__(self):
        self.calls = 0
        self.batch_sizes = []

    def score(self, texts):
        self.calls += 1
        self.batch_sizes.append(len(texts))
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


def _database_url(tmp_path) -> str:
    url = os.environ.get("MODSTREAM_TEST_DATABASE_URL")  # real Postgres in CI
    if url:
        async def reset():
            engine = make_engine(url)
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.drop_all)
            await engine.dispose()

        asyncio.run(reset())
        return url
    return f"sqlite+aiosqlite:///{(tmp_path / 'test.db').as_posix()}"


def _redis_url() -> str | None:
    url = os.environ.get("MODSTREAM_TEST_REDIS_URL")  # real Redis in CI
    if url:
        import redis

        redis.Redis.from_url(url).flushdb()
    return url


@pytest.fixture
def settings(tmp_path):
    return Settings(
        env="test",
        secret_key="test",
        database_url=_database_url(tmp_path),
        redis_url=_redis_url(),
        upload_dir=str(tmp_path / "uploads"),
        warmup=False,
        batch_max_wait_ms=2,
        worker_block_ms=50,
        stream_max_seconds=0.3,
        stream_heartbeat_s=0.1,
    )


@pytest.fixture
def client(settings, detector):
    with TestClient(create_app(settings, detector=detector)) as c:
        yield c


@pytest.fixture
async def runtime(settings, detector):
    rt = Runtime(settings, detector=detector)
    await rt.start(workers=0)
    yield rt
    await rt.stop()
