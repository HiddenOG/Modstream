import json

import pytest
import websockets

from modstream import sources


def post(i, lang="en"):
    return json.dumps({"did": f"did:{i}", "time_us": 1000 + i, "kind": "commit",
                       "commit": {"operation": "create", "record": {"text": f"post {i}", "langs": [lang]}}})


class FakeConnection:
    """Serves events after the requested cursor; optionally drops after ``drop_after`` events."""

    def __init__(self, url, events, drop_after=None):
        self.cursor = int(url.split("cursor=")[1])
        self.events = [e for e in events if json.loads(e)["time_us"] >= self.cursor]
        self.drop_after = drop_after

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def __aiter__(self):
        return self._iterate()

    async def _iterate(self):
        for n, event in enumerate(self.events):
            if self.drop_after is not None and n == self.drop_after:
                raise websockets.exceptions.ConnectionClosedError(None, None)
            yield event


async def test_resumes_after_a_dropped_connection_without_gaps_or_repeats(monkeypatch):
    events = [post(i) for i in range(10)] + [post(99, lang="fr")]
    connections = []

    def fake_connect(url, **_kwargs):
        conn = FakeConnection(url, events, drop_after=3 if not connections else None)
        connections.append(conn)
        return conn

    monkeypatch.setattr(websockets, "connect", fake_connect)
    monkeypatch.setattr(sources.time, "time", lambda: 0.001)  # cursor starts before the first event
    got = [text async for _, text in sources.bluesky_posts(8, lookback_s=0)]

    assert got == [f"post {i}" for i in range(8)]  # French post skipped, nothing repeated
    assert len(connections) == 2
    assert connections[1].cursor == 1000 + 2 + 1  # resumed right after the last event received


async def test_gives_up_after_repeated_failures(monkeypatch):
    monkeypatch.setattr(websockets, "connect",
                        lambda url, **_: FakeConnection(url, [post(i) for i in range(5)], drop_after=0))
    monkeypatch.setattr(sources.time, "time", lambda: 0.001)
    with pytest.raises(websockets.exceptions.ConnectionClosedError):
        _ = [x async for x in sources.bluesky_posts(5, lookback_s=0, retries=2)]
