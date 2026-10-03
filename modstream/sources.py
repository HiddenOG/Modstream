"""Real public message sources for demos and load tests.

Bluesky's Jetstream is a free, public WebSocket feed of every new Bluesky post (no account or
key). Starting from a cursor a few minutes in the past replays recent posts at full speed:
about 5,000 English posts in ~25 seconds, versus ~4 minutes waiting for them live.
"""

from __future__ import annotations

import json
import time
from collections.abc import AsyncIterator

JETSTREAM_URL = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"


async def bluesky_posts(limit: int, lookback_s: int = 900, lang: str = "en") -> AsyncIterator[tuple[str, str]]:
    """Yield ``(author_id, text)`` for up to ``limit`` recent public posts in ``lang``."""
    import websockets

    cursor = int((time.time() - lookback_s) * 1_000_000)  # microseconds
    sent = 0
    # Short close timeout: when a demo reset cancels the feed, don't wait 10 s for the server's goodbye.
    async with websockets.connect(f"{JETSTREAM_URL}&cursor={cursor}", open_timeout=20, close_timeout=1,
                                  max_size=2**20) as ws:
        async for raw in ws:
            event = json.loads(raw)
            commit = event.get("commit") or {}
            record = commit.get("record") or {}
            text = (record.get("text") or "").strip()
            if (event.get("kind") != "commit" or commit.get("operation") != "create"
                    or lang not in (record.get("langs") or []) or not text):
                continue
            yield event.get("did", ""), text[:5000]
            sent += 1
            if sent >= limit:
                return
