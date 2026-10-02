"""Per-process fan-out of the event log to live subscribers.

One background task tails the broker's event log. Each SSE or WebSocket
client gets a bounded in-memory queue, so a node serves thousands of viewers
with one Redis read loop instead of one query per viewer.

Slow consumers never block the hub: when a subscriber's queue is full, its
events are dropped and the subscriber is told how many it missed (``lagged``).
Clients that need every event reconnect with their last event id and replay
from the durable log.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable

from . import metrics
from .broker import Broker, Entry

log = logging.getLogger(__name__)

EventFilter = Callable[[dict], bool]


def make_filter(channel: str | None = None, verdicts: set[str] | None = None) -> EventFilter:
    def accept(event: dict) -> bool:
        if channel and event.get("channel") != channel:
            return False
        return not verdicts or event.get("data", {}).get("verdict") in verdicts

    return accept


class Subscription:
    def __init__(self, hub: Hub, accept: EventFilter, maxsize: int, transport: str):
        self._hub = hub
        self.accept = accept
        self.transport = transport
        self.queue: asyncio.Queue[Entry] = asyncio.Queue(maxsize=maxsize)
        self.dropped = 0

    def offer(self, entry: Entry) -> None:
        if not self.accept(entry[1]):
            return
        try:
            self.queue.put_nowait(entry)
        except asyncio.QueueFull:
            self.dropped += 1
            metrics.HUB_DROPPED.inc()

    async def get(self, timeout: float) -> Entry | None:
        try:
            return await asyncio.wait_for(self.queue.get(), timeout)
        except TimeoutError:
            return None

    def take_dropped(self) -> int:
        dropped, self.dropped = self.dropped, 0
        return dropped

    def close(self) -> None:
        self._hub._remove(self)

    def __enter__(self) -> Subscription:
        return self

    def __exit__(self, *exc) -> None:
        self.close()


class Hub:
    def __init__(self, broker: Broker, queue_size: int = 1000):
        self.broker = broker
        self.queue_size = queue_size
        self._subs: set[Subscription] = set()
        self._task: asyncio.Task | None = None
        self.last_id = "0"

    @property
    def subscriber_count(self) -> int:
        return len(self._subs)

    async def start(self) -> None:
        self.last_id = await self.broker.last_event_id()
        self._task = asyncio.create_task(self._run(), name="event-hub")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    def subscribe(self, accept: EventFilter | None = None, transport: str = "sse") -> Subscription:
        sub = Subscription(self, accept or (lambda _e: True), self.queue_size, transport)
        self._subs.add(sub)
        metrics.SUBSCRIBERS.labels(transport).inc()
        return sub

    def _remove(self, sub: Subscription) -> None:
        if sub in self._subs:
            self._subs.discard(sub)
            metrics.SUBSCRIBERS.labels(sub.transport).dec()

    async def _run(self) -> None:
        backoff = 0.5
        while True:
            try:
                entries = await self.broker.read_events(self.last_id, count=1000, block_ms=5000)
                backoff = 0.5
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - broker outage: keep serving, retry with backoff
                log.exception("event hub read failed; retrying in %.1fs", backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 10)
                continue
            for entry in entries:
                self.last_id = entry[0]
                for sub in tuple(self._subs):
                    sub.offer(entry)
