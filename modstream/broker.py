"""Message broker: the shared backbone between API nodes and workers.

Three responsibilities:

* **Work queue** (``modstream:messages``): durable, at-least-once delivery to a
  consumer group of workers. Crashed workers' messages are reclaimed after an
  idle timeout, and poison messages go to a dead-letter stream.
* **Event log** (``modstream:events``): a capped, ordered log of moderation results.
  Every API node tails it once and fans out to its local subscribers. Clients
  resume after a disconnect by event id.
* **Counters** (``modstream:stats``): atomic counters for dashboards, replacing
  aggregate SQL queries.

``RedisBroker`` is the production implementation (Redis Streams).
``MemoryBroker`` implements the same contract in-process, for local
development and tests.
"""

from __future__ import annotations

import asyncio
import itertools
import json
import time
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field

Entry = tuple[str, dict]

MESSAGES = "modstream:messages"
EVENTS = "modstream:events"
DLQ = "modstream:dlq"
STATS = "modstream:stats"
GROUP = "moderators"


class Broker(ABC):
    async def start(self) -> None:  # optional hook
        return None

    async def close(self) -> None:  # optional hook
        return None

    @abstractmethod
    async def ping(self) -> bool: ...

    # --- work queue ---
    @abstractmethod
    async def enqueue(self, payloads: list[dict]) -> list[str]: ...

    @abstractmethod
    async def consume(self, consumer: str, count: int, block_ms: int) -> list[Entry]: ...

    @abstractmethod
    async def reclaim(self, consumer: str, min_idle_ms: int, count: int, max_deliveries: int) -> list[Entry]:
        """Take over messages stuck with dead consumers. Messages delivered more
        than ``max_deliveries`` times are dead-lettered instead of returned."""

    @abstractmethod
    async def ack(self, ids: list[str]) -> None: ...

    @abstractmethod
    async def dead_letter(self, items: list[tuple[str, dict, str]]) -> None: ...

    @abstractmethod
    async def queue_stats(self) -> dict: ...

    # --- event log ---
    @abstractmethod
    async def publish(self, events: list[dict]) -> list[str]: ...

    @abstractmethod
    async def last_event_id(self) -> str: ...

    @abstractmethod
    async def read_events(self, after: str, count: int, block_ms: int) -> list[Entry]:
        """Block until events newer than ``after`` exist (or the timeout passes)."""

    @abstractmethod
    async def events_after(self, after: str, count: int) -> list[Entry]: ...

    @abstractmethod
    async def recent_events(self, count: int) -> list[Entry]: ...

    # --- counters ---
    @abstractmethod
    async def incr(self, fields: dict[str, float]) -> None: ...

    @abstractmethod
    async def counters(self) -> dict[str, float]: ...


# --- In-memory ----------------------------------------------------------------

@dataclass
class _Pending:
    payload: dict
    consumer: str
    delivered_at: float
    deliveries: int = 1


@dataclass
class MemoryBroker(Broker):
    """Single-process broker with the same semantics as ``RedisBroker``."""

    events_maxlen: int = 10_000
    _ids: itertools.count = field(default_factory=lambda: itertools.count(1))
    _queue: deque = field(default_factory=deque)
    _pending: dict = field(default_factory=dict)
    _dlq: list = field(default_factory=list)
    _events: deque = field(default_factory=deque)
    _counters: dict = field(default_factory=dict)
    _new_work: asyncio.Event = field(default_factory=asyncio.Event)
    _new_event: asyncio.Condition = field(default_factory=asyncio.Condition)

    async def ping(self) -> bool:
        return True

    async def enqueue(self, payloads):
        ids = []
        for payload in payloads:
            entry_id = str(next(self._ids))
            self._queue.append((entry_id, payload))
            ids.append(entry_id)
        self._new_work.set()
        return ids

    async def consume(self, consumer, count, block_ms):
        if not self._queue:
            self._new_work.clear()
            try:
                await asyncio.wait_for(self._new_work.wait(), block_ms / 1000)
            except TimeoutError:
                return []
        out = []
        while self._queue and len(out) < count:
            entry_id, payload = self._queue.popleft()
            self._pending[entry_id] = _Pending(payload, consumer, time.monotonic())
            out.append((entry_id, payload))
        return out

    async def reclaim(self, consumer, min_idle_ms, count, max_deliveries):
        now, out, dead = time.monotonic(), [], []
        for entry_id, p in list(self._pending.items()):
            if len(out) >= count:
                break
            if (now - p.delivered_at) * 1000 < min_idle_ms:
                continue
            p.deliveries += 1
            p.consumer, p.delivered_at = consumer, now
            if p.deliveries > max_deliveries:
                dead.append((entry_id, p.payload, f"exceeded {max_deliveries} deliveries"))
            else:
                out.append((entry_id, p.payload))
        if dead:
            await self.dead_letter(dead)
            await self.ack([d[0] for d in dead])
        return out

    async def ack(self, ids):
        for entry_id in ids:
            self._pending.pop(entry_id, None)

    async def dead_letter(self, items):
        self._dlq.extend(items)

    async def queue_stats(self):
        return {"lag": len(self._queue), "pending": len(self._pending), "dead_lettered": len(self._dlq)}

    async def publish(self, events):
        ids = []
        async with self._new_event:
            for event in events:
                event_id = str(next(self._ids))
                self._events.append((event_id, event))
                ids.append(event_id)
            while len(self._events) > self.events_maxlen:
                self._events.popleft()
            self._new_event.notify_all()
        return ids

    async def last_event_id(self):
        return self._events[-1][0] if self._events else "0"

    async def events_after(self, after, count):
        return [e for e in self._events if int(e[0]) > int(after)][:count]

    async def read_events(self, after, count, block_ms):
        async with self._new_event:
            if not await self.events_after(after, 1):
                try:
                    await asyncio.wait_for(self._new_event.wait(), block_ms / 1000)
                except TimeoutError:
                    return []
        return await self.events_after(after, count)

    async def recent_events(self, count):
        return list(self._events)[-count:] if count else []

    async def incr(self, fields):
        for key, value in fields.items():
            self._counters[key] = self._counters.get(key, 0) + value

    async def counters(self):
        return dict(self._counters)


# --- Redis Streams --------------------------------------------------------------

def _decode(fields: dict) -> dict:
    return json.loads(fields["data"])


class RedisBroker(Broker):
    def __init__(self, client, *, messages_maxlen: int = 1_000_000, events_maxlen: int = 10_000):
        self.r = client
        self.messages_maxlen = messages_maxlen
        self.events_maxlen = events_maxlen

    @classmethod
    def from_url(cls, url: str, **kwargs) -> RedisBroker:
        import redis.asyncio as redis

        return cls(redis.from_url(url, decode_responses=True, health_check_interval=30), **kwargs)

    async def start(self):
        try:
            # id "0" so messages enqueued before any worker started are still processed
            await self.r.xgroup_create(MESSAGES, GROUP, id="0", mkstream=True)
        except Exception as exc:  # noqa: BLE001
            if "BUSYGROUP" not in str(exc):
                raise

    async def close(self):
        await self.r.aclose()

    async def ping(self):
        try:
            return bool(await self.r.ping())
        except Exception:  # noqa: BLE001
            return False

    async def enqueue(self, payloads):
        async with self.r.pipeline(transaction=False) as pipe:
            for payload in payloads:
                pipe.xadd(MESSAGES, {"data": json.dumps(payload)}, maxlen=self.messages_maxlen, approximate=True)
            return await pipe.execute()

    async def consume(self, consumer, count, block_ms):
        resp = await self.r.xreadgroup(GROUP, consumer, {MESSAGES: ">"}, count=count, block=block_ms)
        return [(entry_id, _decode(fields)) for _, entries in resp or [] for entry_id, fields in entries]

    async def reclaim(self, consumer, min_idle_ms, count, max_deliveries):
        resp = await self.r.xautoclaim(MESSAGES, GROUP, consumer, min_idle_ms, "0-0", count=count)
        claimed = [(entry_id, fields) for entry_id, fields in resp[1] if fields]
        if not claimed:
            return []
        pending = await self.r.xpending_range(
            MESSAGES, GROUP, min=claimed[0][0], max=claimed[-1][0], count=len(claimed) * 2, consumername=consumer,
        )
        deliveries = {p["message_id"]: p["times_delivered"] for p in pending}
        out, dead = [], []
        for entry_id, fields in claimed:
            payload = _decode(fields)
            if deliveries.get(entry_id, 1) > max_deliveries:
                dead.append((entry_id, payload, f"exceeded {max_deliveries} deliveries"))
            else:
                out.append((entry_id, payload))
        if dead:
            await self.dead_letter(dead)
            await self.ack([d[0] for d in dead])
        return out

    async def ack(self, ids):
        if ids:
            await self.r.xack(MESSAGES, GROUP, *ids)

    async def dead_letter(self, items):
        async with self.r.pipeline(transaction=False) as pipe:
            for entry_id, payload, error in items:
                pipe.xadd(DLQ, {"data": json.dumps(payload), "source_id": entry_id, "error": error},
                          maxlen=100_000, approximate=True)
            await pipe.execute()

    async def queue_stats(self):
        groups = await self.r.xinfo_groups(MESSAGES)
        group = next((g for g in groups if g["name"] == GROUP), {})
        lag = group.get("lag")
        if lag is None:  # Redis < 7 or lag unknown after trimming
            lag = 0
        return {"lag": int(lag), "pending": int(group.get("pending", 0)), "dead_lettered": await self.r.xlen(DLQ)}

    async def publish(self, events):
        async with self.r.pipeline(transaction=False) as pipe:
            for event in events:
                pipe.xadd(EVENTS, {"data": json.dumps(event)}, maxlen=self.events_maxlen, approximate=True)
            return await pipe.execute()

    async def last_event_id(self):
        last = await self.r.xrevrange(EVENTS, count=1)
        return last[0][0] if last else "0-0"

    async def read_events(self, after, count, block_ms):
        resp = await self.r.xread({EVENTS: after}, count=count, block=block_ms)
        return [(entry_id, _decode(fields)) for _, entries in resp or [] for entry_id, fields in entries]

    async def events_after(self, after, count):
        entries = await self.r.xrange(EVENTS, min=after, count=count + 1)
        return [(entry_id, _decode(fields)) for entry_id, fields in entries if entry_id != after][:count]

    async def recent_events(self, count):
        if not count:
            return []
        entries = await self.r.xrevrange(EVENTS, count=count)
        return [(entry_id, _decode(fields)) for entry_id, fields in reversed(entries)]

    async def incr(self, fields):
        async with self.r.pipeline(transaction=False) as pipe:
            for key, value in fields.items():
                pipe.hincrbyfloat(STATS, key, value)
            await pipe.execute()

    async def counters(self):
        return {k: float(v) for k, v in (await self.r.hgetall(STATS)).items()}


def make_broker(redis_url: str | None, **kwargs) -> Broker:
    if redis_url:
        return RedisBroker.from_url(redis_url, **kwargs)
    return MemoryBroker(events_maxlen=kwargs.get("events_maxlen", 10_000))
