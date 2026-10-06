import asyncio
import os

import pytest

from modstream.broker import MemoryBroker, RedisBroker


@pytest.fixture(params=["memory", "redis"])
async def broker(request):
    if request.param == "memory":
        b = MemoryBroker(events_maxlen=100)
    else:
        url = os.environ.get("MODSTREAM_TEST_REDIS_URL")
        if url:  # real Redis in CI
            b = RedisBroker.from_url(url, events_maxlen=100)
            await b.r.flushdb()
        else:
            import fakeredis

            b = RedisBroker(fakeredis.FakeAsyncRedis(decode_responses=True), events_maxlen=100)
    await b.start()
    yield b
    await b.close()


async def test_enqueue_consume_ack(broker):
    ids = await broker.enqueue([{"n": 1}, {"n": 2}, {"n": 3}])
    assert len(ids) == 3
    got = await broker.consume("w1", count=2, block_ms=100)
    assert [p["n"] for _, p in got] == [1, 2]
    got += await broker.consume("w1", count=10, block_ms=100)
    assert [p["n"] for _, p in got] == [1, 2, 3]
    assert (await broker.queue_stats())["pending"] == 3
    await broker.ack([i for i, _ in got])
    stats = await broker.queue_stats()
    assert stats["pending"] == 0 and stats["lag"] == 0


async def test_consume_times_out_when_empty(broker):
    assert await broker.consume("w1", count=10, block_ms=50) == []


async def test_consume_wakes_on_new_work(broker):
    if isinstance(broker, RedisBroker) and not os.environ.get("MODSTREAM_TEST_REDIS_URL"):
        pytest.skip("fakeredis doesn't wake a blocked XREADGROUP; covered against real Redis in CI")
    task = asyncio.create_task(broker.consume("w1", count=10, block_ms=2000))
    await asyncio.sleep(0.05)
    await broker.enqueue([{"n": 1}])
    got = await asyncio.wait_for(task, 3)
    assert [p["n"] for _, p in got] == [1]


async def test_reclaim_from_dead_consumer_then_dead_letter(broker):
    await broker.enqueue([{"n": 1}])
    await broker.consume("crashed", count=10, block_ms=100)  # never acked
    await asyncio.sleep(0.02)

    reclaimed = await broker.reclaim("w2", min_idle_ms=10, count=10, max_deliveries=2)
    assert [p["n"] for _, p in reclaimed] == [1]

    await asyncio.sleep(0.02)
    again = await broker.reclaim("w3", min_idle_ms=10, count=10, max_deliveries=2)
    assert again == []  # third delivery exceeds the limit
    stats = await broker.queue_stats()
    assert stats["dead_lettered"] == 1 and stats["pending"] == 0


async def test_reclaim_ignores_recent_deliveries(broker):
    await broker.enqueue([{"n": 1}])
    await broker.consume("w1", count=10, block_ms=100)
    assert await broker.reclaim("w2", min_idle_ms=60_000, count=10, max_deliveries=5) == []


async def test_event_log_resume_and_backlog(broker):
    start = await broker.last_event_id()
    ids = await broker.publish([{"i": i} for i in range(5)])
    assert [e["i"] for _, e in await broker.events_after(start, 10)] == [0, 1, 2, 3, 4]
    assert [e["i"] for _, e in await broker.events_after(ids[1], 10)] == [2, 3, 4]
    assert [e["i"] for _, e in await broker.recent_events(2)] == [3, 4]
    assert await broker.last_event_id() == ids[-1]


async def test_read_events_blocks_until_published(broker):
    last = await broker.last_event_id()
    task = asyncio.create_task(broker.read_events(last, count=10, block_ms=2000))
    await asyncio.sleep(0.05)
    await broker.publish([{"hello": "world"}])
    got = await asyncio.wait_for(task, 3)
    assert got[0][1] == {"hello": "world"}


async def test_counters_are_per_workspace(broker):
    await broker.incr("alice", {"scans": 1, "latency": 2.5})
    await broker.incr("alice", {"scans": 2})
    await broker.incr("bob", {"scans": 10})
    assert await broker.counters("alice") == {"scans": 3, "latency": 2.5}
    assert await broker.counters("bob") == {"scans": 10}
    assert await broker.counters("nobody") == {}


async def test_reset_workspace_only_touches_that_workspace(broker):
    await broker.incr("alice", {"scans": 5})
    await broker.incr("bob", {"scans": 7})
    assert await broker.generation("alice") == 0

    await broker.reset_workspace("alice")

    assert await broker.counters("alice") == {}
    assert await broker.generation("alice") == 1
    assert await broker.counters("bob") == {"scans": 7}
    assert await broker.generation("bob") == 0
