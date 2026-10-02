import asyncio

from cybershield.broker import MemoryBroker
from cybershield.hub import Hub, make_filter


def event(channel, verdict):
    return {"type": "message", "channel": channel, "data": {"verdict": verdict}}


async def test_fan_out_with_filters():
    broker = MemoryBroker()
    hub = Hub(broker, queue_size=100)
    await hub.start()
    try:
        everything = hub.subscribe()
        room1 = hub.subscribe(make_filter("room-1"))
        flagged = hub.subscribe(make_filter(verdicts={"flagged"}))
        assert hub.subscriber_count == 3

        await broker.publish([event("room-1", "safe"), event("room-2", "flagged"), event("room-1", "flagged")])
        await asyncio.sleep(0.05)

        assert everything.queue.qsize() == 3
        assert room1.queue.qsize() == 2
        assert flagged.queue.qsize() == 2

        room1.close()
        assert hub.subscriber_count == 2
    finally:
        await hub.stop()


async def test_slow_subscriber_drops_instead_of_blocking():
    broker = MemoryBroker()
    hub = Hub(broker, queue_size=5)
    await hub.start()
    try:
        slow = hub.subscribe()
        fast = hub.subscribe()
        await broker.publish([event("a", "safe") for _ in range(20)])
        await asyncio.sleep(0.05)
        assert slow.queue.qsize() == 5
        assert slow.take_dropped() == 15
        assert slow.take_dropped() == 0
        # The hub kept going for everyone else.
        drained = [await fast.get(0.1) for _ in range(5)]
        assert all(drained) and all(d.event["channel"] == "a" for d in drained)
    finally:
        await hub.stop()


async def test_many_subscribers():
    broker = MemoryBroker()
    hub = Hub(broker)
    await hub.start()
    try:
        subs = [hub.subscribe() for _ in range(5000)]
        await broker.publish([event("a", "safe")])
        await asyncio.sleep(0.1)
        assert all(s.queue.qsize() == 1 for s in subs)
    finally:
        await hub.stop()
