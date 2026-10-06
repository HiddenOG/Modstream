import asyncio
import contextlib
import time

from modstream import db
from modstream.schemas import MessageIn
from modstream.worker import Worker

WS = "alice"


async def messages(rt, ws=WS):
    async with rt.sessions() as s:
        return await db.list_messages(s, ws, None, None, 1000)


async def test_batch_is_moderated_stored_published_and_acked(runtime, scorer):
    start = await runtime.broker.last_event_id()
    await runtime.enqueue(WS, [MessageIn(channel="room-1", text="hello"),
                               MessageIn(channel="room-2", text="you idiot")])
    entries = await runtime.broker.consume("w", 10, 100)

    await Worker(runtime, "w").process(entries)

    stored = sorted(await messages(runtime), key=lambda m: m["id"])
    assert [m["verdict"] for m in stored] == ["safe", "flagged"]
    assert scorer.batch_sizes == [2]  # one forward pass for the whole batch

    events = await runtime.broker.events_after(start, 10)
    assert [e["channel"] for _, e in events] == ["room-1", "room-2"]
    assert all(e["workspace"] == WS and e["gen"] == 0 for _, e in events)
    assert events[1][1]["data"]["verdict"] == "flagged"

    assert (await runtime.broker.queue_stats())["pending"] == 0
    stats = await runtime.stats(WS)
    assert stats["messages"] == 2 and stats["flagged_messages"] == 1


async def test_one_batch_serves_many_workspaces_without_mixing(runtime, scorer):
    await runtime.enqueue("alice", [MessageIn(text="hello from alice")])
    await runtime.enqueue("bob", [MessageIn(text="you idiot"), MessageIn(text="hi from bob")])
    await Worker(runtime, "w").process(await runtime.broker.consume("w", 10, 100))

    assert scorer.batch_sizes == [3]  # still one forward pass across workspaces
    assert [m["body"] for m in await messages(runtime, "alice")] == ["hello from alice"]
    assert len(await messages(runtime, "bob")) == 2
    assert (await runtime.stats("alice"))["messages"] == 1
    assert (await runtime.stats("bob"))["flagged_messages"] == 1


async def test_redelivery_is_idempotent(runtime):
    await runtime.enqueue(WS, [MessageIn(text="hello")])
    entries = await runtime.broker.consume("w", 10, 100)
    worker = Worker(runtime, "w")
    await worker.process(entries)
    await worker.process(entries)  # e.g. crash after commit, before ack
    assert len(await messages(runtime)) == 1


async def test_invalid_payloads_are_dead_lettered(runtime):
    await runtime.broker.enqueue([{"text": ""}, {"nope": 1}, {"text": "fine", "enqueued_at": time.time()}])
    entries = await runtime.broker.consume("w", 10, 100)
    await Worker(runtime, "w").process(entries)
    stats = await runtime.broker.queue_stats()
    assert stats["dead_lettered"] == 2 and stats["pending"] == 0
    assert len(await messages(runtime, db.PUBLIC)) == 1  # no workspace in the payload: "public"


async def test_failed_batch_is_not_acked_and_gets_reclaimed(runtime, scorer, settings):
    await runtime.enqueue(WS, [MessageIn(text="hello")])
    entries = await runtime.broker.consume("crashy", 10, 100)

    original = scorer.score
    scorer.score = lambda texts: (_ for _ in ()).throw(RuntimeError("GPU fell over"))
    with contextlib.suppress(RuntimeError):
        await Worker(runtime, "crashy").process(entries)
    scorer.score = original
    assert (await runtime.broker.queue_stats())["pending"] == 1

    await asyncio.sleep(0.05)
    reclaimed = await runtime.broker.reclaim("healthy", 10, 10, settings.worker_max_deliveries)
    await Worker(runtime, "healthy").process(reclaimed)
    assert len(await messages(runtime)) == 1
    assert (await runtime.broker.queue_stats())["pending"] == 0


async def test_worker_loop_drains_queue(runtime):
    stop = asyncio.Event()
    task = asyncio.create_task(Worker(runtime, "loop").run(stop))
    await runtime.enqueue(WS, [MessageIn(channel=f"room-{i % 7}", text=f"message {i}") for i in range(300)])
    for _ in range(100):
        if len(await messages(runtime)) == 300:
            break
        await asyncio.sleep(0.05)
    stop.set()
    await asyncio.wait_for(task, 5)
    assert len(await messages(runtime)) == 300


async def test_messages_queued_before_a_reset_are_skipped(runtime, scorer):
    await runtime.enqueue("alice", [MessageIn(text="before reset")])
    await runtime.enqueue("bob", [MessageIn(text="bob is unaffected")])
    await runtime.broker.reset_workspace("alice")
    await runtime.enqueue("alice", [MessageIn(text="after reset")])

    await Worker(runtime, "w").process(await runtime.broker.consume("w", 10, 100))

    assert [m["body"] for m in await messages(runtime, "alice")] == ["after reset"]
    assert [m["body"] for m in await messages(runtime, "bob")] == ["bob is unaffected"]
    assert scorer.batch_sizes == [2]  # the stale message was never scored
    assert (await runtime.broker.queue_stats())["pending"] == 0


async def test_batch_in_flight_during_reset_is_discarded(runtime, scorer):
    await runtime.enqueue(WS, [MessageIn(text="hello"), MessageIn(text="you idiot")])
    entries = await runtime.broker.consume("w", 10, 100)
    start = await runtime.broker.last_event_id()

    # The owner resets while the model is running: generation 0 at the worker's check before
    # scoring, 1 at its check after. Works for both the memory and Redis brokers.
    seen = iter(range(100))

    async def changing_generation(_ws):
        return next(seen)

    runtime.broker.generation = changing_generation
    await Worker(runtime, "w").process(entries)
    assert scorer.batch_sizes == [2]  # it was scored...
    assert await messages(runtime) == []  # ...but nothing stored or published
    assert await runtime.broker.events_after(start, 10) == []
    assert (await runtime.broker.queue_stats())["pending"] == 0
