import asyncio

import pytest

from modstream.batching import MicroBatcher, Overloaded


@pytest.fixture
async def make():
    batchers = []

    async def factory(fn, **kwargs):
        b = MicroBatcher(fn, **kwargs)
        await b.start()
        batchers.append(b)
        return b

    yield factory
    for b in batchers:
        await b.stop()


async def test_concurrent_requests_share_one_call(make):
    calls = []

    def fn(items):
        calls.append(list(items))
        return [i * 2 for i in items]

    b = await make(fn, max_batch=64, max_wait_ms=20)
    results = await asyncio.gather(*(b.submit(i) for i in range(50)))
    assert results == [i * 2 for i in range(50)]
    assert len(calls) == 1 and len(calls[0]) == 50


async def test_respects_max_batch(make):
    sizes = []

    def fn(items):
        sizes.append(len(items))
        return items

    b = await make(fn, max_batch=8, max_wait_ms=20)
    await b.submit_many(list(range(20)))
    assert max(sizes) <= 8 and sum(sizes) == 20


async def test_single_request_is_not_delayed_much(make):
    b = await make(lambda items: items, max_batch=32, max_wait_ms=5)
    loop = asyncio.get_running_loop()
    started = loop.time()
    assert await b.submit("x") == "x"
    assert loop.time() - started < 0.5


async def test_errors_propagate_to_every_caller(make):
    def boom(_items):
        raise ValueError("model crashed")

    b = await make(boom, max_wait_ms=10)
    results = await asyncio.gather(b.submit(1), b.submit(2), return_exceptions=True)
    assert all(isinstance(r, ValueError) for r in results)


async def test_sheds_load_when_queue_full():
    release = asyncio.Event()

    def slow(items):
        return items

    b = MicroBatcher(slow, max_batch=1, max_wait_ms=0, max_queue=2)
    # Not started, so nothing drains the queue.
    first = asyncio.create_task(b.submit(1))
    second = asyncio.create_task(b.submit(2))
    await asyncio.sleep(0)
    with pytest.raises(Overloaded):
        await b.submit(3)
    await b.start()
    assert await asyncio.gather(first, second) == [1, 2]
    release.set()
    await b.stop()
