"""Dynamic micro-batching.

Transformer inference costs roughly the same for 1 text as for 32 (the work
is dominated by fixed per-call overhead and matrix ops that parallelise over the
batch). The batcher holds each request for at most ``max_wait_ms`` so that
requests from many concurrent users and streams share one forward pass.

Inference runs on a single dedicated thread: the event loop never blocks, and
PyTorch's own intra-op threads aren't oversubscribed by parallel forward passes.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from typing import Generic, TypeVar

from . import metrics

T = TypeVar("T")
R = TypeVar("R")


class Overloaded(Exception):
    """Raised when the queue is full, so callers can shed load (HTTP 503)."""


class MicroBatcher(Generic[T, R]):
    def __init__(
        self,
        fn: Callable[[list[T]], Sequence[R]],
        *,
        max_batch: int = 32,
        max_wait_ms: float = 5.0,
        max_queue: int = 10_000,
        name: str = "model",
    ):
        self._fn = fn
        self.max_batch = max_batch
        self.max_wait = max_wait_ms / 1000
        self.name = name
        self._queue: asyncio.Queue[tuple[T, asyncio.Future]] = asyncio.Queue(maxsize=max_queue)
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix=f"{name}-inference")
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name=f"{self.name}-batcher")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        while not self._queue.empty():
            _, fut = self._queue.get_nowait()
            if not fut.done():
                fut.set_exception(RuntimeError("batcher stopped"))
        self._executor.shutdown(wait=False, cancel_futures=True)

    @property
    def depth(self) -> int:
        return self._queue.qsize()

    async def submit(self, item: T) -> R:
        fut = asyncio.get_running_loop().create_future()
        try:
            self._queue.put_nowait((item, fut))
        except asyncio.QueueFull:
            metrics.BATCHER_REJECTED.labels(self.name).inc()
            raise Overloaded(f"{self.name} queue is full") from None
        return await fut

    async def submit_many(self, items: Sequence[T]) -> list[R]:
        return list(await asyncio.gather(*(self.submit(i) for i in items)))

    async def run_direct(self, items: list[T]) -> Sequence[R]:
        """Run an already-formed batch on the inference thread (used by stream workers)."""
        return await self._infer(items)

    async def _infer(self, items: list[T]) -> Sequence[R]:
        started = time.perf_counter()
        try:
            return await asyncio.get_running_loop().run_in_executor(self._executor, self._fn, items)
        finally:
            metrics.BATCH_SIZE.labels(self.name).observe(len(items))
            metrics.INFERENCE_SECONDS.labels(self.name).observe(time.perf_counter() - started)

    async def _run(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            batch = [await self._queue.get()]
            deadline = loop.time() + self.max_wait
            while len(batch) < self.max_batch:
                try:
                    batch.append(self._queue.get_nowait())
                    continue
                except asyncio.QueueEmpty:
                    pass
                remaining = deadline - loop.time()
                if remaining <= 0:
                    break
                try:
                    batch.append(await asyncio.wait_for(self._queue.get(), remaining))
                except TimeoutError:
                    break
            metrics.BATCHER_DEPTH.labels(self.name).set(self._queue.qsize())

            live = [(item, fut) for item, fut in batch if not fut.cancelled()]
            if not live:
                continue
            try:
                results = await self._infer([item for item, _ in live])
            except Exception as exc:  # noqa: BLE001 - propagate to every caller in the batch
                for _, fut in live:
                    if not fut.done():
                        fut.set_exception(exc)
            else:
                for (_, fut), result in zip(live, results, strict=True):
                    if not fut.done():
                        fut.set_result(result)
