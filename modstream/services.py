"""Runtime wiring and use cases shared by the pages, the API and the workers."""

from __future__ import annotations

import asyncio
import logging
import os
import random
import shutil
import threading
import time
import uuid

from . import db, metrics
from .batching import MicroBatcher
from .broker import Broker, make_broker
from .config import Settings
from .detector import Analysis, Detector, load_scorer
from .hub import Hub
from .schemas import MessageIn

log = logging.getLogger(__name__)


class ServiceError(Exception):
    status = 400
    code = "invalid_request"

    def __init__(self, message: str, field: str | None = None):
        super().__init__(message)
        self.message, self.field = message, field


class NotFound(ServiceError):
    status = 404
    code = "not_found"


def _sniff_image(head: bytes) -> str | None:
    if head.startswith(b"\x89PNG\r\n\x1a\n"):
        return "png"
    if head.startswith(b"\xff\xd8\xff"):
        return "jpg"
    if head[:6] in (b"GIF87a", b"GIF89a"):
        return "gif"
    if head[:4] == b"RIFF" and head[8:12] == b"WEBP":
        return "webp"
    return None


class Runtime:
    """Owns every long-lived component and its lifecycle."""

    def __init__(self, settings: Settings, *, detector: Detector | None = None, broker: Broker | None = None):
        self.settings = settings
        self.detector = detector or Detector(
            load_scorer(settings.scorer, settings.model_variant),
            flag_threshold=settings.flag_threshold,
            review_threshold=settings.review_threshold,
        )
        self.broker = broker or make_broker(
            settings.redis_url, messages_maxlen=settings.messages_maxlen, events_maxlen=settings.events_maxlen,
        )
        self.engine = db.make_engine(settings.database_url)
        self.sessions = db.make_sessionmaker(self.engine)
        self.batcher: MicroBatcher[str, Analysis] = MicroBatcher(
            self.detector.analyze_many,
            max_batch=settings.batch_max_size,
            max_wait_ms=settings.batch_max_wait_ms,
            max_queue=settings.batch_max_queue,
            name="detector",
        )
        self.hub = Hub(self.broker, settings.subscriber_queue_size)
        self._stop = asyncio.Event()
        self._workers: list[asyncio.Task] = []
        self._background: set[asyncio.Task] = set()
        os.makedirs(settings.upload_dir, exist_ok=True)

    # --- lifecycle ---

    async def start(self, *, api: bool = True, workers: int | None = None) -> None:
        from .worker import Worker

        if self.settings.create_schema:
            await db.create_schema(self.engine)
        await self.broker.start()
        await self.batcher.start()
        if api:
            await self.hub.start()
        if workers is None:
            workers = self.settings.worker_concurrency if self.settings.run_worker else 0
        host = os.environ.get("HOSTNAME") or f"pid{os.getpid()}"
        for i in range(workers):
            worker = Worker(self, f"{host}-{i}")
            self._workers.append(asyncio.create_task(worker.run(self._stop), name=worker.name))
        self.spawn(self._poll_queue_metrics())
        if self.settings.warmup:
            # Load model weights off the event loop so the server accepts traffic immediately.
            threading.Thread(target=self.detector.warmup, name="model-warmup", daemon=True).start()
        log.info("runtime started: engine=%s broker=%s workers=%d",
                 self.detector.engine, type(self.broker).__name__, workers)

    async def stop(self) -> None:
        self._stop.set()
        if self._workers:  # let workers finish (and ack) their current batch
            _, pending = await asyncio.wait(self._workers, timeout=10)
            for task in pending:
                task.cancel()
        for task in self._background:
            task.cancel()
        await asyncio.gather(*self._background, return_exceptions=True)
        await self.hub.stop()
        await self.batcher.stop()
        await self.broker.close()
        await self.engine.dispose()

    def spawn(self, coro) -> asyncio.Task:
        task = asyncio.create_task(coro)
        self._background.add(task)
        task.add_done_callback(self._background.discard)
        return task

    async def _poll_queue_metrics(self) -> None:
        while True:
            try:
                stats = await self.broker.queue_stats()
                metrics.STREAM_LAG.set(stats["lag"])
                metrics.STREAM_PENDING.set(stats["pending"])
            except Exception:  # noqa: BLE001
                log.debug("queue metrics poll failed", exc_info=True)
            await asyncio.sleep(5)

    async def readiness(self) -> dict:
        checks = {"model": bool(getattr(self.detector.scorer, "ready", True)), "broker": await self.broker.ping()}
        try:
            async with self.engine.connect() as conn:
                await conn.exec_driver_sql("SELECT 1")
            checks["database"] = True
        except Exception:  # noqa: BLE001
            checks["database"] = False
        return checks

    # --- detection ---

    async def analyze(self, text: str, source: str, record: bool = True) -> Analysis:
        result = await self.batcher.submit(text)
        if record:
            await self.record([result], source)
        return result

    async def analyze_batch(self, texts: list[str], source: str) -> list[Analysis]:
        results = await self.batcher.submit_many(texts)
        await self.record(results, source)
        return results

    async def record(self, results: list[Analysis], source: str) -> None:
        counts: dict[str, float] = {"scans": len(results), f"source:{source}": len(results)}
        for r in results:
            metrics.ANALYSES.labels(source, r.verdict).inc()
            counts[f"verdict:{r.verdict}"] = counts.get(f"verdict:{r.verdict}", 0) + 1
            counts["latency_ms_sum"] = counts.get("latency_ms_sum", 0) + r.latency_ms
            if r.verdict != "safe":
                for cat in r.categories:
                    counts[f"cat:{cat}"] = counts.get(f"cat:{cat}", 0) + 1
            if r.verdict == "flagged":
                counts[f"flagged:{source}"] = counts.get(f"flagged:{source}", 0) + 1
        await self.broker.incr(counts)

    # --- feed ---

    async def save_image(self, upload) -> str | None:
        if upload is None or not upload.filename:
            return None
        if upload.size is not None and upload.size > self.settings.max_upload_bytes:
            raise ServiceError("Image must be 5 MB or smaller", "image")
        ext = _sniff_image(await upload.read(16))
        await upload.seek(0)
        if ext is None:
            raise ServiceError("Image must be a PNG, JPEG, GIF or WebP file", "image")
        name = f"{uuid.uuid4().hex}.{ext}"
        path = os.path.join(self.settings.upload_dir, name)

        def write():
            with open(path, "wb") as out:
                shutil.copyfileobj(upload.file, out)

        await asyncio.to_thread(write)
        return name

    async def create_post(self, author: str, text: str, image=None) -> dict:
        result = await self.analyze(text, "post")
        image_name = await self.save_image(image)
        async with self.sessions() as s:
            post = await db.insert_post(s, author, text, image_name, result)
        await self.broker.publish([{"type": "post", "channel": "feed", "data": post}])
        return post

    async def add_comment(self, post_id: int, author: str, text: str) -> dict:
        async with self.sessions() as s:
            if await s.get(db.Post, post_id) is None:
                raise NotFound(f"Post {post_id} not found")
            result = await self.analyze(text, "comment")
            comment = await db.insert_comment(s, post_id, author, text, result)
        await self.broker.publish([{"type": "comment", "channel": "feed", "data": comment}])
        return comment

    async def react(self, post_id: int, kind: str) -> dict:
        async with self.sessions() as s:
            counts = await db.react(s, post_id, kind)
        if counts is None:
            raise NotFound(f"Post {post_id} not found")
        return counts

    async def list_posts(self, limit: int = 50) -> list[dict]:
        async with self.sessions() as s:
            return await db.list_posts(s, limit)

    # --- streams ---

    async def enqueue(self, messages: list[MessageIn]) -> list[str]:
        now = time.time()
        return await self.broker.enqueue([{**m.model_dump(), "enqueued_at": now} for m in messages])

    async def list_messages(self, channel: str | None, verdict: str | None, limit: int) -> list[dict]:
        async with self.sessions() as s:
            return await db.list_messages(s, channel, verdict, limit)

    async def simulate(self, total: int, channels: int, rate: int) -> None:
        """Generate synthetic chat traffic across many channels (demo + smoke testing)."""
        tick = 0.05
        per_tick = max(1, int(rate * tick))
        sent = 0
        while sent < total:
            n = min(per_tick, total - sent)
            await self.enqueue([
                MessageIn(channel=f"room-{random.randrange(channels)}", author=random.choice(_AUTHORS),
                          text=random.choice(_SAMPLES))
                for _ in range(n)
            ])
            sent += n
            await asyncio.sleep(tick)

    # --- stats ---

    async def stats(self) -> dict:
        c = await self.broker.counters()
        async with self.sessions() as s:
            content = await db.content_counts(s)
        scans = int(c.get("scans", 0))
        flagged = int(c.get("verdict:flagged", 0))
        categories = {k[4:]: int(v) for k, v in c.items() if k.startswith("cat:")}
        return {
            "scans": scans,
            "flagged": flagged,
            "review": int(c.get("verdict:review", 0)),
            "flag_rate": round(flagged / scans, 4) if scans else 0,
            "avg_latency_ms": round(c.get("latency_ms_sum", 0) / scans, 2) if scans else 0,
            "categories": dict(sorted(categories.items(), key=lambda kv: -kv[1])),
            "by_source": {k[7:]: int(v) for k, v in c.items() if k.startswith("source:")},
            "messages": int(c.get("source:stream", 0)),
            "flagged_messages": int(c.get("flagged:stream", 0)),
            "queue": await self.broker.queue_stats(),
            "subscribers": self.hub.subscriber_count,
            **content,
        }


_AUTHORS = ["alex", "sam", "priya", "jordan", "kai", "lee", "noor", "max", "zoe", "remi"]
_SAMPLES = [
    "good luck on the exam tomorrow!", "who's coming to practice tonight?", "lol that clip was hilarious",
    "can someone send the notes from today", "gg everyone, great match", "happy birthday!! 🎉",
    "omw, 5 minutes", "this playlist is fire", "thanks for helping me earlier", "see you all on monday",
    "that exam was stupid hard", "the traffic today was insane", "ugh my phone died again",
    "the news covered the kidnapping in the north", "this game is kinda lame ngl",
    "you're such a loser", "nobody likes you, just leave", "ur an 1d10t", "kill yourself",
    "i will kill you if you show up", "shut up you moron", "go die",
]
