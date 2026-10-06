"""Runtime wiring and use cases shared by the pages, the API and the workers."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import logging
import os
import random
import secrets
import shutil
import threading
import time
import uuid
from datetime import timedelta

from . import db, metrics, sources
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


class Conflict(ServiceError):
    status = 409
    code = "conflict"


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
        self._demo: dict[str, set[asyncio.Task]] = {}  # workspace -> its running simulator / feed jobs
        self.batch_lock = asyncio.Lock()  # demo reset waits for this node's in-flight worker batch
        self.bluesky_source = sources.bluesky_posts  # swappable in tests
        self._author_salt = secrets.token_hex(8)  # anonymises real authors; new per process
        self.feeds: dict[str, dict] = {}  # workspace -> progress of its current simulator / live feed
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
        if api and self.settings.demo_retention_hours > 0:
            self.spawn(self._purge_loop())
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
        demo = [task for tasks in self._demo.values() for task in tasks]
        for task in (*self._background, *demo):
            task.cancel()
        await asyncio.gather(*self._background, *demo, return_exceptions=True)
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
    # Every use case takes ``ws``, the caller's workspace: a private sandbox per visitor.

    async def analyze(self, ws: str, text: str, source: str, record: bool = True) -> Analysis:
        result = await self.batcher.submit(text)
        if record:
            await self.record(ws, [result], source)
        return result

    async def analyze_batch(self, ws: str, texts: list[str], source: str) -> list[Analysis]:
        results = await self.batcher.submit_many(texts)
        await self.record(ws, results, source)
        return results

    async def record(self, ws: str, results: list[Analysis], source: str) -> None:
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
        await self.broker.incr(ws, counts)

    async def _event(self, ws: str, kind: str, channel: str, data: dict) -> dict:
        return {"type": kind, "channel": channel, "workspace": ws,
                "gen": await self.broker.generation(ws), "data": data}

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

    def _remove_images(self, names: list[str]) -> None:
        for name in names:
            with contextlib.suppress(OSError):
                os.remove(os.path.join(self.settings.upload_dir, os.path.basename(name)))

    async def create_post(self, ws: str, author: str, text: str, image=None) -> dict:
        result = await self.analyze(ws, text, "post")
        image_name = await self.save_image(image)
        async with self.sessions() as s:
            post = await db.insert_post(s, ws, author, text, image_name, result)
        await self.broker.publish([await self._event(ws, "post", "feed", post)])
        return post

    async def add_comment(self, ws: str, post_id: int, author: str, text: str) -> dict:
        async with self.sessions() as s:
            if await db.find_post(s, ws, post_id) is None:
                raise NotFound(f"Post {post_id} not found")
            result = await self.analyze(ws, text, "comment")
            comment = await db.insert_comment(s, ws, post_id, author, text, result)
        await self.broker.publish([await self._event(ws, "comment", "feed", comment)])
        return comment

    async def react(self, ws: str, post_id: int, kind: str) -> dict:
        async with self.sessions() as s:
            counts = await db.react(s, ws, post_id, kind)
        if counts is None:
            raise NotFound(f"Post {post_id} not found")
        return counts

    async def delete_post(self, ws: str, post_id: int) -> None:
        async with self.sessions() as s:
            image = await db.delete_post(s, ws, post_id)
        if image is None:
            raise NotFound(f"Post {post_id} not found")
        self._remove_images([image] if image else [])

    async def clear_feed(self, ws: str) -> None:
        async with self.sessions() as s:
            images = await db.clear_posts(s, ws)
        self._remove_images(images)

    async def list_posts(self, ws: str, limit: int = 50) -> list[dict]:
        async with self.sessions() as s:
            return await db.list_posts(s, ws, limit)

    # --- streams ---

    async def enqueue(self, ws: str, messages: list[MessageIn]) -> list[str]:
        now, gen = time.time(), await self.broker.generation(ws)
        return await self.broker.enqueue(
            [{**m.model_dump(), "workspace": ws, "gen": gen, "enqueued_at": now} for m in messages]
        )

    async def list_messages(
        self, ws: str, channel: str | None, verdict: str | None, limit: int, before_id: int | None = None,
    ) -> list[dict]:
        async with self.sessions() as s:
            return await db.list_messages(s, ws, channel, verdict, limit, before_id)

    # --- demo jobs (simulator, live feeds) ---

    def start_demo(self, ws: str, coro) -> asyncio.Task:
        """Run a simulator or live-feed job for one workspace; its Reset can cancel it.
        One job per visitor at a time, and a global cap, so a public demo can't be overloaded."""
        if self._demo.get(ws):
            coro.close()
            raise Conflict("A feed is already running. Wait for it to finish, or press Reset.")
        if sum(len(tasks) for tasks in self._demo.values()) >= self.settings.demo_max_jobs:
            coro.close()
            raise Conflict("The demo is busy right now. Please try again in a minute.")
        task = asyncio.create_task(coro)
        tasks = self._demo.setdefault(ws, set())
        tasks.add(task)

        def done(t: asyncio.Task) -> None:
            tasks.discard(t)
            if not tasks and self._demo.get(ws) is tasks:
                del self._demo[ws]

        task.add_done_callback(done)
        return task

    def _progress(self, ws: str, source: str, state: str, sent: int, target: int) -> None:
        """Progress of a workspace's current demo feed, shown on its monitor (this API node only)."""
        self.feeds[ws] = {"source": source, "state": state, "sent": sent, "target": target}

    async def simulate(self, ws: str, total: int, channels: int, rate: int) -> None:
        """Generate synthetic chat traffic across many channels (demo + smoke testing)."""
        tick = 0.05
        per_tick = max(1, int(rate * tick))
        sent = 0
        self._progress(ws, "simulator", "running", 0, total)
        while sent < total:
            n = min(per_tick, total - sent)
            await self.enqueue(ws, [
                MessageIn(channel=f"room-{random.randrange(channels)}", author=random.choice(_AUTHORS),
                          text=random.choice(_SAMPLES))
                for _ in range(n)
            ])
            sent += n
            self._progress(ws, "simulator", "running", sent, total)
            await asyncio.sleep(tick)
        self._progress(ws, "simulator", "done", sent, total)

    async def ingest_bluesky(self, ws: str, total: int) -> int:
        """Feed ``total`` real, recent public Bluesky posts through the pipeline.
        Authors are replaced by a salted hash so no real identity is stored or shown."""
        batch: list[MessageIn] = []
        sent = 0
        self._progress(ws, "bluesky", "connecting", 0, total)
        try:
            async for author_id, text in self.bluesky_source(total):
                tag = hashlib.sha256(f"{self._author_salt}:{author_id}".encode()).hexdigest()[:6]
                batch.append(MessageIn(channel="bluesky", author=f"bsky-{tag}", text=text))
                if len(batch) >= 50:
                    await self.enqueue(ws, batch)
                    sent += len(batch)
                    batch = []
                    self._progress(ws, "bluesky", "running", sent, total)
            state = "done"
        except Exception:  # noqa: BLE001 - feed outage: keep what was collected
            log.exception("bluesky feed stopped after %d posts", sent + len(batch))
            state = "failed"
        if batch:
            await self.enqueue(ws, batch)
            sent += len(batch)
        self._progress(ws, "bluesky", state, sent, total)
        log.info("bluesky feed: %d posts enqueued", sent)
        return sent

    async def reset_demo(self, ws: str) -> None:
        """Stop this workspace's demo jobs and wipe its stream messages, live events and counters.
        Other visitors' sandboxes are untouched."""
        tasks = list(self._demo.get(ws, ()))
        for task in tasks:
            task.cancel()
        if tasks:
            # Don't wait on slow shutdowns (e.g. a WebSocket close handshake): once cancelled,
            # a job can't enqueue anything more, and in-flight messages are discarded below.
            await asyncio.wait(tasks, timeout=0.5)
        async with self.batch_lock:
            await self.broker.reset_workspace(ws)  # bumps the generation: queued work is dropped
            self.hub.flush(ws)
            async with self.sessions() as s:
                await db.delete_messages(s, ws)  # stream messages only; feed posts are kept
        self.feeds.pop(ws, None)

    async def _purge_loop(self) -> None:
        """Delete sandbox data older than the retention period, so a public demo stays small."""
        while True:
            await asyncio.sleep(600)
            try:
                cutoff = db.utcnow() - timedelta(hours=self.settings.demo_retention_hours)
                async with self.sessions() as s:
                    images = await db.purge_older_than(s, cutoff)
                self._remove_images(images)
            except Exception:  # noqa: BLE001
                log.exception("retention purge failed")

    # --- stats ---

    async def stats(self, ws: str) -> dict:
        c = await self.broker.counters(ws)
        async with self.sessions() as s:
            content = await db.content_counts(s, ws)
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
            "queue": await self.broker.queue_stats(),  # shared pipeline: same for everyone
            "subscribers": self.hub.subscriber_count,
            "feed": self.feeds.get(ws),
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
