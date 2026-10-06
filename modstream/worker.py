"""Stream worker: consumes the message queue in batches and publishes verdicts.

Delivery is at-least-once. A batch is acknowledged only after its results are
stored and published, so a crash mid-batch means the messages are redelivered
(reclaimed by another worker after ``worker_reclaim_idle_ms``). Inserts are
idempotent on the broker entry id, so redelivery never creates duplicate rows.
Malformed payloads, and messages that keep failing, go to the dead-letter queue.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import UTC, datetime

from pydantic import ValidationError

from . import db, metrics
from .schemas import MessageIn

log = logging.getLogger(__name__)


def _ws(payload: dict) -> str:
    return payload.get("workspace") or db.PUBLIC


class Worker:
    def __init__(self, runtime, name: str):
        self.rt = runtime
        self.name = name

    async def run(self, stop: asyncio.Event) -> None:
        s = self.rt.settings
        broker = self.rt.broker
        loop = asyncio.get_running_loop()
        next_reclaim = loop.time()
        log.info("worker %s started", self.name)
        while not stop.is_set():
            try:
                if loop.time() >= next_reclaim:
                    next_reclaim = loop.time() + s.worker_reclaim_idle_ms / 2000
                    claimed = await broker.reclaim(
                        self.name, s.worker_reclaim_idle_ms, s.worker_batch_size, s.worker_max_deliveries,
                    )
                    if claimed:
                        metrics.STREAM_RECLAIMED.inc(len(claimed))
                        log.warning("worker %s reclaimed %d stalled messages", self.name, len(claimed))
                        await self.process(claimed)
                entries = await broker.consume(self.name, s.worker_batch_size, s.worker_block_ms)
                if entries:
                    await self.process(entries)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - unacked messages will be reclaimed and retried
                log.exception("worker %s failed a batch; it will be retried", self.name)
                await asyncio.sleep(1)
        log.info("worker %s stopped", self.name)

    async def process(self, entries: list[tuple[str, dict]]) -> None:
        # A demo reset on this node waits for the batch in flight, so a half-finished batch can't
        # write counters after the wipe. Across nodes, the generation check below covers it.
        async with self.rt.batch_lock:
            await self._process(entries)

    async def _current(self, valid) -> list:
        """Keep only messages whose workspace hasn't been reset since they were queued."""
        generations = {ws: await self.rt.broker.generation(ws) for ws in {_ws(p) for _, p, _ in valid}}
        return [v for v in valid if v[1].get("gen", 0) == generations[_ws(v[1])]]

    async def _process(self, entries: list[tuple[str, dict]]) -> None:
        broker = self.rt.broker
        valid, invalid = [], []
        for entry_id, payload in entries:
            try:
                valid.append((entry_id, payload, MessageIn.model_validate(payload)))
            except ValidationError as exc:
                invalid.append((entry_id, payload, f"invalid payload: {exc.errors()[0]['msg']}"))
        if invalid:
            await broker.dead_letter(invalid)
            await broker.ack([i[0] for i in invalid])
            metrics.STREAM_DEAD_LETTERED.inc(len(invalid))
        all_ids = [entry_id for entry_id, _, _ in valid]

        valid = await self._current(valid)  # queued before their owner pressed Reset: skip
        if not valid:
            await broker.ack(all_ids)
            return
        analyses = await self.rt.batcher.run_direct([m.text for _, _, m in valid])
        current = {entry_id for entry_id, _, _ in await self._current(valid)}  # reset during scoring
        scored = [(v, a) for v, a in zip(valid, analyses, strict=True) if v[0] in current]

        now = datetime.now(UTC)
        rows = []
        for (entry_id, payload, msg), analysis in scored:
            enqueued = payload.get("enqueued_at")
            rows.append({
                "entry_id": entry_id, "workspace": _ws(payload), "channel": msg.channel, "author": msg.author,
                "body": msg.text, "verdict": analysis.verdict, "risk": analysis.risk, "analysis": analysis.to_dict(),
                "enqueued_at": datetime.fromtimestamp(enqueued, UTC) if enqueued else now, "created_at": now,
            })
        if rows:
            async with self.rt.sessions() as session:
                ids = await db.insert_messages(session, rows)
            events, by_workspace = [], {}
            for row, ((_, payload, _), analysis) in zip(rows, scored, strict=True):
                data = {**row, "id": ids.get(row["entry_id"]),
                        "enqueued_at": row["enqueued_at"].isoformat(), "created_at": row["created_at"].isoformat()}
                events.append({"type": "message", "channel": row["channel"], "workspace": row["workspace"],
                               "gen": payload.get("gen", 0), "data": data})
                by_workspace.setdefault(row["workspace"], []).append(analysis)
            await broker.publish(events)
            for ws, results in by_workspace.items():
                await self.rt.record(ws, results, "stream")
        await broker.ack(all_ids)

        metrics.STREAM_PROCESSED.inc(len(scored))
        wall = time.time()
        for (_, payload, _), _analysis in scored:
            if payload.get("enqueued_at"):
                metrics.STREAM_E2E_SECONDS.observe(max(0.0, wall - payload["enqueued_at"]))
