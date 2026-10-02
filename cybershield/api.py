"""Versioned JSON API: /api/v1."""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from typing import Annotated

from fastapi import APIRouter, Depends, Header, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import ValidationError

from . import __version__
from .batching import Overloaded
from .hub import make_filter
from .schemas import (
    AnalysisOut,
    AnalyzeRequest,
    BatchAnalysisOut,
    BatchAnalyzeRequest,
    CommentIn,
    MessagesAccepted,
    MessagesIn,
    PostIn,
    ReactionIn,
    SimulateIn,
)
from .services import Runtime, ServiceError

router = APIRouter(prefix="/api/v1")


def get_runtime(request: Request) -> Runtime:
    return request.app.state.rt


RT = Annotated[Runtime, Depends(get_runtime)]


# --- Health ------------------------------------------------------------------------

@router.get("/health", name="api.health", tags=["ops"])
async def health(rt: RT):
    """Liveness: the process is up and serving."""
    return {"status": "ok", "version": __version__, "engine": rt.detector.engine,
            "model_ready": bool(getattr(rt.detector.scorer, "ready", True))}


@router.get("/ready", name="api.ready", tags=["ops"])
async def ready(rt: RT):
    """Readiness: model loaded and dependencies reachable. Load balancers route traffic only when 200."""
    checks = await rt.readiness()
    ok = all(checks.values())
    return JSONResponse({"ready": ok, "checks": checks}, status_code=200 if ok else 503)


# --- Detection (synchronous path) --------------------------------------------------------

@router.post("/analyze", response_model=AnalysisOut, name="api.analyze", tags=["detection"])
async def analyze(body: AnalyzeRequest, rt: RT):
    """Analyze one text. Concurrent requests are micro-batched into shared model passes."""
    return (await rt.analyze(body.text, body.source, record=body.record)).to_dict()


@router.post("/analyze/batch", response_model=BatchAnalysisOut, tags=["detection"])
async def analyze_batch(body: BatchAnalyzeRequest, rt: RT):
    results = await rt.analyze_batch(body.texts, body.source)
    return {"results": [r.to_dict() for r in results]}


# --- Streams (asynchronous path) ------------------------------------------------------------

@router.post("/messages", status_code=202, response_model=MessagesAccepted, tags=["streams"])
async def ingest_messages(body: MessagesIn, rt: RT):
    """Enqueue up to 1,000 messages for moderation by the worker pool. Verdicts arrive on
    ``/stream`` and the WebSocket within milliseconds; nothing blocks on the model here."""
    ids = await rt.enqueue(body.messages)
    return {"accepted": len(ids), "ids": ids}


@router.get("/messages", tags=["streams"])
async def list_messages(
    rt: RT,
    channel: str | None = None,
    verdict: str | None = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
):
    return {"messages": await rt.list_messages(channel, verdict, limit)}


@router.post("/simulate", status_code=202, tags=["streams"])
async def simulate(body: SimulateIn, rt: RT):
    """Generate synthetic traffic across many channels (demo and smoke testing)."""
    if not rt.settings.enable_simulator:
        raise ServiceError("Simulator is disabled on this deployment")
    total = min(body.messages, rt.settings.simulator_max_messages)
    rt.spawn(rt.simulate(total, body.channels, body.rate))
    return {"started": True, "messages": total, "channels": body.channels, "rate": body.rate}


def _sse(event_id: str, event: dict) -> str:
    return f"id: {event_id}\nevent: {event['type']}\ndata: {json.dumps(event['data'])}\n\n"


@router.get("/stream", name="api.stream", tags=["streams"])
async def stream(
    request: Request,
    rt: RT,
    channel: str | None = None,
    verdict: Annotated[str | None, Query(description="Comma-separated, e.g. flagged,review")] = None,
    last_event_id: Annotated[str | None, Header()] = None,
):
    """Server-Sent Events of moderation results (posts, comments and stream messages).

    Resumable: reconnecting browsers send ``Last-Event-ID`` and replay what they missed.
    Filters are applied server-side, so a client watching one channel only receives that channel.
    """
    accept = make_filter(channel, set(verdict.split(",")) if verdict else None)
    settings = rt.settings

    async def events():
        # Subscribe before reading history so nothing published in between is missed.
        with rt.hub.subscribe(accept, "sse") as sub:
            yield "retry: 3000\n\n"
            if last_event_id:
                history = await rt.broker.events_after(last_event_id, 1000)
            else:
                history = await rt.broker.recent_events(settings.stream_backlog)
            seen = set()
            for event_id, event in history:
                seen.add(event_id)
                if accept(event):
                    yield _sse(event_id, event)
            deadline = time.monotonic() + settings.stream_max_seconds
            while (remaining := deadline - time.monotonic()) > 0:
                entry = await sub.get(min(settings.stream_heartbeat_s, remaining))
                if dropped := sub.take_dropped():
                    yield f"event: lagged\ndata: {json.dumps({'dropped': dropped})}\n\n"
                if entry is None:
                    yield ": keep-alive\n\n"
                elif entry[0] not in seen:
                    yield _sse(*entry)

    return StreamingResponse(
        events(), media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.websocket("/ws", name="api.ws")
async def websocket(ws: WebSocket):
    """Bidirectional stream. Client frames (JSON):

    * ``{"type": "analyze", "ref": "1", "text": "..."}`` → ``{"type": "result", "ref": "1", "result": {...}}``
    * ``{"type": "publish", "ref": "2", "channel": "room-1", "author": "a", "text": "..."}`` → ``accepted``
    * ``{"type": "subscribe", "channel": "room-1", "verdicts": ["flagged"]}`` → ``event`` frames
    * ``{"type": "ping"}`` → ``pong``
    """
    rt: Runtime = ws.app.state.rt
    await ws.accept()
    outbox: asyncio.Queue[dict] = asyncio.Queue(maxsize=1000)
    inflight = asyncio.Semaphore(64)
    tasks: set[asyncio.Task] = set()
    subs = []

    def spawn(coro):
        task = asyncio.create_task(coro)
        tasks.add(task)
        task.add_done_callback(tasks.discard)

    async def writer():
        while True:
            await ws.send_text(json.dumps(await outbox.get()))

    async def do_analyze(frame: dict):
        async with inflight:
            try:
                req = AnalyzeRequest.model_validate({"text": frame.get("text"), "record": frame.get("record", True)})
                result = await rt.analyze(req.text, "ws", record=req.record)
                await outbox.put({"type": "result", "ref": frame.get("ref"), "result": result.to_dict()})
            except ValidationError as exc:
                await outbox.put({"type": "error", "ref": frame.get("ref"), "message": exc.errors()[0]["msg"]})
            except Overloaded:
                await outbox.put({"type": "error", "ref": frame.get("ref"), "message": "overloaded, retry later"})

    async def forward(sub):
        while True:
            entry = await sub.get(rt.settings.stream_heartbeat_s)
            if dropped := sub.take_dropped():
                await outbox.put({"type": "lagged", "dropped": dropped})
            if entry is not None:
                await outbox.put({"type": "event", "id": entry[0], "event": entry[1]})

    writer_task = asyncio.create_task(writer())
    try:
        while True:
            try:
                frame = json.loads(await ws.receive_text())
                kind = frame.get("type") if isinstance(frame, dict) else None
            except json.JSONDecodeError:
                await outbox.put({"type": "error", "message": "frames must be JSON objects"})
                continue
            if kind == "analyze":
                spawn(do_analyze(frame))
            elif kind == "publish":
                try:
                    batch = MessagesIn.model_validate({"messages": frame.get("messages") or [frame]})
                except ValidationError as exc:
                    await outbox.put({"type": "error", "ref": frame.get("ref"), "message": exc.errors()[0]["msg"]})
                    continue
                ids = await rt.enqueue(batch.messages)
                await outbox.put({"type": "accepted", "ref": frame.get("ref"), "ids": ids})
            elif kind == "subscribe":
                verdicts = set(frame.get("verdicts") or []) or None
                sub = rt.hub.subscribe(make_filter(frame.get("channel"), verdicts), "ws")
                subs.append(sub)
                spawn(forward(sub))
                await outbox.put({"type": "subscribed", "channel": frame.get("channel")})
            elif kind == "ping":
                await outbox.put({"type": "pong"})
            else:
                await outbox.put({"type": "error", "message": f"unknown frame type: {kind!r}"})
    except WebSocketDisconnect:
        pass
    finally:
        for sub in subs:
            sub.close()
        for task in (*tasks, writer_task):
            task.cancel()
        with contextlib.suppress(Exception):
            await asyncio.gather(*tasks, writer_task, return_exceptions=True)


# --- Feed -----------------------------------------------------------------------------------

@router.get("/stats", tags=["ops"])
async def stats(rt: RT):
    return await rt.stats()


@router.get("/posts", tags=["feed"])
async def list_posts(rt: RT, limit: Annotated[int, Query(ge=1, le=100)] = 50):
    return {"posts": await rt.list_posts(limit)}


@router.post("/posts", status_code=201, tags=["feed"])
async def create_post(body: PostIn, rt: RT):
    return await rt.create_post(body.author, body.text)


@router.post("/posts/{post_id}/comments", status_code=201, tags=["feed"])
async def create_comment(post_id: int, body: CommentIn, rt: RT):
    return await rt.add_comment(post_id, body.author, body.text)


@router.post("/posts/{post_id}/reactions", tags=["feed"])
async def react(post_id: int, body: ReactionIn, rt: RT):
    return await rt.react(post_id, body.kind)
