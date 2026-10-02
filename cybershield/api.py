"""Versioned JSON API: /api/v1."""

from __future__ import annotations

import json
import time

from flask import Blueprint, Response, current_app, jsonify, request
from werkzeug.exceptions import HTTPException

from . import __version__, services
from . import db as store

bp = Blueprint("api", __name__, url_prefix="/api/v1")


def _json_body() -> dict:
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        raise services.ValidationError("Request body must be a JSON object")
    return data


@bp.errorhandler(services.ValidationError)
def _validation_error(err: services.ValidationError):
    code = "not_found" if err.status == 404 else "invalid_request"
    return jsonify(error={"code": code, "message": err.message, "field": err.field}), err.status


@bp.errorhandler(HTTPException)
def _http_error(err: HTTPException):
    code = (err.name or "error").lower().replace(" ", "_")
    return jsonify(error={"code": code, "message": err.description}), err.code


@bp.get("/health")
def health():
    detector = services.detector()
    return jsonify(
        status="ok",
        version=__version__,
        engine=detector.engine,
        model_ready=getattr(detector.scorer, "ready", True),
    )


@bp.post("/analyze")
def analyze():
    body = _json_body()
    result = services.analyze(
        body.get("text"), body.get("source", "api"), record=body.get("record") is not False
    )
    return jsonify(result.to_dict())


@bp.post("/analyze/batch")
def analyze_batch():
    body = _json_body()
    results = services.analyze_batch(body.get("texts"), body.get("source", "api"))
    return jsonify(results=[r.to_dict() for r in results])


@bp.get("/stats")
def stats():
    return jsonify(store.stats(store.get_db()))


@bp.get("/posts")
def list_posts():
    limit = min(request.args.get("limit", 50, type=int), 100)
    return jsonify(posts=store.list_posts(store.get_db(), limit))


@bp.post("/posts")
def create_post():
    if request.mimetype == "application/json":
        body = _json_body()
        post = services.create_post(body.get("author"), body.get("text"))
    else:
        post = services.create_post(
            request.form.get("author"), request.form.get("text"), request.files.get("image")
        )
    return jsonify(post), 201


@bp.post("/posts/<int:post_id>/comments")
def create_comment(post_id: int):
    body = _json_body()
    comment = services.add_comment(post_id, body.get("author"), body.get("text"))
    return jsonify(comment), 201


@bp.post("/posts/<int:post_id>/reactions")
def react(post_id: int):
    body = _json_body()
    return jsonify(services.react(post_id, body.get("kind")))


@bp.get("/stream")
def stream():
    """Server-Sent Events feed of new posts and comments.

    Resumable: browsers send ``Last-Event-ID`` on reconnect and receive only
    what they missed. Connections are recycled after STREAM_MAX_SECONDS so a
    worker thread is never held forever; EventSource reconnects automatically.
    """
    cfg = current_app.config
    path, poll = cfg["DATABASE"], cfg["STREAM_POLL_SECONDS"]
    max_seconds, backlog = cfg["STREAM_MAX_SECONDS"], cfg["STREAM_BACKLOG"]
    resume = request.headers.get("Last-Event-ID", type=int)

    def generate():
        conn = store.connect(path)
        try:
            last = resume if resume is not None else max(store.last_event_id(conn) - backlog, 0)
            yield "retry: 3000\n\n"
            started = idle = time.monotonic()
            while True:
                for event in store.events_since(conn, last):
                    last = event["id"]
                    idle = time.monotonic()
                    yield f"id: {event['id']}\nevent: {event['type']}\ndata: {json.dumps(event['data'])}\n\n"
                if time.monotonic() - started >= max_seconds:
                    return
                if time.monotonic() - idle >= 15:
                    idle = time.monotonic()
                    yield ": keep-alive\n\n"
                time.sleep(poll)
        finally:
            conn.close()

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
