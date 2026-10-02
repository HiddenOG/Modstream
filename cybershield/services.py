"""Business logic shared by the HTML pages and the JSON API."""

from __future__ import annotations

import os
import uuid

from flask import current_app

from . import db as store
from .detector import Analysis, Detector

MAX_TEXT = 5000
MAX_AUTHOR = 40
MAX_BATCH = 32
SCAN_SOURCES = {"api", "analyze", "chat", "composer", "home"}


class ValidationError(Exception):
    def __init__(self, message: str, field: str | None = None, status: int = 400):
        super().__init__(message)
        self.message, self.field, self.status = message, field, status


class NotFound(ValidationError):
    def __init__(self, message: str):
        super().__init__(message, status=404)


def detector() -> Detector:
    return current_app.extensions["detector"]


# --- Validation --------------------------------------------------------------

def clean_text(value, field: str = "text", max_len: int = MAX_TEXT) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"'{field}' is required", field)
    value = value.strip()
    if len(value) > max_len:
        raise ValidationError(f"'{field}' must be at most {max_len} characters", field)
    return value


def clean_author(value) -> str:
    value = (value or "").strip() if isinstance(value, str) else ""
    return value[:MAX_AUTHOR] or "Anonymous"


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


def save_image(file) -> str | None:
    """Validate an upload by its magic bytes and store it under a random name."""
    if file is None or not file.filename:
        return None
    ext = _sniff_image(file.stream.read(16))
    file.stream.seek(0)
    if ext is None:
        raise ValidationError("Image must be a PNG, JPEG, GIF or WebP file", "image")
    name = f"{uuid.uuid4().hex}.{ext}"
    file.save(os.path.join(current_app.config["UPLOAD_FOLDER"], name))
    return name


# --- Use cases ---------------------------------------------------------------

def analyze(text, source: str = "api", record: bool = True) -> Analysis:
    """Analyze text. ``record=False`` is used for pre-send checks so the same
    message isn't counted twice in stats once it is actually posted."""
    text = clean_text(text)
    result = detector().analyze(text)
    if record:
        store.record_scan(store.get_db(), source if source in SCAN_SOURCES else "api", result)
    return result


def analyze_batch(texts, source: str = "api") -> list[Analysis]:
    if not isinstance(texts, list) or not texts:
        raise ValidationError("'texts' must be a non-empty array", "texts")
    if len(texts) > MAX_BATCH:
        raise ValidationError(f"'texts' accepts at most {MAX_BATCH} items", "texts")
    cleaned = [clean_text(t, f"texts[{i}]") for i, t in enumerate(texts)]
    results = detector().analyze_many(cleaned)
    conn = store.get_db()
    for result in results:
        store.record_scan(conn, source if source in SCAN_SOURCES else "api", result)
    return results


def create_post(author, body, image_file=None) -> dict:
    body = clean_text(body, "text")
    author = clean_author(author)
    result = detector().analyze(body)
    image = save_image(image_file)
    conn = store.get_db()
    store.record_scan(conn, "post", result)
    post_id = store.insert_post(conn, author, body, image, result)
    return store.get_post(conn, post_id)


def add_comment(post_id: int, author, body) -> dict:
    body = clean_text(body, "comment", max_len=1000)
    author = clean_author(author)
    conn = store.get_db()
    if store.get_post(conn, post_id) is None:
        raise NotFound(f"Post {post_id} not found")
    result = detector().analyze(body)
    store.record_scan(conn, "comment", result)
    comment_id = store.insert_comment(conn, post_id, author, body, result)
    post = store.get_post(conn, post_id)
    return next(c for c in post["comments"] if c["id"] == comment_id)


def react(post_id: int, kind: str) -> dict:
    if kind not in ("like", "share"):
        raise ValidationError("'kind' must be 'like' or 'share'", "kind")
    counts = store.react(store.get_db(), post_id, kind)
    if counts is None:
        raise NotFound(f"Post {post_id} not found")
    return counts
