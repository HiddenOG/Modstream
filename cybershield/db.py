"""SQLite persistence.

Every post/comment write also appends a row to ``events`` (a transactional
outbox). The live monitor streams that table by id, which makes the stream
resumable via the SSE ``Last-Event-ID`` header and safe across workers.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone

from flask import current_app, g

SCHEMA = """
CREATE TABLE IF NOT EXISTS posts (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    author      TEXT NOT NULL,
    body        TEXT NOT NULL,
    image       TEXT,
    likes       INTEGER NOT NULL DEFAULT 0,
    shares      INTEGER NOT NULL DEFAULT 0,
    verdict     TEXT NOT NULL,
    risk        REAL NOT NULL,
    analysis    TEXT NOT NULL,
    created_at  TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS comments (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    post_id     INTEGER NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    author      TEXT NOT NULL,
    body        TEXT NOT NULL,
    verdict     TEXT NOT NULL,
    risk        REAL NOT NULL,
    analysis    TEXT NOT NULL,
    created_at  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_comments_post ON comments(post_id);

-- Metadata-only log of every analysis. Raw text from the analyzer and chat
-- tools is deliberately NOT stored.
CREATE TABLE IF NOT EXISTS scans (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source      TEXT NOT NULL,
    verdict     TEXT NOT NULL,
    risk        REAL NOT NULL,
    categories  TEXT NOT NULL,
    latency_ms  REAL NOT NULL,
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    kind        TEXT NOT NULL,
    post_id     INTEGER NOT NULL,
    comment_id  INTEGER,
    created_at  TEXT NOT NULL
);
"""


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def get_db() -> sqlite3.Connection:
    if "db" not in g:
        g.db = connect(current_app.config["DATABASE"])
    return g.db


def close_db(_exc=None) -> None:
    conn = g.pop("db", None)
    if conn is not None:
        conn.close()


def init_db(path: str) -> None:
    conn = connect(path)
    conn.execute("PRAGMA journal_mode = WAL")  # readers don't block the writer
    conn.executescript(SCHEMA)
    conn.close()


# --- Row mappers -------------------------------------------------------------

def _item(row: sqlite3.Row) -> dict:
    item = dict(row)
    item["analysis"] = json.loads(item["analysis"])
    return item


def post_dict(row: sqlite3.Row, comments: list[dict] | None = None) -> dict:
    post = _item(row)
    post["comments"] = comments or []
    return post


# --- Queries -----------------------------------------------------------------

def insert_post(db, author, body, image, analysis) -> int:
    cur = db.execute(
        "INSERT INTO posts (author, body, image, verdict, risk, analysis, created_at)"
        " VALUES (?, ?, ?, ?, ?, ?, ?)",
        (author, body, image, analysis.verdict, analysis.risk, json.dumps(analysis.to_dict()), now()),
    )
    db.execute(
        "INSERT INTO events (kind, post_id, created_at) VALUES ('post', ?, ?)",
        (cur.lastrowid, now()),
    )
    db.commit()
    return cur.lastrowid


def insert_comment(db, post_id, author, body, analysis) -> int:
    cur = db.execute(
        "INSERT INTO comments (post_id, author, body, verdict, risk, analysis, created_at)"
        " VALUES (?, ?, ?, ?, ?, ?, ?)",
        (post_id, author, body, analysis.verdict, analysis.risk, json.dumps(analysis.to_dict()), now()),
    )
    db.execute(
        "INSERT INTO events (kind, post_id, comment_id, created_at) VALUES ('comment', ?, ?, ?)",
        (post_id, cur.lastrowid, now()),
    )
    db.commit()
    return cur.lastrowid


def get_post(db, post_id: int) -> dict | None:
    row = db.execute("SELECT * FROM posts WHERE id = ?", (post_id,)).fetchone()
    if row is None:
        return None
    comments = db.execute(
        "SELECT * FROM comments WHERE post_id = ? ORDER BY id", (post_id,)
    ).fetchall()
    return post_dict(row, [_item(c) for c in comments])


def list_posts(db, limit: int = 50) -> list[dict]:
    rows = db.execute("SELECT * FROM posts ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    if not rows:
        return []
    ids = [r["id"] for r in rows]
    marks = ",".join("?" * len(ids))
    by_post: dict[int, list] = {i: [] for i in ids}
    for c in db.execute(
        f"SELECT * FROM comments WHERE post_id IN ({marks}) ORDER BY id", ids
    ).fetchall():
        by_post[c["post_id"]].append(_item(c))
    return [post_dict(r, by_post[r["id"]]) for r in rows]


def react(db, post_id: int, kind: str) -> dict | None:
    column = {"like": "likes", "share": "shares"}[kind]
    cur = db.execute(f"UPDATE posts SET {column} = {column} + 1 WHERE id = ?", (post_id,))
    db.commit()
    if cur.rowcount == 0:
        return None
    row = db.execute("SELECT likes, shares FROM posts WHERE id = ?", (post_id,)).fetchone()
    return dict(row)


def record_scan(db, source: str, analysis) -> None:
    db.execute(
        "INSERT INTO scans (source, verdict, risk, categories, latency_ms, created_at)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        (source, analysis.verdict, analysis.risk, json.dumps(analysis.categories),
         analysis.latency_ms, now()),
    )
    db.commit()


def stats(db) -> dict:
    totals = db.execute(
        "SELECT COUNT(*) AS scans,"
        " COALESCE(SUM(verdict = 'flagged'), 0) AS flagged,"
        " COALESCE(SUM(verdict = 'review'), 0) AS review,"
        " COALESCE(AVG(latency_ms), 0) AS avg_latency_ms"
        " FROM scans"
    ).fetchone()
    content = db.execute(
        "SELECT"
        " (SELECT COUNT(*) FROM posts) AS posts,"
        " (SELECT COUNT(*) FROM posts WHERE verdict = 'flagged') AS flagged_posts,"
        " (SELECT COUNT(*) FROM comments) AS comments,"
        " (SELECT COUNT(*) FROM comments WHERE verdict = 'flagged') AS flagged_comments"
    ).fetchone()
    categories: dict[str, int] = {}
    for (raw,) in db.execute("SELECT categories FROM scans WHERE verdict != 'safe'"):
        for cat in json.loads(raw):
            categories[cat] = categories.get(cat, 0) + 1
    by_source = {
        r["source"]: r["n"]
        for r in db.execute("SELECT source, COUNT(*) AS n FROM scans GROUP BY source")
    }
    result = dict(totals) | dict(content)
    result["avg_latency_ms"] = round(result["avg_latency_ms"], 1)
    result["flag_rate"] = round(result["flagged"] / result["scans"], 4) if result["scans"] else 0
    result["categories"] = dict(sorted(categories.items(), key=lambda kv: -kv[1]))
    result["by_source"] = by_source
    return result


def events_since(db, last_id: int, limit: int = 100) -> list[dict]:
    """Return events after ``last_id`` with their post/comment payloads."""
    rows = db.execute(
        "SELECT * FROM events WHERE id > ? ORDER BY id LIMIT ?", (last_id, limit)
    ).fetchall()
    out = []
    for e in rows:
        if e["kind"] == "post":
            row = db.execute("SELECT * FROM posts WHERE id = ?", (e["post_id"],)).fetchone()
            payload = post_dict(row) if row else None
        else:
            row = db.execute("SELECT * FROM comments WHERE id = ?", (e["comment_id"],)).fetchone()
            payload = _item(row) if row else None
        if payload is not None:
            out.append({"id": e["id"], "type": e["kind"], "data": payload})
    return out


def last_event_id(db) -> int:
    return db.execute("SELECT COALESCE(MAX(id), 0) FROM events").fetchone()[0]
