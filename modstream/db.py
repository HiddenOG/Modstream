"""Async persistence with SQLAlchemy 2.0: Postgres in production, SQLite locally."""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime

from sqlalchemy import (
    JSON,
    BigInteger,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    delete,
    event,
    func,
    select,
    update,
)
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# BIGINT ids on Postgres; SQLite only auto-increments a column declared INTEGER.
Id = BigInteger().with_variant(Integer, "sqlite")


def utcnow() -> datetime:
    return datetime.now(UTC)


class Base(DeclarativeBase):
    def to_dict(self) -> dict:
        out = {}
        for col in self.__table__.columns:
            value = getattr(self, col.key)
            if isinstance(value, datetime):
                # SQLite drops the timezone on read; every timestamp is written in UTC, so restore it.
                # Without this, browsers parse the time as local and show e.g. "1 hr. ago" for new posts.
                value = (value if value.tzinfo else value.replace(tzinfo=UTC)).isoformat()
            out[col.key] = value
        return out


PUBLIC = "public"  # workspace for rows written before workspaces existed


class Post(Base):
    __tablename__ = "posts"
    id: Mapped[int] = mapped_column(Id, primary_key=True, autoincrement=True)
    workspace: Mapped[str] = mapped_column(String(32), default=PUBLIC, server_default=PUBLIC, index=True)
    author: Mapped[str] = mapped_column(String(40))
    body: Mapped[str] = mapped_column(Text)
    image: Mapped[str | None] = mapped_column(String(64))
    likes: Mapped[int] = mapped_column(Integer, default=0)
    shares: Mapped[int] = mapped_column(Integer, default=0)
    verdict: Mapped[str] = mapped_column(String(10), index=True)
    risk: Mapped[float] = mapped_column(Float)
    analysis: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class Comment(Base):
    __tablename__ = "comments"
    id: Mapped[int] = mapped_column(Id, primary_key=True, autoincrement=True)
    post_id: Mapped[int] = mapped_column(ForeignKey("posts.id", ondelete="CASCADE"), index=True)
    workspace: Mapped[str] = mapped_column(String(32), default=PUBLIC, server_default=PUBLIC, index=True)
    author: Mapped[str] = mapped_column(String(40))
    body: Mapped[str] = mapped_column(Text)
    verdict: Mapped[str] = mapped_column(String(10), index=True)
    risk: Mapped[float] = mapped_column(Float)
    analysis: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class Message(Base):
    """A message from an external stream, moderated asynchronously by workers."""

    __tablename__ = "messages"
    id: Mapped[int] = mapped_column(Id, primary_key=True, autoincrement=True)
    entry_id: Mapped[str] = mapped_column(String(64), unique=True)  # broker id: makes redelivery idempotent
    workspace: Mapped[str] = mapped_column(String(32), default=PUBLIC, server_default=PUBLIC)
    channel: Mapped[str] = mapped_column(String(64))
    author: Mapped[str] = mapped_column(String(40))
    body: Mapped[str] = mapped_column(Text)
    verdict: Mapped[str] = mapped_column(String(10))
    risk: Mapped[float] = mapped_column(Float)
    analysis: Mapped[dict] = mapped_column(JSON)
    enqueued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    __table_args__ = (
        Index("ix_messages_workspace_id", "workspace", "id"),
        Index("ix_messages_channel_id", "channel", "id"),
        Index("ix_messages_verdict_id", "verdict", "id"),
    )


# --- Engine --------------------------------------------------------------------

def make_engine(url: str) -> AsyncEngine:
    if url.startswith("sqlite"):
        path = url.split("///", 1)[-1]
        if path and path != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        engine = create_async_engine(url, connect_args={"timeout": 30})

        @event.listens_for(engine.sync_engine, "connect")
        def _pragmas(conn, _record):
            cur = conn.cursor()
            cur.execute("PRAGMA journal_mode=WAL")
            cur.execute("PRAGMA synchronous=NORMAL")
            cur.execute("PRAGMA foreign_keys=ON")
            cur.close()

        return engine
    return create_async_engine(url, pool_size=10, max_overflow=20, pool_pre_ping=True)


def make_sessionmaker(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


async def create_schema(engine: AsyncEngine, attempts: int = 5) -> None:
    """Create missing tables. Safe when several processes or replicas boot at once:
    a peer winning the race surfaces as "already exists", so we re-check and retry."""
    for attempt in range(attempts):
        try:
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
                await conn.run_sync(_add_missing_columns)
            return
        except DBAPIError as exc:
            if attempt == attempts - 1 or not any(
                s in str(exc).lower() for s in ("already exists", "duplicate key", "database is locked")
            ):
                raise
            await asyncio.sleep(0.2 * (attempt + 1))


def _add_missing_columns(conn) -> None:
    """Upgrade databases created before workspaces existed: add the column (existing rows
    land in the "public" workspace) and its indexes. A stand-in until real migrations."""
    from sqlalchemy import inspect, text

    inspector = inspect(conn)
    for table in ("posts", "comments", "messages"):
        if "workspace" not in {c["name"] for c in inspector.get_columns(table)}:
            conn.execute(text(f"ALTER TABLE {table} ADD COLUMN workspace VARCHAR(32) NOT NULL DEFAULT '{PUBLIC}'"))
    for index in (*Post.__table__.indexes, *Comment.__table__.indexes, *Message.__table__.indexes):
        index.create(conn, checkfirst=True)


# --- Posts & comments ---------------------------------------------------------------
# Every query takes the caller's workspace: a visitor only ever sees and changes their own rows.

def _with_comments(post: Post, comments: list[Comment]) -> dict:
    out = post.to_dict()
    out["comments"] = [c.to_dict() for c in comments]
    return out


async def insert_post(s: AsyncSession, ws: str, author: str, body: str, image: str | None, analysis) -> dict:
    post = Post(workspace=ws, author=author, body=body, image=image, verdict=analysis.verdict,
                risk=analysis.risk, analysis=analysis.to_dict(), likes=0, shares=0)
    s.add(post)
    await s.commit()
    return _with_comments(post, [])


async def find_post(s: AsyncSession, ws: str, post_id: int) -> Post | None:
    return (await s.scalars(select(Post).where(Post.id == post_id, Post.workspace == ws))).first()


async def get_post(s: AsyncSession, ws: str, post_id: int) -> dict | None:
    post = await find_post(s, ws, post_id)
    if post is None:
        return None
    comments = (await s.scalars(select(Comment).where(Comment.post_id == post_id).order_by(Comment.id))).all()
    return _with_comments(post, list(comments))


async def list_posts(s: AsyncSession, ws: str, limit: int = 50) -> list[dict]:
    posts = (await s.scalars(select(Post).where(Post.workspace == ws).order_by(Post.id.desc()).limit(limit))).all()
    if not posts:
        return []
    by_post: dict[int, list] = {p.id: [] for p in posts}
    comments = await s.scalars(select(Comment).where(Comment.post_id.in_(by_post)).order_by(Comment.id))
    for c in comments:
        by_post[c.post_id].append(c)
    return [_with_comments(p, by_post[p.id]) for p in posts]


async def insert_comment(s: AsyncSession, ws: str, post_id: int, author: str, body: str, analysis) -> dict:
    comment = Comment(workspace=ws, post_id=post_id, author=author, body=body, verdict=analysis.verdict,
                      risk=analysis.risk, analysis=analysis.to_dict())
    s.add(comment)
    await s.commit()
    return comment.to_dict()


async def react(s: AsyncSession, ws: str, post_id: int, kind: str) -> dict | None:
    column = {"like": Post.likes, "share": Post.shares}[kind]
    result = await s.execute(
        update(Post).where(Post.id == post_id, Post.workspace == ws)
        .values({column: column + 1}).returning(Post.likes, Post.shares)
    )
    row = result.first()
    await s.commit()
    return {"likes": row.likes, "shares": row.shares} if row else None


async def delete_post(s: AsyncSession, ws: str, post_id: int) -> str | None:
    """Delete one post and its comments. Returns its image file name (if any) or None if not found."""
    post = await find_post(s, ws, post_id)
    if post is None:
        return None
    image = post.image or ""
    await s.execute(delete(Comment).where(Comment.post_id == post_id))
    await s.execute(delete(Post).where(Post.id == post_id))
    await s.commit()
    return image


async def clear_posts(s: AsyncSession, ws: str) -> list[str]:
    """Delete every post and comment in a workspace. Returns image file names to remove."""
    images = [i for i in (await s.scalars(select(Post.image).where(Post.workspace == ws))).all() if i]
    await s.execute(delete(Comment).where(Comment.workspace == ws))
    await s.execute(delete(Post).where(Post.workspace == ws))
    await s.commit()
    return images


async def content_counts(s: AsyncSession, ws: str) -> dict:
    def count(model, *where):
        return select(func.count()).select_from(model).where(model.workspace == ws, *where).scalar_subquery()

    row = (await s.execute(select(
        count(Post).label("posts"),
        count(Post, Post.verdict == "flagged").label("flagged_posts"),
        count(Comment).label("comments"),
        count(Comment, Comment.verdict == "flagged").label("flagged_comments"),
    ))).one()
    return dict(row._mapping)


async def purge_older_than(s: AsyncSession, cutoff: datetime) -> list[str]:
    """Delete demo rows created before ``cutoff``; returns image file names to remove."""
    images = [i for i in (await s.scalars(select(Post.image).where(Post.created_at < cutoff))).all() if i]
    await s.execute(delete(Comment).where(Comment.post_id.in_(select(Post.id).where(Post.created_at < cutoff))))
    await s.execute(delete(Comment).where(Comment.created_at < cutoff))
    await s.execute(delete(Post).where(Post.created_at < cutoff))
    await s.execute(delete(Message).where(Message.created_at < cutoff))
    await s.commit()
    return images


# --- Stream messages -----------------------------------------------------------------

def _upsert(s: AsyncSession):
    if s.bind.dialect.name == "postgresql":
        from sqlalchemy.dialects.postgresql import insert
    else:
        from sqlalchemy.dialects.sqlite import insert
    return insert


async def insert_messages(s: AsyncSession, rows: list[dict]) -> dict[str, int]:
    """Bulk insert, ignoring entries already stored by an earlier delivery.
    Returns ``entry_id -> message id`` for every row in the batch."""
    if not rows:
        return {}
    insert = _upsert(s)
    await s.execute(insert(Message).values(rows).on_conflict_do_nothing(index_elements=["entry_id"]))
    await s.commit()
    ids = await s.execute(
        select(Message.entry_id, Message.id).where(Message.entry_id.in_([r["entry_id"] for r in rows]))
    )
    return {entry_id: msg_id for entry_id, msg_id in ids}


async def list_messages(
    s: AsyncSession, ws: str, channel: str | None, verdict: str | None, limit: int, before_id: int | None = None,
) -> list[dict]:
    """Newest first. Pass the smallest id you have as ``before_id`` to page further back."""
    q = select(Message).where(Message.workspace == ws).order_by(Message.id.desc()).limit(limit)
    if channel:
        q = q.where(Message.channel == channel)
    if verdict:
        q = q.where(Message.verdict == verdict)
    if before_id is not None:
        q = q.where(Message.id < before_id)
    return [m.to_dict() for m in (await s.scalars(q)).all()]


async def message_counts(s: AsyncSession, ws: str) -> dict:
    row = (await s.execute(select(
        func.count().label("messages"),
        func.count().filter(Message.verdict == "flagged").label("flagged_messages"),
    ).where(Message.workspace == ws))).one()
    return dict(row._mapping)


async def delete_messages(s: AsyncSession, ws: str) -> None:
    await s.execute(delete(Message).where(Message.workspace == ws))
    await s.commit()
