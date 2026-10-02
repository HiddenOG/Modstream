"""Async persistence with SQLAlchemy 2.0: Postgres in production, SQLite locally."""

from __future__ import annotations

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
    event,
    func,
    select,
    update,
)
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
            out[col.key] = value.isoformat() if isinstance(value, datetime) else value
        return out


class Post(Base):
    __tablename__ = "posts"
    id: Mapped[int] = mapped_column(Id, primary_key=True, autoincrement=True)
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
    channel: Mapped[str] = mapped_column(String(64))
    author: Mapped[str] = mapped_column(String(40))
    body: Mapped[str] = mapped_column(Text)
    verdict: Mapped[str] = mapped_column(String(10))
    risk: Mapped[float] = mapped_column(Float)
    analysis: Mapped[dict] = mapped_column(JSON)
    enqueued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    __table_args__ = (
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


async def create_schema(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


# --- Posts & comments ---------------------------------------------------------------

def _with_comments(post: Post, comments: list[Comment]) -> dict:
    out = post.to_dict()
    out["comments"] = [c.to_dict() for c in comments]
    return out


async def insert_post(s: AsyncSession, author: str, body: str, image: str | None, analysis) -> dict:
    post = Post(author=author, body=body, image=image, verdict=analysis.verdict,
                risk=analysis.risk, analysis=analysis.to_dict(), likes=0, shares=0)
    s.add(post)
    await s.commit()
    return _with_comments(post, [])


async def get_post(s: AsyncSession, post_id: int) -> dict | None:
    post = await s.get(Post, post_id)
    if post is None:
        return None
    comments = (await s.scalars(select(Comment).where(Comment.post_id == post_id).order_by(Comment.id))).all()
    return _with_comments(post, list(comments))


async def list_posts(s: AsyncSession, limit: int = 50) -> list[dict]:
    posts = (await s.scalars(select(Post).order_by(Post.id.desc()).limit(limit))).all()
    if not posts:
        return []
    by_post: dict[int, list] = {p.id: [] for p in posts}
    comments = await s.scalars(select(Comment).where(Comment.post_id.in_(by_post)).order_by(Comment.id))
    for c in comments:
        by_post[c.post_id].append(c)
    return [_with_comments(p, by_post[p.id]) for p in posts]


async def insert_comment(s: AsyncSession, post_id: int, author: str, body: str, analysis) -> dict:
    comment = Comment(post_id=post_id, author=author, body=body, verdict=analysis.verdict,
                      risk=analysis.risk, analysis=analysis.to_dict())
    s.add(comment)
    await s.commit()
    return comment.to_dict()


async def react(s: AsyncSession, post_id: int, kind: str) -> dict | None:
    column = {"like": Post.likes, "share": Post.shares}[kind]
    result = await s.execute(
        update(Post).where(Post.id == post_id).values({column: column + 1}).returning(Post.likes, Post.shares)
    )
    row = result.first()
    await s.commit()
    return {"likes": row.likes, "shares": row.shares} if row else None


async def content_counts(s: AsyncSession) -> dict:
    row = (await s.execute(select(
        select(func.count()).select_from(Post).scalar_subquery().label("posts"),
        select(func.count()).select_from(Post).where(Post.verdict == "flagged").scalar_subquery()
        .label("flagged_posts"),
        select(func.count()).select_from(Comment).scalar_subquery().label("comments"),
        select(func.count()).select_from(Comment).where(Comment.verdict == "flagged").scalar_subquery()
        .label("flagged_comments"),
    ))).one()
    return dict(row._mapping)


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


async def list_messages(s: AsyncSession, channel: str | None, verdict: str | None, limit: int) -> list[dict]:
    q = select(Message).order_by(Message.id.desc()).limit(limit)
    if channel:
        q = q.where(Message.channel == channel)
    if verdict:
        q = q.where(Message.verdict == verdict)
    return [m.to_dict() for m in (await s.scalars(q)).all()]
