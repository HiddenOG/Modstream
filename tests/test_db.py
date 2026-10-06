import sqlite3

from modstream import db

# The tables as they were before workspaces existed.
OLD_SCHEMA = """
CREATE TABLE posts (id INTEGER PRIMARY KEY AUTOINCREMENT, author VARCHAR(40), body TEXT, image VARCHAR(64),
    likes INTEGER, shares INTEGER, verdict VARCHAR(10), risk FLOAT, analysis JSON, created_at DATETIME);
CREATE TABLE comments (id INTEGER PRIMARY KEY AUTOINCREMENT, post_id INTEGER REFERENCES posts(id) ON DELETE CASCADE,
    author VARCHAR(40), body TEXT, verdict VARCHAR(10), risk FLOAT, analysis JSON, created_at DATETIME);
CREATE TABLE messages (id INTEGER PRIMARY KEY AUTOINCREMENT, entry_id VARCHAR(64) UNIQUE, channel VARCHAR(64),
    author VARCHAR(40), body TEXT, verdict VARCHAR(10), risk FLOAT, analysis JSON, enqueued_at DATETIME,
    created_at DATETIME);
INSERT INTO posts (author, body, likes, shares, verdict, risk, analysis, created_at)
    VALUES ('old', 'from before workspaces', 0, 0, 'safe', 0.0, '{}', '2026-10-01 10:00:00');
"""


async def test_old_database_is_upgraded_in_place(tmp_path):
    path = tmp_path / "old.db"
    with sqlite3.connect(path) as conn:
        conn.executescript(OLD_SCHEMA)

    engine = db.make_engine(f"sqlite+aiosqlite:///{path.as_posix()}")
    await db.create_schema(engine)
    await db.create_schema(engine)  # running twice is harmless
    async with db.make_sessionmaker(engine)() as s:
        posts = await db.list_posts(s, db.PUBLIC)
    await engine.dispose()

    assert [p["body"] for p in posts] == ["from before workspaces"]  # existing rows kept, now "public"
    with sqlite3.connect(path) as conn:
        for table in ("posts", "comments", "messages"):
            assert "workspace" in [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]
        assert "ix_messages_workspace_id" in [r[1] for r in conn.execute("PRAGMA index_list(messages)")]
