"""End-to-end smoke test against a running Modstream server (local or deployed).

    python scripts/smoke.py                         # http://127.0.0.1:8000
    python scripts/smoke.py https://your-deployment.example.com

Exercises every page and API feature over real HTTP, SSE and WebSocket
connections. Content it creates is tagged with the author "smoke-test".
Exits non-zero if any check fails.
"""

from __future__ import annotations

import asyncio
import base64
import json
import sys
import time
import uuid

import httpx
import websockets

# A valid 1x1 transparent PNG.
PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII="
)
AUTHOR = "smoke-test"


class Smoke:
    def __init__(self, base: str):
        self.base = base.rstrip("/")
        self.ws_url = self.base.replace("http", "ws", 1) + "/api/v1/ws"
        self.http = httpx.AsyncClient(base_url=self.base, timeout=20, follow_redirects=False)
        self.results: list[tuple[str, bool, str]] = []
        self.tag = uuid.uuid4().hex[:8]

    async def check(self, name: str, coro) -> None:
        started = time.perf_counter()
        try:
            detail = await coro or ""
            ok = True
        except Exception as exc:  # noqa: BLE001 - report every failure, keep going
            detail, ok = f"{type(exc).__name__}: {exc}", False
        ms = (time.perf_counter() - started) * 1000
        self.results.append((name, ok, detail))
        print(f"  {'PASS' if ok else 'FAIL'}  {name:<46} {ms:7.0f} ms  {detail}")

    async def get_json(self, path: str, **kw):
        res = await self.http.get(path, **kw)
        res.raise_for_status()
        return res.json()

    async def post_json(self, path: str, body: dict, expect: int = 200):
        res = await self.http.post(path, json=body)
        assert res.status_code == expect, f"{path} -> {res.status_code}: {res.text[:200]}"
        return res.json()

    # --- health -----------------------------------------------------------------------

    async def ready(self):
        for _ in range(60):
            res = await self.http.get("/api/v1/ready")
            if res.status_code == 200:
                checks = res.json()["checks"]
                return f"model={checks['model']} broker={checks['broker']} db={checks['database']}"
            await asyncio.sleep(1)
        raise AssertionError(f"not ready after 60s: {res.text}")

    async def health(self):
        body = await self.get_json("/api/v1/health")
        assert body["status"] == "ok"
        return f"v{body['version']} engine={body['engine']}"

    async def docs(self):
        spec = await self.get_json("/openapi.json")
        assert "/api/v1/messages" in spec["paths"]
        assert (await self.http.get("/docs")).status_code == 200
        return f"{len(spec['paths'])} documented paths"

    async def metrics(self):
        res = await self.http.get("/metrics/")
        assert res.status_code == 200 and "modstream_analyses_total" in res.text
        return f"{sum(1 for line in res.text.splitlines() if line.startswith('modstream_'))} series"

    # --- pages ----------------------------------------------------------------------------

    async def pages(self):
        for path in ("/", "/analyze", "/feed", "/monitor", "/chat", "/resources", "/about"):
            res = await self.http.get(path)
            assert res.status_code == 200, f"{path} -> {res.status_code}"
            assert "Modstream" in res.text and 'name="viewport"' in res.text, f"{path} content"
            for header in ("x-content-type-options", "x-frame-options"):
                assert header in res.headers, f"{path} missing {header}"
        return "7 pages, security headers present"

    async def assets(self):
        for path in ("/static/css/app.css", "/static/js/core.js"):
            res = await self.http.get(path)
            assert res.status_code == 200 and len(res.content) > 1000, f"{path} -> {res.status_code}"
        return "css + js"

    async def redirects(self):
        legacy = {"/paste": "/analyze", "/facebook": "/feed", "/facebook/live": "/monitor",
                  "/chatbot": "/chat", "/actions": "/resources", "/help": "/about"}
        for old, new in legacy.items():
            res = await self.http.get(old)
            assert res.status_code == 301 and res.headers["location"] == new, f"{old} -> {res.status_code}"
        return f"{len(legacy)} legacy URLs"

    async def not_found(self):
        page = await self.http.get(f"/missing-{self.tag}")
        api = await self.http.get(f"/api/v1/missing-{self.tag}")
        assert page.status_code == 404 and "Back home" in page.text
        assert api.status_code == 404 and api.json()["error"]["code"] == "not_found"
        return "HTML page + JSON error"

    # --- detection ----------------------------------------------------------------------

    async def analyze(self):
        cases = {
            "Great game today, proud of the team!": "safe",
            "that exam was stupid hard": "review",
            "you're such a loser, nobody likes you": "flagged",
            "ur an 1d10t": "flagged",  # leetspeak
        }
        for text, expected in cases.items():
            body = await self.post_json("/api/v1/analyze", {"text": text, "record": False})
            assert body["verdict"] == expected, f"{text!r}: {body['verdict']} != {expected}"
        hit = (await self.post_json("/api/v1/analyze", {"text": "ur an 1d10t", "record": False}))["matches"][0]
        assert "ur an 1d10t"[hit["start"]:hit["end"]] == "1d10t", "highlight offsets"
        return f"{len(cases)} verdicts correct, offsets map to original text"

    async def validation(self):
        res = await self.http.post("/api/v1/analyze", json={"text": "  "})
        assert res.status_code == 422 and res.json()["error"]["field"] == "text"
        return "422 with field"

    async def batch(self):
        body = await self.post_json("/api/v1/analyze/batch", {"texts": ["hello", "you idiot"]})
        assert [r["verdict"] for r in body["results"]] == ["safe", "flagged"]
        return "2 results"

    # --- feed -----------------------------------------------------------------------------

    async def feed_api(self):
        post = await self.post_json("/api/v1/posts", {"author": AUTHOR, "text": f"hello from smoke {self.tag}"}, 201)
        comment = await self.post_json(
            f"/api/v1/posts/{post['id']}/comments", {"author": AUTHOR, "text": "you idiot"}, 201
        )
        assert comment["verdict"] == "flagged"
        counts = await self.post_json(f"/api/v1/posts/{post['id']}/reactions", {"kind": "like"})
        assert counts["likes"] >= 1
        page = (await self.http.get("/feed")).text
        assert f"hello from smoke {self.tag}" in page and "you idiot" in page
        return f"post #{post['id']} + flagged comment + like, visible on /feed"

    async def feed_form_upload(self):
        res = await self.http.post("/feed", data={"author": AUTHOR, "text": f"pic {self.tag}"},
                                   files={"image": ("smoke.png", PNG, "image/png")})
        assert res.status_code == 303, f"form -> {res.status_code}"
        posts = (await self.get_json("/api/v1/posts"))["posts"]
        post = next(p for p in posts if p["body"] == f"pic {self.tag}")
        img = await self.http.get(f"/uploads/{post['image']}")
        assert img.status_code == 200 and img.content == PNG, f"upload -> {img.status_code}"
        bad = await self.http.post("/feed", data={"text": f"bad {self.tag}"},
                                   files={"image": ("x.png", b"<script>", "image/png")})
        assert bad.status_code == 303
        assert not any(p["body"] == f"bad {self.tag}" for p in (await self.get_json("/api/v1/posts"))["posts"])
        return "image stored + served; non-image rejected"

    async def content_warning(self):
        await self.http.post("/feed", data={"author": AUTHOR, "text": f"you are a loser {self.tag}"})
        page = (await self.http.get("/feed")).text
        assert "cw is-hidden" in page
        return "flagged post blurred"

    # --- streaming -----------------------------------------------------------------------

    async def sse(self):
        body = f"sse check {self.tag}"

        async def listen():
            async with self.http.stream("GET", "/api/v1/stream", params={"channel": "feed"}, timeout=30) as res:
                assert res.headers["content-type"].startswith("text/event-stream")
                async for line in res.aiter_lines():
                    if line.startswith("data: ") and body in line:
                        return json.loads(line[6:])

        task = asyncio.create_task(listen())
        await asyncio.sleep(0.5)
        await self.post_json("/api/v1/posts", {"author": AUTHOR, "text": body}, 201)
        event = await asyncio.wait_for(task, 15)
        return f"post delivered live (verdict={event['verdict']})"

    async def messages_pipeline(self):
        channel = f"smoke-{self.tag}"
        msgs = [{"channel": channel, "author": AUTHOR, "text": t}
                for t in ("good luck tomorrow", "you idiot", "see you later", "nobody likes you")]
        accepted = await self.post_json("/api/v1/messages", {"messages": msgs}, 202)
        assert accepted["accepted"] == 4
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            stored = (await self.get_json("/api/v1/messages", params={"channel": channel}))["messages"]
            if len(stored) == 4:
                flagged = sum(m["verdict"] == "flagged" for m in stored)
                assert flagged == 2, f"{flagged} flagged"
                return "4 queued -> moderated by worker -> stored (2 flagged)"
            await asyncio.sleep(0.25)
        raise AssertionError(f"only {len(stored)}/4 processed in 20s (is a worker running?)")

    async def websocket(self):
        channel = f"ws-{self.tag}"
        async with websockets.connect(self.ws_url, open_timeout=15) as ws:
            async def recv():
                return json.loads(await asyncio.wait_for(ws.recv(), 15))

            await ws.send(json.dumps({"type": "subscribe", "channel": channel}))
            assert (await recv())["type"] == "subscribed"
            await ws.send(json.dumps({"type": "analyze", "ref": "a", "text": "you idiot", "record": False}))
            result = await recv()
            assert result["type"] == "result" and result["result"]["verdict"] == "flagged"
            await ws.send(json.dumps({"type": "publish", "ref": "p", "channel": channel, "author": AUTHOR,
                                      "text": "hello over websocket"}))
            frames = [await recv(), await recv()]
            kinds = {f["type"] for f in frames}
            assert kinds == {"accepted", "event"}, kinds
            event = next(f for f in frames if f["type"] == "event")
            assert event["event"]["data"]["body"] == "hello over websocket"
            await ws.send(json.dumps({"type": "ping"}))
            assert (await recv())["type"] == "pong"
        return "analyze + publish -> live event + ping"

    async def stats(self):
        body = await self.get_json("/api/v1/stats")
        for key in ("scans", "flag_rate", "categories", "messages", "queue", "posts"):
            assert key in body, key
        return f"{body['scans']} scans, flag rate {body['flag_rate']:.0%}, queue lag {body['queue']['lag']}"

    async def run(self) -> int:
        print(f"Smoke testing {self.base}\n")
        steps = [
            ("readiness (model, broker, database)", self.ready()),
            ("health", self.health()),
            ("OpenAPI docs", self.docs()),
            ("Prometheus metrics", self.metrics()),
            ("all pages render", self.pages()),
            ("static assets", self.assets()),
            ("legacy URL redirects", self.redirects()),
            ("404 handling", self.not_found()),
            ("analyze: verdicts + highlighting", self.analyze()),
            ("analyze: validation", self.validation()),
            ("analyze: batch", self.batch()),
            ("feed: post, comment, react (API)", self.feed_api()),
            ("feed: form + image upload", self.feed_form_upload()),
            ("feed: content warning", self.content_warning()),
            ("live stream (SSE)", self.sse()),
            ("message queue -> worker -> storage", self.messages_pipeline()),
            ("WebSocket protocol", self.websocket()),
            ("stats", self.stats()),
        ]
        for name, coro in steps:
            await self.check(name, coro)
        await self.http.aclose()
        failed = [r for r in self.results if not r[1]]
        print(f"\n{len(self.results) - len(failed)}/{len(self.results)} checks passed")
        return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(Smoke(sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000").run()))
