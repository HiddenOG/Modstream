import json
import time

from fastapi.testclient import TestClient

from modstream import create_app


def sse_events(body: str) -> list[tuple[str, str, dict]]:
    out = []
    for block in body.split("\n\n"):
        fields = dict(line.split(": ", 1) for line in block.splitlines() if ": " in line and not line.startswith(":"))
        if "data" in fields and "event" in fields:
            out.append((fields.get("id"), fields["event"], json.loads(fields["data"])))
    return out


def wait_for(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if result := predicate():
            return result
        time.sleep(0.05)
    raise AssertionError("condition not met in time")


# --- health & docs ---

def test_health_and_readiness(client):
    assert client.get("/api/v1/health").json()["engine"] == "fake"
    ready = client.get("/api/v1/ready")
    assert ready.status_code == 200
    assert ready.json()["checks"] == {"model": True, "broker": True, "database": True}


def test_openapi_docs(client):
    spec = client.get("/openapi.json").json()
    assert "/api/v1/analyze" in spec["paths"] and "/api/v1/messages" in spec["paths"]
    assert client.get("/docs").status_code == 200


def test_metrics_endpoint(client):
    client.post("/api/v1/analyze", json={"text": "hello"})
    body = client.get("/metrics/").text
    assert "modstream_inference_batch_size" in body
    assert 'modstream_http_requests_total{method="POST",route="/api/v1/analyze",status="200"}' in body


# --- synchronous detection ---

def test_analyze(client):
    body = client.post("/api/v1/analyze", json={"text": "you are a loser"}).json()
    assert body["verdict"] == "flagged"
    assert body["matches"][0]["term"] == "loser"


def test_analyze_validation_error_shape(client):
    res = client.post("/api/v1/analyze", json={"text": "   "})
    assert res.status_code == 422
    assert res.json()["error"]["field"] == "text"
    assert client.post("/api/v1/analyze", json={"text": "x" * 5001}).status_code == 422
    assert client.post("/api/v1/analyze", content="nope").status_code == 422


def test_analyze_batch(client):
    res = client.post("/api/v1/analyze/batch", json={"texts": ["hi", "you idiot"]})
    assert [r["verdict"] for r in res.json()["results"]] == ["safe", "flagged"]
    assert client.post("/api/v1/analyze/batch", json={"texts": ["a"] * 257}).status_code == 422


def test_unrecorded_checks_do_not_affect_stats(client):
    client.post("/api/v1/analyze", json={"text": "hello", "record": False})
    assert client.get("/api/v1/stats").json()["scans"] == 0
    client.post("/api/v1/analyze", json={"text": "hello"})
    assert client.get("/api/v1/stats").json()["scans"] == 1


# --- asynchronous stream path ---

def test_ingested_messages_are_moderated_by_workers(client):
    res = client.post("/api/v1/messages", json={"messages": [
        {"channel": "room-1", "author": "a", "text": "good game"},
        {"channel": "room-1", "author": "b", "text": "you idiot"},
        {"channel": "room-2", "text": "see you later"},
    ]})
    assert res.status_code == 202 and res.json()["accepted"] == 3

    stored = wait_for(lambda: (m := client.get("/api/v1/messages").json()["messages"]) and len(m) == 3 and m)
    assert sorted(m["verdict"] for m in stored) == ["flagged", "safe", "safe"]
    flagged = client.get("/api/v1/messages", params={"verdict": "flagged"}).json()["messages"]
    assert [m["author"] for m in flagged] == ["b"]
    assert client.get("/api/v1/stats").json()["messages"] == 3


def test_message_validation(client):
    assert client.post("/api/v1/messages", json={"messages": []}).status_code == 422
    assert client.post("/api/v1/messages", json={"messages": [{"text": ""}]}).status_code == 422


def test_simulator(client):
    assert client.post("/api/v1/simulate", json={"messages": 200, "channels": 10, "rate": 5000}).status_code == 202
    wait_for(lambda: len(client.get("/api/v1/messages", params={"limit": 500}).json()["messages"]) == 200)


# --- live stream (SSE) ---

def test_stream_replays_backlog_filters_and_resumes(client):
    client.post("/api/v1/posts", json={"text": "first"})
    client.post("/api/v1/posts", json={"text": "you idiot"})
    client.post("/api/v1/posts", json={"text": "third"})

    events = sse_events(client.get("/api/v1/stream").text)
    assert [e[2]["body"] for e in events] == ["first", "you idiot", "third"]
    assert all(e[1] == "post" for e in events)

    flagged = sse_events(client.get("/api/v1/stream", params={"verdict": "flagged"}).text)
    assert [e[2]["body"] for e in flagged] == ["you idiot"]

    resumed = sse_events(client.get("/api/v1/stream", headers={"Last-Event-ID": events[0][0]}).text)
    assert [e[2]["body"] for e in resumed] == ["you idiot", "third"]


# --- WebSocket ---

def test_websocket_analyze_publish_subscribe(client):
    with client.websocket_connect("/api/v1/ws") as ws:
        ws.send_json({"type": "subscribe", "channel": "room-9"})
        assert ws.receive_json() == {"type": "subscribed", "channel": "room-9"}

        ws.send_json({"type": "analyze", "ref": "a1", "text": "you are a loser"})
        result = ws.receive_json()
        assert result["type"] == "result" and result["ref"] == "a1"
        assert result["result"]["verdict"] == "flagged"

        ws.send_json({"type": "publish", "ref": "p1", "channel": "room-9", "author": "z", "text": "hi all"})
        accepted = ws.receive_json()
        assert accepted["type"] == "accepted" and len(accepted["ids"]) == 1

        event = ws.receive_json()  # the worker's verdict, pushed to the subscriber
        assert event["type"] == "event"
        assert event["event"]["channel"] == "room-9" and event["event"]["data"]["body"] == "hi all"

        ws.send_json({"type": "ping"})
        assert ws.receive_json() == {"type": "pong"}


def test_websocket_errors(client):
    with client.websocket_connect("/api/v1/ws") as ws:
        ws.send_text("not json")
        assert ws.receive_json()["type"] == "error"
        ws.send_json({"type": "analyze", "ref": "x", "text": ""})
        err = ws.receive_json()
        assert err["type"] == "error" and err["ref"] == "x"
        ws.send_json({"type": "dance"})
        assert "unknown frame" in ws.receive_json()["message"]


def test_websocket_many_concurrent_analyses_are_batched(settings, detector, scorer):
    # A wider batching window than the other tests, so frames sent back-to-back share a pass.
    app = create_app(settings.model_copy(update={"batch_max_wait_ms": 50}), detector=detector)
    with TestClient(app) as client, client.websocket_connect("/api/v1/ws") as ws:
        for i in range(100):
            ws.send_json({"type": "analyze", "ref": str(i), "text": f"message number {i}"})
        refs = {ws.receive_json()["ref"] for _ in range(100)}
    assert refs == {str(i) for i in range(100)}
    assert len(scorer.batch_sizes) < 100  # fewer forward passes than requests
    assert max(scorer.batch_sizes) > 1


# --- feed ---

def test_post_comment_react_flow(client):
    post = client.post("/api/v1/posts", json={"author": "Ada", "text": "hello world"}).json()
    assert post["verdict"] == "safe"

    comment = client.post(f"/api/v1/posts/{post['id']}/comments", json={"text": "you idiot"})
    assert comment.status_code == 201
    assert comment.json()["verdict"] == "flagged" and comment.json()["author"] == "Anonymous"

    assert client.post(f"/api/v1/posts/{post['id']}/reactions", json={"kind": "like"}).json() == \
        {"likes": 1, "shares": 0}

    posts = client.get("/api/v1/posts").json()["posts"]
    assert len(posts[0]["comments"]) == 1

    stats = client.get("/api/v1/stats").json()
    assert stats["posts"] == 1 and stats["flagged_comments"] == 1
    assert stats["categories"] == {"harassment": 1}


def test_missing_post_returns_404(client):
    res = client.post("/api/v1/posts/999/comments", json={"text": "hi"})
    assert res.status_code == 404 and res.json()["error"]["code"] == "not_found"
    assert client.post("/api/v1/posts/999/reactions", json={"kind": "like"}).status_code == 404


def test_api_unknown_route_is_json(client):
    res = client.get("/api/v1/nope")
    assert res.status_code == 404 and res.json()["error"]["code"] == "not_found"


def test_timestamps_are_timezone_aware(client):
    """Stored timestamps must carry their UTC offset; SQLite drops it on read, and
    browsers would otherwise show new posts as hours old in non-UTC timezones."""
    post = client.post("/api/v1/posts", json={"text": "hello"}).json()
    client.post(f"/api/v1/posts/{post['id']}/comments", json={"text": "hi"})
    client.post("/api/v1/messages", json={"messages": [{"text": "hey"}]})
    listed = client.get("/api/v1/posts").json()["posts"][0]
    stored = wait_for(lambda: client.get("/api/v1/messages").json()["messages"])
    for stamp in (listed["created_at"], listed["comments"][0]["created_at"], stored[0]["created_at"],
                  stored[0]["enqueued_at"]):
        assert stamp.endswith("+00:00"), stamp


def test_demo_reset_stops_simulator_and_clears_stats(client):
    client.post("/api/v1/analyze", json={"text": "hello"})
    client.post("/api/v1/simulate", json={"messages": 50000, "channels": 10, "rate": 1000})
    assert client.post("/api/v1/demo/reset").json() == {"reset": True}
    stats = client.get("/api/v1/stats").json()
    assert stats["scans"] == 0 and stats["queue"]["lag"] == 0
    time.sleep(0.3)  # the cancelled simulator must not keep adding messages
    assert client.get("/api/v1/stats").json()["queue"]["lag"] == 0


def test_bluesky_feed_anonymises_authors(client):
    async def fake_source(limit):
        for i in range(limit):
            yield f"did:plc:user{i % 3}", "you idiot" if i == 0 else f"lovely day number {i}"

    client.app.state.rt.bluesky_source = fake_source
    assert client.post("/api/v1/feeds/bluesky", json={"messages": 7}).status_code == 202
    stored = wait_for(lambda: (m := client.get("/api/v1/messages", params={"channel": "bluesky"}).json()["messages"])
                      and len(m) == 7 and m)
    authors = {m["author"] for m in stored}
    assert len(authors) == 3 and all(a.startswith("bsky-") and "did" not in a for a in authors)
    assert sum(m["verdict"] == "flagged" for m in stored) == 1


def test_demo_controls_can_be_disabled(settings, detector):
    app = create_app(settings.model_copy(update={"enable_simulator": False}), detector=detector)
    with TestClient(app) as client:
        for path in ("/api/v1/demo/reset", "/api/v1/feeds/bluesky", "/api/v1/simulate"):
            assert client.post(path, json={}).status_code == 400
