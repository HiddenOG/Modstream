import io
import json

PNG = b"\x89PNG\r\n\x1a\n" + b"\0" * 64


def test_health(client):
    body = client.get("/api/v1/health").get_json()
    assert body["status"] == "ok"
    assert body["engine"] == "fake"


def test_analyze(client):
    res = client.post("/api/v1/analyze", json={"text": "you are a loser"})
    assert res.status_code == 200
    body = res.get_json()
    assert body["verdict"] == "flagged"
    assert body["matches"][0]["term"] == "loser"


def test_analyze_validation(client):
    res = client.post("/api/v1/analyze", json={"text": "   "})
    assert res.status_code == 400
    assert res.get_json()["error"]["field"] == "text"
    assert client.post("/api/v1/analyze", json={"text": "x" * 5001}).status_code == 400
    assert client.post("/api/v1/analyze", data="nope").status_code == 400


def test_analyze_batch(client):
    res = client.post("/api/v1/analyze/batch", json={"texts": ["hi", "you idiot"]})
    assert [r["verdict"] for r in res.get_json()["results"]] == ["safe", "flagged"]
    assert client.post("/api/v1/analyze/batch", json={"texts": ["a"] * 33}).status_code == 400


def test_unrecorded_checks_do_not_affect_stats(client):
    client.post("/api/v1/analyze", json={"text": "hello", "record": False})
    assert client.get("/api/v1/stats").get_json()["scans"] == 0
    client.post("/api/v1/analyze", json={"text": "hello"})
    assert client.get("/api/v1/stats").get_json()["scans"] == 1


def test_post_comment_react_flow(client):
    res = client.post("/api/v1/posts", json={"author": "Ada", "text": "hello world"})
    assert res.status_code == 201
    post = res.get_json()
    assert post["verdict"] == "safe"

    res = client.post(f"/api/v1/posts/{post['id']}/comments", json={"text": "you idiot"})
    assert res.status_code == 201
    assert res.get_json()["verdict"] == "flagged"
    assert res.get_json()["author"] == "Anonymous"

    res = client.post(f"/api/v1/posts/{post['id']}/reactions", json={"kind": "like"})
    assert res.get_json() == {"likes": 1, "shares": 0}

    posts = client.get("/api/v1/posts").get_json()["posts"]
    assert len(posts[0]["comments"]) == 1

    stats = client.get("/api/v1/stats").get_json()
    assert stats["posts"] == 1 and stats["flagged_comments"] == 1
    assert stats["categories"] == {"harassment": 1}


def test_missing_post_returns_404(client):
    res = client.post("/api/v1/posts/999/comments", json={"text": "hi"})
    assert res.status_code == 404
    assert res.get_json()["error"]["code"] == "not_found"
    assert client.post("/api/v1/posts/999/reactions", json={"kind": "like"}).status_code == 404


def test_image_upload_is_sniffed_and_renamed(client):
    res = client.post(
        "/api/v1/posts",
        data={"text": "pic", "image": (io.BytesIO(PNG), "../../evil.png")},
        content_type="multipart/form-data",
    )
    assert res.status_code == 201
    name = res.get_json()["image"]
    assert name.endswith(".png") and "evil" not in name
    assert client.get(f"/uploads/{name}").status_code == 200


def test_non_image_upload_rejected(client):
    res = client.post(
        "/api/v1/posts",
        data={"text": "pic", "image": (io.BytesIO(b"<script>alert(1)</script>"), "x.png")},
        content_type="multipart/form-data",
    )
    assert res.status_code == 400
    assert res.get_json()["error"]["field"] == "image"


def test_stream_replays_events_and_resumes(client):
    client.post("/api/v1/posts", json={"text": "first"})
    client.post("/api/v1/posts", json={"text": "second"})

    body = client.get("/api/v1/stream").get_data(as_text=True)
    events = [json.loads(line[6:]) for line in body.splitlines() if line.startswith("data: ")]
    assert [e["body"] for e in events] == ["first", "second"]

    resumed = client.get("/api/v1/stream", headers={"Last-Event-ID": "1"}).get_data(as_text=True)
    assert "second" in resumed and "first" not in resumed


def test_api_unknown_route_is_json(client):
    res = client.get("/api/v1/nope")
    assert res.status_code == 404
    assert res.get_json()["error"]["code"] == "not_found"
