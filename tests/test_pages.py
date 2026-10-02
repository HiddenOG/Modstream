import pytest


@pytest.mark.parametrize("path", ["/", "/analyze", "/feed", "/monitor", "/chat", "/resources", "/about"])
def test_pages_render(client, path):
    res = client.get(path)
    assert res.status_code == 200
    assert b'name="viewport"' in res.data


@pytest.mark.parametrize("old, new", [
    ("/paste", "/analyze"), ("/facebook", "/feed"), ("/facebook/live", "/monitor"),
    ("/chatbot", "/chat"), ("/actions", "/resources"), ("/help", "/about"),
])
def test_legacy_urls_redirect(client, old, new):
    res = client.get(old)
    assert res.status_code == 301
    assert res.headers["Location"].endswith(new)


def test_feed_form_escapes_html(client):
    res = client.post("/feed", data={"author": "<b>x</b>", "text": "<script>alert(1)</script>"})
    assert res.status_code == 302
    page = client.get("/feed").data
    assert b"<script>alert(1)</script>" not in page
    assert b"&lt;script&gt;" in page


def test_flagged_post_is_behind_content_warning(client):
    client.post("/feed", data={"text": "you are a loser"})
    page = client.get("/feed").data
    assert b"cw is-hidden" in page
    assert b"Content warning" in page


def test_comment_form(client):
    client.post("/feed", data={"text": "hello"})
    client.post("/feed/1/comments", data={"comment": "nice one"})
    assert b"nice one" in client.get("/feed").data


def test_404_page(client):
    res = client.get("/does-not-exist")
    assert res.status_code == 404
    assert b"Back home" in res.data


def test_security_headers(client):
    res = client.get("/")
    assert res.headers["X-Content-Type-Options"] == "nosniff"
    assert res.headers["X-Frame-Options"] == "DENY"
