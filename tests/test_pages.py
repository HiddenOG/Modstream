import io

import pytest

PNG = b"\x89PNG\r\n\x1a\n" + b"\0" * 64


@pytest.mark.parametrize("path", ["/", "/analyze", "/feed", "/monitor", "/chat", "/resources", "/about"])
def test_pages_render(client, path):
    res = client.get(path)
    assert res.status_code == 200
    assert b'name="viewport"' in res.content


def test_active_nav_item(client):
    assert b'href="/feed" aria-current="page"' in client.get("/feed").content


@pytest.mark.parametrize("old, new", [
    ("/paste", "/analyze"), ("/facebook", "/feed"), ("/facebook/live", "/monitor"),
    ("/chatbot", "/chat"), ("/actions", "/resources"), ("/help", "/about"),
])
def test_legacy_urls_redirect(client, old, new):
    res = client.get(old, follow_redirects=False)
    assert res.status_code == 301 and res.headers["location"] == new


def test_feed_form_escapes_html(client):
    res = client.post("/feed", data={"author": "<b>x</b>", "text": "<script>alert(1)</script>"},
                      follow_redirects=False)
    assert res.status_code == 303
    page = client.get("/feed").content
    assert b"<script>alert(1)</script>" not in page
    assert b"&lt;script&gt;" in page


def test_flagged_post_is_behind_content_warning_and_flashes(client):
    page = client.post("/feed", data={"text": "you are a loser"}).content  # follows the redirect
    assert b"cw is-hidden" in page
    assert b"hidden behind a content warning" in page
    assert b"hidden behind a content warning" not in client.get("/feed").content  # flash shown once


def test_empty_post_flashes_error(client):
    assert b"flash-error" in client.post("/feed", data={"text": "  "}).content


def test_comment_and_reaction_forms(client):
    client.post("/feed", data={"text": "hello"})
    client.post("/feed/1/comments", data={"comment": "nice one"})
    client.post("/feed/1/react", data={"kind": "like"})
    page = client.get("/feed").content
    assert b"nice one" in page
    assert b'<span data-count>1</span>' in page


def test_image_upload_is_sniffed_and_renamed(client):
    client.post("/feed", data={"text": "pic"}, files={"image": ("../../evil.png", io.BytesIO(PNG), "image/png")})
    name = client.get("/api/v1/posts").json()["posts"][0]["image"]
    assert name.endswith(".png") and "evil" not in name
    assert client.get(f"/uploads/{name}").content == PNG


def test_non_image_upload_rejected(client):
    page = client.post("/feed", data={"text": "pic"},
                       files={"image": ("x.png", io.BytesIO(b"<script>alert(1)</script>"), "image/png")}).content
    assert b"PNG, JPEG, GIF or WebP" in page
    assert client.get("/api/v1/posts").json()["posts"] == []


def test_404_page(client):
    res = client.get("/does-not-exist")
    assert res.status_code == 404 and b"Back home" in res.content


def test_security_headers(client):
    res = client.get("/")
    assert res.headers["x-content-type-options"] == "nosniff"
    assert res.headers["x-frame-options"] == "DENY"
