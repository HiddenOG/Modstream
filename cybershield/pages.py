"""Server-rendered pages. Forms work without JavaScript; JS enhances them."""

from __future__ import annotations

from flask import (
    Blueprint,
    current_app,
    flash,
    redirect,
    render_template,
    request,
    send_from_directory,
    url_for,
)

from . import db as store
from . import services

bp = Blueprint("pages", __name__)


@bp.get("/")
def home():
    return render_template("index.html", stats=store.stats(store.get_db()))


@bp.get("/analyze")
def analyze():
    return render_template("analyze.html")


@bp.get("/feed")
def feed():
    conn = store.get_db()
    return render_template("feed.html", posts=store.list_posts(conn), stats=store.stats(conn))


@bp.post("/feed")
def create_post():
    try:
        post = services.create_post(
            request.form.get("author"), request.form.get("text"), request.files.get("image")
        )
    except services.ValidationError as err:
        flash(err.message, "error")
        return redirect(url_for(".feed"))
    if post["verdict"] == "flagged":
        flash("Your post was published but hidden behind a content warning.", "warning")
    return redirect(url_for(".feed", _anchor=f"post-{post['id']}"))


@bp.post("/feed/<int:post_id>/comments")
def add_comment(post_id: int):
    try:
        services.add_comment(post_id, request.form.get("author"), request.form.get("comment"))
    except services.ValidationError as err:
        flash(err.message, "error")
    return redirect(url_for(".feed", _anchor=f"post-{post_id}"))


@bp.post("/feed/<int:post_id>/react")
def react(post_id: int):
    try:
        services.react(post_id, request.form.get("kind"))
    except services.ValidationError as err:
        flash(err.message, "error")
    return redirect(url_for(".feed", _anchor=f"post-{post_id}"))


@bp.get("/monitor")
def monitor():
    return render_template("monitor.html", stats=store.stats(store.get_db()))


@bp.get("/chat")
def chat():
    return render_template("chat.html")


@bp.get("/resources")
def resources():
    return render_template("resources.html")


@bp.get("/about")
def about():
    return render_template("about.html", engine=services.detector().engine)


@bp.get("/uploads/<path:name>")
def upload(name: str):
    return send_from_directory(current_app.config["UPLOAD_FOLDER"], name)


# Old URLs from v1 of the app keep working.
LEGACY = {
    "/paste": ".analyze",
    "/facebook": ".feed",
    "/facebook/live": ".monitor",
    "/chatbot": ".chat",
    "/actions": ".resources",
    "/help": ".about",
}

for _path, _endpoint in LEGACY.items():
    bp.add_url_rule(
        _path,
        f"legacy_{_endpoint[1:]}",
        lambda endpoint=_endpoint: redirect(url_for(endpoint), 301),
    )
