"""CyberShield: real-time harmful content detection."""

from __future__ import annotations

import logging
import os
import threading

from flask import Flask, render_template, request

__version__ = "2.0.0"


class Config:
    SECRET_KEY = os.environ.get("SECRET_KEY") or os.urandom(32).hex()
    DATABASE = os.environ.get("DATABASE_PATH")  # defaults to instance/cybershield.db
    UPLOAD_FOLDER = os.environ.get("UPLOAD_FOLDER")  # defaults to instance/uploads
    MAX_CONTENT_LENGTH = 5 * 1024 * 1024
    SCORER = os.environ.get("CYBERSHIELD_SCORER", "auto")  # auto | detoxify | none
    FLAG_THRESHOLD = float(os.environ.get("FLAG_THRESHOLD", 0.7))
    REVIEW_THRESHOLD = float(os.environ.get("REVIEW_THRESHOLD", 0.4))
    WARMUP = os.environ.get("CYBERSHIELD_WARMUP", "1") == "1"
    STREAM_POLL_SECONDS = 1.0
    STREAM_MAX_SECONDS = 300
    STREAM_BACKLOG = 50


def create_app(overrides: dict | None = None) -> Flask:
    from . import api, db, pages
    from .detector import Detector, load_scorer

    app = Flask(__name__, instance_relative_config=True)
    app.config.from_object(Config)
    app.config.update(overrides or {})
    os.makedirs(app.instance_path, exist_ok=True)
    app.config["DATABASE"] = app.config["DATABASE"] or os.path.join(app.instance_path, "cybershield.db")
    app.config["UPLOAD_FOLDER"] = app.config["UPLOAD_FOLDER"] or os.path.join(app.instance_path, "uploads")
    os.makedirs(app.config["UPLOAD_FOLDER"], exist_ok=True)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    detector = app.config.get("DETECTOR") or Detector(
        load_scorer(app.config["SCORER"]),
        flag_threshold=app.config["FLAG_THRESHOLD"],
        review_threshold=app.config["REVIEW_THRESHOLD"],
    )
    app.extensions["detector"] = detector
    if app.config["WARMUP"] and not app.testing:
        # Load model weights in the background so the server starts instantly.
        threading.Thread(target=detector.warmup, name="model-warmup", daemon=True).start()

    db.init_db(app.config["DATABASE"])
    app.teardown_appcontext(db.close_db)
    app.register_blueprint(pages.bp)
    app.register_blueprint(api.bp)

    # Stable avatar colour per name; mirrored by hue() in static/js/core.js.
    app.add_template_filter(lambda name: sum(map(ord, name)) % 360, "hue")

    @app.context_processor
    def _globals():
        return {"app_version": __version__, "engine": detector.engine}

    @app.after_request
    def _security_headers(resp):
        resp.headers.setdefault("X-Content-Type-Options", "nosniff")
        resp.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
        resp.headers.setdefault("X-Frame-Options", "DENY")
        return resp

    @app.errorhandler(404)
    @app.errorhandler(413)
    def _page_error(err):
        if request.path.startswith("/api/"):
            code = err.name.lower().replace(" ", "_")
            return {"error": {"code": code, "message": err.description}}, err.code
        return render_template("error.html", error=err), err.code

    return app
