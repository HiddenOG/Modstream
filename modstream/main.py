"""ASGI application factory."""

from __future__ import annotations

import json
import logging
import sys
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from prometheus_client import make_asgi_app
from starlette.datastructures import MutableHeaders
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.sessions import SessionMiddleware

from . import __version__, api, metrics, pages
from .batching import Overloaded
from .broker import Broker
from .config import Settings
from .detector import Detector
from .services import Runtime, ServiceError

SECURITY_HEADERS = {
    b"x-content-type-options": b"nosniff",
    b"referrer-policy": b"strict-origin-when-cross-origin",
    b"x-frame-options": b"DENY",
}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        out = {"ts": self.formatTime(record), "level": record.levelname, "logger": record.name,
               "msg": record.getMessage()}
        if record.exc_info:
            out["exc"] = self.formatException(record.exc_info)
        return json.dumps(out)


def configure_logging(json_logs: bool) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter() if json_logs else logging.Formatter(
        "%(asctime)s %(levelname)-7s %(name)s: %(message)s"))
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(logging.INFO)


class ObservabilityMiddleware:
    """Pure ASGI middleware (safe for streaming responses): request metrics + security headers."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        started = time.perf_counter()
        status = 500

        async def send_wrapper(message):
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
                headers = MutableHeaders(scope=message)
                for key, value in SECURITY_HEADERS.items():
                    headers.setdefault(key.decode(), value.decode())
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            route = getattr(scope.get("route"), "path", None) or (
                "/static" if scope["path"].startswith(("/static", "/uploads")) else "unmatched")
            if route != "/metrics":
                metrics.HTTP_REQUESTS.labels(scope["method"], route, status).inc()
                if not route.endswith("/stream"):
                    metrics.HTTP_LATENCY.labels(scope["method"], route).observe(time.perf_counter() - started)


def _error(status: int, code: str, message: str, field: str | None = None, headers=None) -> JSONResponse:
    return JSONResponse({"error": {"code": code, "message": message, "field": field}}, status, headers=headers)


def create_app(
    settings: Settings | None = None, *, detector: Detector | None = None, broker: Broker | None = None,
) -> FastAPI:
    settings = settings or Settings()
    configure_logging(settings.log_json)
    rt = Runtime(settings, detector=detector, broker=broker)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        await rt.start()
        yield
        await rt.stop()

    app = FastAPI(
        title="Modstream API",
        version=__version__,
        description="Real-time content moderation for posts, chats and high-volume message streams.",
        lifespan=lifespan,
        redoc_url=None,
    )
    app.state.rt = rt

    app.add_middleware(SessionMiddleware, secret_key=settings.secret_key, same_site="lax",
                       https_only=settings.env == "prod")
    app.add_middleware(ObservabilityMiddleware)

    @app.exception_handler(ServiceError)
    async def _service_error(_req: Request, exc: ServiceError):
        return _error(exc.status, exc.code, exc.message, exc.field)

    @app.exception_handler(Overloaded)
    async def _overloaded(_req: Request, _exc: Overloaded):
        return _error(503, "overloaded", "Server is at capacity, retry shortly", headers={"Retry-After": "1"})

    @app.exception_handler(RequestValidationError)
    async def _validation(_req: Request, exc: RequestValidationError):
        err = exc.errors()[0] if exc.errors() else {"loc": (), "msg": "Invalid request"}
        loc = [str(p) for p in err["loc"] if p not in ("body", "query", "path", "header")]
        return _error(422, "invalid_request", err["msg"], ".".join(loc) or None)

    @app.exception_handler(StarletteHTTPException)
    async def _http_error(request: Request, exc: StarletteHTTPException):
        if request.url.path.startswith("/api/"):
            code = {404: "not_found", 405: "method_not_allowed"}.get(exc.status_code, "error")
            return _error(exc.status_code, code, str(exc.detail))
        return pages.render(request, "error.html", error=exc, status_code=exc.status_code)

    package = Path(__file__).parent
    app.mount("/static", StaticFiles(directory=package / "static"), name="static")
    app.mount("/uploads", StaticFiles(directory=settings.upload_dir), name="pages.upload")
    app.mount("/metrics", make_asgi_app(), name="metrics")
    app.include_router(api.router)
    app.include_router(pages.router)
    return app
