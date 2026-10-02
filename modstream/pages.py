"""Server-rendered pages. Forms work without JavaScript; JS enhances them."""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, File, Form, Request, UploadFile
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from jinja2 import pass_context
from pydantic import ValidationError

from . import __version__
from .schemas import CommentIn, PostIn, ReactionIn
from .services import Runtime, ServiceError

router = APIRouter(include_in_schema=False)
templates = Jinja2Templates(directory=Path(__file__).parent / "templates")


@pass_context
def _url_for(ctx, name: str, **params) -> str:
    """Flask-style url_for returning a relative path (works behind any proxy)."""
    if "filename" in params:
        params["path"] = params.pop("filename")
    return str(ctx["request"].app.url_path_for(name, **params))


@pass_context
def _get_flashed_messages(ctx, with_categories: bool = False):
    messages = ctx["request"].session.pop("_flashes", [])
    return [tuple(m) for m in messages] if with_categories else [m[1] for m in messages]


templates.env.globals.update(url_for=_url_for, get_flashed_messages=_get_flashed_messages)
templates.env.filters["hue"] = lambda name: sum(map(ord, name)) % 360


def flash(request: Request, message: str, category: str = "info") -> None:
    request.session.setdefault("_flashes", []).append([category, message])


def render(request: Request, template: str, status_code: int = 200, **context):
    rt: Runtime = request.app.state.rt
    route = request.scope.get("route")
    context.update(endpoint=getattr(route, "name", None), app_version=__version__, engine=rt.detector.engine)
    return templates.TemplateResponse(request, template, context, status_code=status_code)


def back_to_feed(post_id: int | None = None) -> RedirectResponse:
    return RedirectResponse(f"/feed#post-{post_id}" if post_id else "/feed", status_code=303)


def _first_error(exc: ValidationError) -> str:
    err = exc.errors()[0]
    return f"{err['loc'][-1]}: {err['msg']}".capitalize()


@router.get("/", name="pages.home")
async def home(request: Request):
    return render(request, "index.html", stats=await request.app.state.rt.stats())


@router.get("/analyze", name="pages.analyze")
async def analyze(request: Request):
    return render(request, "analyze.html")


@router.get("/feed", name="pages.feed")
async def feed(request: Request):
    rt: Runtime = request.app.state.rt
    return render(request, "feed.html", posts=await rt.list_posts(), stats=await rt.stats())


@router.post("/feed", name="pages.create_post")
async def create_post(
    request: Request,
    text: str = Form(""),
    author: str = Form(""),
    image: UploadFile | None = File(None),
):
    rt: Runtime = request.app.state.rt
    try:
        body = PostIn(author=author, text=text)
        post = await rt.create_post(body.author, body.text, image)
    except ValidationError as exc:
        flash(request, _first_error(exc), "error")
        return back_to_feed()
    except ServiceError as exc:
        flash(request, exc.message, "error")
        return back_to_feed()
    if post["verdict"] == "flagged":
        flash(request, "Your post was published but hidden behind a content warning.", "warning")
    return back_to_feed(post["id"])


@router.post("/feed/{post_id}/comments", name="pages.add_comment")
async def add_comment(request: Request, post_id: int, comment: str = Form(""), author: str = Form("")):
    try:
        body = CommentIn(author=author, text=comment)
        await request.app.state.rt.add_comment(post_id, body.author, body.text)
    except ValidationError as exc:
        flash(request, _first_error(exc), "error")
    except ServiceError as exc:
        flash(request, exc.message, "error")
    return back_to_feed(post_id)


@router.post("/feed/{post_id}/react", name="pages.react")
async def react(request: Request, post_id: int, kind: str = Form("")):
    try:
        await request.app.state.rt.react(post_id, ReactionIn(kind=kind).kind)
    except (ValidationError, ServiceError):
        flash(request, "Couldn't save that reaction.", "error")
    return back_to_feed(post_id)


@router.get("/monitor", name="pages.monitor")
async def monitor(request: Request):
    return render(request, "monitor.html", stats=await request.app.state.rt.stats())


@router.get("/chat", name="pages.chat")
async def chat(request: Request):
    return render(request, "chat.html")


@router.get("/resources", name="pages.resources")
async def resources(request: Request):
    return render(request, "resources.html")


@router.get("/about", name="pages.about")
async def about(request: Request):
    return render(request, "about.html")


# Old URLs from v1 keep working.
LEGACY = {"/paste": "/analyze", "/facebook": "/feed", "/facebook/live": "/monitor",
          "/chatbot": "/chat", "/actions": "/resources", "/help": "/about"}


def _redirect(target: str):
    async def endpoint():
        return RedirectResponse(target, status_code=301)
    return endpoint


for _old, _new in LEGACY.items():
    router.add_api_route(_old, _redirect(_new), methods=["GET"], name=f"legacy{_old.replace('/', '.')}")
