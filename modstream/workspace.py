"""Per-visitor workspaces: private sandboxes so many people can try the demo at once.

A visitor's workspace id lives in the signed session cookie, created on their first
request. Everything they create (posts, comments, stream messages, counters, live events)
is tagged with it, and every read is filtered by it, so visitors never see each other's
activity. A client may instead name a workspace with an ``X-Workspace`` header or a
``?workspace=`` query parameter, e.g. to share one across a load test's connections.

Workspaces separate demo sessions; they are not access control.
"""

from __future__ import annotations

import re
import secrets
from typing import Annotated

from fastapi import Depends
from starlette.requests import HTTPConnection

SESSION_KEY = "ws"
_VALID = re.compile(r"^[A-Za-z0-9_-]{1,32}$")


def workspace_of(conn: HTTPConnection) -> str:
    explicit = conn.headers.get("x-workspace") or conn.query_params.get("workspace")
    if explicit and _VALID.match(explicit):
        return explicit
    ws = conn.session.get(SESSION_KEY)
    if not (isinstance(ws, str) and _VALID.match(ws)):
        ws = secrets.token_hex(8)
        conn.session[SESSION_KEY] = ws  # sets the cookie on this response
    return ws


WS = Annotated[str, Depends(workspace_of)]
