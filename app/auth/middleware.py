"""The gate of the application.

Every request needs a valid key of the application, except the entry points
of Bitrix24 (they check AUTH_ID themselves), the integration of n8n (its own
key), static scripts and /health. The key comes:

  - in the header X-Checklist-Auth (every fetch and upload of a page);
  - in the parameter "at": a beacon key (sendBeacon cannot set headers);
  - for opening pages and files only (GET): in the cookie of a window or in
    the link key "lt" added to a link at the moment of a click.

A request that changes something never takes the cookie, so another site
cannot make the browser of an employee send such a request.

A key of a window opened for viewing only (the platform portal) opens pages
and reads data; every change and the service pages are refused with 403.

The user of a request is the one of the key: userId/userName (or
actingUserId/actingUserName when a request names both an actor and another
person) in links, JSON bodies and forms are replaced by the checked ones.
Every HTML page of a checked request gets the keys of its window and the
client script (checklist-auth.js) that sends them.
"""
from __future__ import annotations

import json
import re
import threading
import time
from http.cookies import SimpleCookie
from pathlib import Path
from urllib.parse import parse_qsl, urlencode

from app.auth import tokens
from app.auth.config import auth_mode, frame_ancestors, session_seconds
from app.auth.identity import SCOPE_KEY
from app.auth.pages import closed_html, closed_json
from app.logging_utils import write_debug_log


COOKIE_NAME = "checklist_auth"
HEADER_NAME = b"x-checklist-auth"
LINK_PARAM = "lt"
BEACON_PARAM = "at"

PUBLIC_PATHS = frozenset({"/health", "/favicon.ico", "/install", "/install/", "/api/auth/csp-report"})
PUBLIC_PREFIXES = ("/ui-static/",)
# Bitrix24 posts AUTH_ID here; the routes check it and call establish().
ENTRY_POST_PATHS = frozenset({"/", "/textarea", "/launch"})
# n8n is checked by its own key (N8N_SHARED_TOKEN).
OWN_KEY_PREFIXES = ("/api/integrations/n8n/",)

# What a view-only window may send besides GET: renewal of its keys and
# diagnostics. Service pages stay closed to it.
READ_ONLY_POST_PATHS = frozenset({"/api/auth/refresh", "/api/debug/event"})
READ_ONLY_CLOSED_PREFIXES = ("/admin", "/api/admin/", "/debug/")

MAX_REWRITTEN_BODY = 5 * 1024 * 1024
MAX_INJECTED_PAGE = 8 * 1024 * 1024
SAFE_METHODS = frozenset({"GET", "HEAD"})

CLIENT_SCRIPT_PATH = Path(__file__).resolve().parent.parent / "ui" / "static" / "js" / "checklist-auth.js"
_client_script_cache: str | None = None

_report_lock = threading.Lock()
_report_window = [0.0, 0]
REPORT_LIMIT_PER_HOUR = 2000


def _client_script() -> str:
    global _client_script_cache
    if _client_script_cache is None:
        try:
            _client_script_cache = CLIENT_SCRIPT_PATH.read_text(encoding="utf-8")
        except OSError:
            _client_script_cache = ""
    return _client_script_cache


def _header(scope, name: bytes) -> str:
    for key, value in scope.get("headers") or []:
        if key == name:
            return value.decode("latin-1")
    return ""


def _path(scope) -> str:
    path = scope.get("path") or "/"
    root = scope.get("root_path") or ""
    if root and path.startswith(root):
        path = path[len(root):] or "/"
    return path


def _cookie(scope) -> str:
    raw = _header(scope, b"cookie")
    if not raw:
        return ""
    try:
        cookie = SimpleCookie()
        cookie.load(raw)
    except Exception:
        return ""
    morsel = cookie.get(COOKIE_NAME)
    return morsel.value if morsel else ""


def _is_https(scope) -> bool:
    return scope.get("scheme") == "https" or _header(scope, b"x-forwarded-proto").lower().startswith("https")


def _wants_html(scope) -> bool:
    accept = _header(scope, b"accept").lower()
    dest = _header(scope, b"sec-fetch-dest").lower()
    return "text/html" in accept or dest in {"document", "iframe", "frame"}


def classify(method: str, path: str) -> str:
    if path in PUBLIC_PATHS or path.startswith(PUBLIC_PREFIXES):
        return "public"
    if path.startswith(OWN_KEY_PREFIXES):
        return "own_key"
    if method == "POST" and path in ENTRY_POST_PATHS:
        return "entry"
    return "protected"


def authenticate(scope) -> tuple[dict | None, str, str]:
    """(identity, how, reason of failure)."""
    method = scope.get("method", "GET").upper()
    params = dict(parse_qsl((scope.get("query_string") or b"").decode("latin-1"), keep_blank_values=True))
    candidates = [("header", _header(scope, HEADER_NAME), ("s",))]
    candidates.append(("beacon", params.get(BEACON_PARAM, ""), ("b",)))
    if method in SAFE_METHODS:
        candidates.append(("link", params.get(LINK_PARAM, ""), ("l", "s")))
        candidates.append(("cookie", _cookie(scope), ("s",)))
    first_reason = ""
    for how, value, kinds in candidates:
        if not value:
            continue
        try:
            return tokens.verify(value, kinds=kinds), how, ""
        except tokens.TokenError as exc:
            first_reason = first_reason or str(exc)
    if not first_reason and method not in SAFE_METHODS and _cookie(scope):
        first_reason = "header_required"
    return None, "", first_reason or "missing"


def read_only_allows(method: str, path: str) -> bool:
    if path.startswith(READ_ONLY_CLOSED_PREFIXES):
        return False
    return method in SAFE_METHODS or path in READ_ONLY_POST_PATHS


# ---------------------------------------------------------------- identity

def _rewrite_fields(data: dict, identity: dict) -> bool:
    """Put the checked employee into a request that names a user."""
    user_id = identity["userId"]
    user_name = identity.get("userName") or ""
    changed = False

    def put(key: str, value: str) -> None:
        nonlocal changed
        if key in data and str(data.get(key) or "") != value:
            data[key] = value
            changed = True

    if "actingUserId" in data:
        # userId of such a request is another person (a GIP to add).
        put("actingUserId", user_id)
        if user_name:
            put("actingUserName", user_name)
    else:
        put("userId", user_id)
        if user_name:
            put("userName", user_name)
    editor = data.get("editor")
    if isinstance(editor, dict):
        for key in ("id", "userId"):
            if key in editor and str(editor.get(key) or "") != user_id:
                editor[key] = user_id
                changed = True
        for key in ("name", "userName"):
            if user_name and key in editor and str(editor.get(key) or "") != user_name:
                editor[key] = user_name
                changed = True
    return changed


def _rewrite_query(scope, identity: dict | None) -> None:
    raw = (scope.get("query_string") or b"").decode("latin-1")
    if not raw:
        return
    pairs = parse_qsl(raw, keep_blank_values=True)
    kept = [(k, v) for k, v in pairs if k not in (LINK_PARAM, BEACON_PARAM)]
    changed = len(kept) != len(pairs)
    if identity:
        data = dict(kept)
        if _rewrite_fields(data, identity):
            kept = [(k, data.get(k, v)) for k, v in kept]
            changed = True
    if changed:
        scope["query_string"] = urlencode(kept).encode("latin-1")


async def _read_body(receive) -> tuple[bytes, list]:
    chunks = []
    messages = []
    while True:
        message = await receive()
        messages.append(message)
        if message["type"] != "http.request":
            break
        chunks.append(message.get("body", b""))
        if not message.get("more_body", False):
            break
    return b"".join(chunks), messages


def _rewrite_body(body: bytes, content_type: str, identity: dict) -> bytes | None:
    content_type = content_type.lower()
    if "application/x-www-form-urlencoded" in content_type:
        pairs = parse_qsl(body.decode("utf-8", errors="replace"), keep_blank_values=True)
        data = dict(pairs)
        if not _rewrite_fields(data, identity):
            return None
        return urlencode([(k, data.get(k, v)) for k, v in pairs]).encode("utf-8")
    if content_type and "json" not in content_type and "text/plain" not in content_type:
        return None
    try:
        data = json.loads(body.decode("utf-8"))
    except Exception:
        return None
    if not isinstance(data, dict) or not _rewrite_fields(data, identity):
        return None
    return json.dumps(data, ensure_ascii=False).encode("utf-8")


# ---------------------------------------------------------------- report

def _report(scope, reason: str, path: str) -> None:
    now = time.time()
    with _report_lock:
        if now - _report_window[0] > 3600:
            _report_window[0], _report_window[1] = now, 0
        _report_window[1] += 1
        count = _report_window[1]
    if count > REPORT_LIMIT_PER_HOUR:
        return
    client = scope.get("client") or ("", 0)
    write_debug_log("auth_would_block", {
        "method": scope.get("method", ""),
        "path": path,
        "reason": reason,
        "html": _wants_html(scope),
        "referer": _header(scope, b"referer")[:200],
        "ip": _header(scope, b"x-forwarded-for") or (client[0] if client else ""),
        "userAgent": _header(scope, b"user-agent")[:160],
        "limitReached": count == REPORT_LIMIT_PER_HOUR,
    })


# ---------------------------------------------------------------- injection

_HEAD_RE = re.compile(rb"<head\b[^>]*>", re.IGNORECASE)
_HTML_RE = re.compile(rb"<html\b[^>]*>", re.IGNORECASE)


def _bootstrap(identity: dict) -> bytes:
    bundle = tokens.client_bundle(identity)
    data = json.dumps(bundle, ensure_ascii=False).replace("</", "<\\/")
    script = _client_script().replace("</script", "<\\/script")
    return (
        "<script>window.CHECKLIST_AUTH=" + data + ";</script>"
        + "<script>" + script + "</script>"
    ).encode("utf-8")


def _inject(body: bytes, identity: dict) -> bytes:
    snippet = _bootstrap(identity)
    match = _HEAD_RE.search(body) or _HTML_RE.search(body)
    if match:
        return body[:match.end()] + snippet + body[match.end():]
    return snippet + body


def _session_cookie(identity: dict, secure: bool, in_frame: bool) -> bytes:
    """The cookie of a window, used when a page reloads or opens a file.

    A window of its own gets a usual cookie. A frame inside Bitrix24 gets a
    cookie kept for that portal only (Partitioned): another site never sees
    it. Browsers that keep no cookies of frames are served by the link key.
    """
    value = tokens.issue(identity, kind="s", sid=identity.get("sid") or "")
    parts = [
        f"{COOKIE_NAME}={value}",
        "Path=/",
        f"Max-Age={session_seconds()}",
        "HttpOnly",
    ]
    if in_frame and secure:
        parts += ["SameSite=None", "Secure", "Partitioned"]
    else:
        parts.append("SameSite=Lax")
        if secure:
            parts.append("Secure")
    return "; ".join(parts).encode("latin-1")


def _csp_header(mode: str) -> tuple[bytes, bytes] | None:
    ancestors = frame_ancestors()
    if not ancestors:
        return None
    if mode == "on":
        return b"content-security-policy", f"frame-ancestors {ancestors}".encode("latin-1")
    return (
        b"content-security-policy-report-only",
        f"frame-ancestors {ancestors}; report-uri /api/auth/csp-report".encode("latin-1"),
    )


class AuthMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        mode = auth_mode()
        if mode == "off":
            await self.app(scope, receive, send)
            return

        method = scope.get("method", "GET").upper()
        path = _path(scope)
        kind = classify(method, path)
        if kind in {"public", "own_key"}:
            await self.app(scope, receive, send)
            return

        identity, how = None, ""
        if kind == "protected":
            identity, how, reason = authenticate(scope)
            if identity is None:
                if mode == "on":
                    await self._close(scope, send, reason)
                    return
                _report(scope, reason, path)
            else:
                scope[SCOPE_KEY] = identity
                if identity.get("readOnly") and not read_only_allows(method, path):
                    await self._close(scope, send, "read_only", status=403)
                    return
        _rewrite_query(scope, identity)

        if identity and method not in SAFE_METHODS:
            receive = await self._rewritten_receive(scope, receive, identity)

        set_cookie = bool(identity) and how in {"link", "beacon"} and method in SAFE_METHODS
        await self.app(scope, receive, self._wrap_send(scope, send, mode, set_cookie))

    async def _close(self, scope, send, reason: str, status: int = 401) -> None:
        as_html = _wants_html(scope)
        body = closed_html(reason) if as_html else closed_json(reason)
        headers = [
            (b"content-type", b"text/html; charset=utf-8" if as_html else b"application/json; charset=utf-8"),
            (b"content-length", str(len(body)).encode("latin-1")),
            (b"cache-control", b"no-store"),
        ]
        csp = _csp_header("on")
        if csp:
            headers.append(csp)
        await send({"type": "http.response.start", "status": status, "headers": headers})
        await send({"type": "http.response.body", "body": body if scope.get("method") != "HEAD" else b""})

    async def _rewritten_receive(self, scope, receive, identity: dict):
        content_type = _header(scope, b"content-type")
        length = _header(scope, b"content-length")
        if "multipart/" in content_type.lower() or not length.isdigit() or int(length) > MAX_REWRITTEN_BODY:
            return receive
        body, messages = await _read_body(receive)
        new_body = _rewrite_body(body, content_type, identity)
        if new_body is not None:
            body = new_body
            scope["headers"] = [
                (k, v) for k, v in scope.get("headers") or [] if k != b"content-length"
            ] + [(b"content-length", str(len(body)).encode("latin-1"))]
        tail = [m for m in messages if m["type"] != "http.request"]
        sent = False

        async def replay():
            nonlocal sent
            if not sent:
                sent = True
                return {"type": "http.request", "body": body, "more_body": False}
            if tail:
                return tail.pop(0)
            return await receive()

        return replay

    def _wrap_send(self, scope, send, mode: str, set_cookie: bool):
        state = {"inject": False, "start": None, "chunks": [], "size": 0}

        async def wrapped(message):
            if message["type"] == "http.response.start":
                headers = list(message.get("headers") or [])
                content_type = b""
                for key, value in headers:
                    if key.lower() == b"content-type":
                        content_type = value.lower()
                is_html = content_type.startswith(b"text/html")
                identity = scope.get(SCOPE_KEY)
                if is_html:
                    csp = _csp_header(mode)
                    if csp:
                        headers.append(csp)
                    if identity:
                        headers = [(k, v) for k, v in headers if k.lower() != b"cache-control"]
                        headers.append((b"cache-control", b"no-store"))
                if set_cookie and identity and is_html:
                    in_frame = _header(scope, b"sec-fetch-dest").lower() in {"iframe", "frame"}
                    headers.append((b"set-cookie", _session_cookie(identity, _is_https(scope), in_frame)))
                message = {**message, "headers": headers}
                if is_html and identity and scope.get("method") != "HEAD":
                    state["inject"] = True
                    state["start"] = message
                    return
                await send(message)
                return

            if message["type"] == "http.response.body" and state["inject"]:
                state["chunks"].append(message.get("body", b""))
                state["size"] += len(message.get("body", b""))
                more = message.get("more_body", False)
                if more and state["size"] <= MAX_INJECTED_PAGE:
                    return
                body = b"".join(state["chunks"])
                start = state["start"]
                state["inject"] = False
                if not more:
                    body = _inject(body, scope.get(SCOPE_KEY))
                    start["headers"] = [
                        (k, v) for k, v in start["headers"] if k.lower() != b"content-length"
                    ] + [(b"content-length", str(len(body)).encode("latin-1"))]
                # A page too large to hold is sent as it is, without keys.
                await send(start)
                await send({"type": "http.response.body", "body": body, "more_body": more})
                return

            await send(message)

        return wrapped
