from __future__ import annotations

import threading
import time

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response

from app.auth import tokens
from app.auth.config import auth_mode
from app.auth.identity import current_identity
from app.logging_utils import write_debug_log


router = APIRouter()

_csp_lock = threading.Lock()
_csp_window = [0.0, 0]
CSP_REPORTS_PER_HOUR = 200


@router.post("/api/auth/refresh")
def api_auth_refresh(request: Request):
    """New keys for a window that stays open."""
    identity = current_identity(request)
    if not identity:
        return JSONResponse({"ok": False, "authRequired": True, "error": "no key"}, status_code=401)
    bundle = tokens.client_bundle(identity)
    return JSONResponse({"ok": True, **bundle})


@router.get("/api/auth/me")
def api_auth_me(request: Request):
    identity = current_identity(request)
    return JSONResponse({
        "ok": bool(identity),
        "mode": auth_mode(),
        "user": {
            "id": (identity or {}).get("userId", ""),
            "name": (identity or {}).get("userName", ""),
        },
        "source": (identity or {}).get("source", ""),
        "readOnly": bool((identity or {}).get("readOnly")),
        "expiresAt": (identity or {}).get("expiresAt", 0),
    })


@router.post("/api/auth/csp-report")
async def api_auth_csp_report(request: Request):
    """A page of the application was shown inside a foreign site."""
    now = time.time()
    with _csp_lock:
        if now - _csp_window[0] > 3600:
            _csp_window[0], _csp_window[1] = now, 0
        _csp_window[1] += 1
        count = _csp_window[1]
    if count <= CSP_REPORTS_PER_HOUR:
        body = (await request.body())[:4000]
        write_debug_log("auth_frame_violation", {
            "report": body.decode("utf-8", errors="replace"),
            "userAgent": request.headers.get("user-agent", "")[:160],
        })
    return Response(status_code=204)
