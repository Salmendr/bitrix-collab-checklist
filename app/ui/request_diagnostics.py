from __future__ import annotations

import hashlib
import uuid
from typing import Any, Iterable, Mapping
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from fastapi import Request

from app.logging_utils import write_debug_log


_SENSITIVE_PARTS = (
    "auth",
    "token",
    "secret",
    "password",
    "refresh",
)


def _is_sensitive_key(key: str) -> bool:
    normalized = str(key or "").strip().lower()
    return normalized == "app_sid" or any(
        part in normalized for part in _SENSITIVE_PARTS
    )


def _trim(value: Any, limit: int = 500) -> str:
    text = str(value or "")
    return text if len(text) <= limit else f"{text[:limit]}…"


def _fingerprint(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def _sanitize_url(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""

    try:
        parsed = urlsplit(raw)
        safe_query = []
        for key, item_value in parse_qsl(
            parsed.query,
            keep_blank_values=True,
        ):
            safe_query.append(
                (
                    key,
                    "<redacted>"
                    if _is_sensitive_key(key)
                    else _trim(item_value, 240),
                )
            )

        return urlunsplit(
            (
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                urlencode(safe_query, doseq=True),
                "",
            )
        )
    except Exception:
        return _trim(raw)


def _safe_query(request: Request) -> dict[str, str]:
    return {
        str(key): (
            "<redacted>"
            if _is_sensitive_key(key)
            else _trim(value, 240)
        )
        for key, value in request.query_params.multi_items()
    }


def new_ui_request_id() -> str:
    return f"ui_{uuid.uuid4().hex[:16]}"


def write_ui_request_diagnostic(
    event: str,
    request: Request,
    *,
    request_id: str = "",
    form_keys: Iterable[Any] = (),
    extra: Mapping[str, Any] | None = None,
) -> str:
    resolved_request_id = str(request_id or new_ui_request_id())
    try:
        query = _safe_query(request)
        app_sid = (
            request.query_params.get("APP_SID")
            or request.query_params.get("app_sid")
            or ""
        )

        payload = {
            "diagnosticVersion": "8.15.9.8-popup-host-diagnostics",
            "requestId": resolved_request_id,
            "method": request.method,
            "path": request.url.path,
            "url": _sanitize_url(str(request.url)),
            "query": query,
            "queryKeys": sorted(
                str(key) for key in request.query_params.keys()
            ),
            "formKeys": sorted(str(key) for key in form_keys),
            "appSidPresent": bool(app_sid),
            "appSidFingerprint": _fingerprint(app_sid),
            "headers": {
                "host": _trim(request.headers.get("host"), 240),
                "origin": _sanitize_url(request.headers.get("origin")),
                "referer": _sanitize_url(request.headers.get("referer")),
                "userAgent": _trim(
                    request.headers.get("user-agent"),
                    500,
                ),
                "secFetchDest": _trim(
                    request.headers.get("sec-fetch-dest"),
                    80,
                ),
                "secFetchMode": _trim(
                    request.headers.get("sec-fetch-mode"),
                    80,
                ),
                "secFetchSite": _trim(
                    request.headers.get("sec-fetch-site"),
                    80,
                ),
                "xForwardedHost": _trim(
                    request.headers.get("x-forwarded-host"),
                    240,
                ),
                "xForwardedPrefix": _trim(
                    request.headers.get("x-forwarded-prefix"),
                    240,
                ),
                "xForwardedProto": _trim(
                    request.headers.get("x-forwarded-proto"),
                    80,
                ),
            },
            "extra": dict(extra or {}),
        }
        write_debug_log(str(event or "popup_diag_http_unknown"), payload)
    except Exception:
        # Diagnostics must never alter the UI route response.
        pass
    return resolved_request_id
