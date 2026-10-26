"""Check of the token the platform portal opens the application with.

Like the AUTH_ID of Bitrix24, the token is a random string only the portal
knows the owner of. The server asks the portal whose token it is, always at
the address of the settings (PORTAL_AUTH_URL), never at one taken from the
request. The portal answers with the employee and their ID in Bitrix24:

  200 {"result": {"ID": "5012", "NAME": "Иван", "LAST_NAME": "Иванов",
                  "BITRIX_USER_ID": "138"}}
  401 {"error": "invalid_token" | "expired_token", "error_description": "..."}

A window of the portal is open for viewing only, whoever opened it: the
server does not need the employee and acts for one viewer (PORTAL_VIEWER),
the key of the window refuses every change. The data of the employee in the
answer is only written to the log.
"""
from __future__ import annotations

import hashlib
import threading
import time

import requests

from app.auth.config import (
    normalize_domain,
    portal_auth_key,
    portal_auth_method,
    portal_auth_url,
    portal_origins,
)
from app.logging_utils import write_debug_log


REQUEST_TIMEOUT_SECONDS = 10
# Until the users of the portal and Bitrix24 are matched, every window of the
# portal acts for this viewer.
PORTAL_VIEWER = {"userId": "1", "userName": "Компания ТОЛК"}
# The portal gives a token for one opening; a reload of the window (F5 sends
# the form again) is served by the check already made.
CACHE_SECONDS = 300

_cache_lock = threading.Lock()
_cache: dict[str, tuple[float, dict]] = {}
_plain_http_logged: set[str] = set()


class PortalAuthError(Exception):
    def __init__(self, reason: str, detail: str = ""):
        super().__init__(reason)
        self.reason = reason
        self.detail = detail


def _fingerprint(auth_id: str) -> str:
    return hashlib.sha256(auth_id.encode("utf-8")).hexdigest()


def _hide(text: str, auth_id: str) -> str:
    """An error of requests names the address, with the token in it."""
    return text.replace(auth_id, "<hidden>") if auth_id else text


def _ask(url: str, auth_id: str) -> dict:
    headers = {"Accept": "application/json"}
    key = portal_auth_key()
    if key:
        headers["X-App-Key"] = key
    try:
        # A redirect could carry the token to another address: the portal
        # must answer itself.
        if portal_auth_method() == "POST":
            response = requests.post(
                url, data={"auth": auth_id}, headers=headers,
                timeout=REQUEST_TIMEOUT_SECONDS, allow_redirects=False,
            )
        else:
            response = requests.get(
                url, params={"auth": auth_id}, headers=headers,
                timeout=REQUEST_TIMEOUT_SECONDS, allow_redirects=False,
            )
    except requests.RequestException as exc:
        raise PortalAuthError("portal_unreachable", _hide(str(exc), auth_id)) from exc
    try:
        data = response.json()
    except ValueError:
        data = None
    if response.status_code >= 500:
        raise PortalAuthError("portal_unreachable", str(response.status_code))
    if response.status_code != 200 or not isinstance(data, dict) or data.get("error"):
        error = data.get("error") if isinstance(data, dict) else ""
        raise PortalAuthError("portal_rejected", f"{response.status_code} {error or ''}".strip())
    result = data.get("result")
    if not isinstance(result, dict):
        raise PortalAuthError("portal_rejected", "200 without result")
    return result


def verify_portal_auth(auth_id: str, domain: str) -> dict:
    """The employee a portal token belongs to, else PortalAuthError."""
    auth_id = str(auth_id or "").strip()
    domain = normalize_domain(domain)
    allowed = portal_origins()
    if not allowed:
        raise PortalAuthError("portal_not_configured", "set PORTAL_DOMAIN")
    if not auth_id:
        raise PortalAuthError("no_auth_id")
    if domain not in allowed:
        raise PortalAuthError("domain_not_allowed", domain)

    key = _fingerprint(auth_id) + ":" + domain
    now = time.time()
    with _cache_lock:
        cached = _cache.get(key)
        if cached and cached[0] > now:
            return dict(cached[1])

    url = portal_auth_url(domain)
    if url.lower().startswith("http://") and domain not in _plain_http_logged:
        _plain_http_logged.add(domain)
        write_debug_log("auth_portal_plain_http", {"domain": domain})
    user = _ask(url, auth_id)
    identity = {
        **PORTAL_VIEWER,
        "source": "portal",
        "readOnly": True,
        "authAt": int(now),
        "domain": domain,
        "portalUserId": str(user.get("ID") or "").strip(),
    }
    with _cache_lock:
        for stale in [k for k, (expires, _) in _cache.items() if expires <= now]:
            _cache.pop(stale, None)
        _cache[key] = (now + CACHE_SECONDS, identity)
    return dict(identity)
