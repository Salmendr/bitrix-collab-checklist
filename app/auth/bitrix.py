"""Check of the key Bitrix24 opens the application with.

AUTH_ID cannot be read: it is a random OAuth access key that only Bitrix24
knows the owner of. The server asks the portal itself (user.current) whose key
it is, always at a domain from the settings, never at an address taken from
the request, and with app.info that the key was given to this application.
"""
from __future__ import annotations

import hashlib
import threading
import time

import requests

from app.auth.config import bitrix_app_id, bitrix_portal_domains, normalize_domain
from app.logging_utils import write_debug_log


REQUEST_TIMEOUT_SECONDS = 10
# Bitrix24 sends the same key to the chat button and to the window opened
# from it a second later; one check serves both.
CACHE_SECONDS = 300

_cache_lock = threading.Lock()
_cache: dict[str, tuple[float, dict]] = {}
_app_info_logged = False


class BitrixAuthError(Exception):
    def __init__(self, reason: str, detail: str = ""):
        super().__init__(reason)
        self.reason = reason
        self.detail = detail


def _fingerprint(auth_id: str) -> str:
    return hashlib.sha256(auth_id.encode("utf-8")).hexdigest()


def _call(domain: str, method: str, auth_id: str) -> dict:
    try:
        response = requests.get(
            f"https://{domain}/rest/{method}.json",
            params={"auth": auth_id},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
    except requests.RequestException as exc:
        raise BitrixAuthError("bitrix_unreachable", str(exc)) from exc
    try:
        data = response.json()
    except ValueError:
        data = {}
    if response.status_code != 200 or not isinstance(data, dict) or data.get("error"):
        error = data.get("error") if isinstance(data, dict) else ""
        raise BitrixAuthError("bitrix_rejected", f"{method}: {response.status_code} {error or ''}".strip())
    result = data.get("result")
    return result if isinstance(result, dict) else {}


def _check_app(domain: str, auth_id: str) -> None:
    """The key must be the key of this application, not of another one."""
    global _app_info_logged
    expected, explicit = bitrix_app_id()
    try:
        info = _call(domain, "app.info", auth_id)
    except BitrixAuthError as exc:
        # app.info is an extra check: when Bitrix24 does not answer it, the
        # key is still proven by user.current.
        write_debug_log("auth_bitrix_app_info_unavailable", {"domain": domain, "error": exc.detail or exc.reason})
        return
    found = {str(info.get(key) or "") for key in ("ID", "CODE", "CLIENT_ID")} - {""}
    if not _app_info_logged:
        _app_info_logged = True
        write_debug_log("auth_bitrix_app_info", {
            "domain": domain,
            "expectedAppId": expected,
            "id": str(info.get("ID") or ""),
            "code": str(info.get("CODE") or ""),
            "status": str(info.get("STATUS") or ""),
        })
    if expected and found and expected not in found:
        if explicit:
            raise BitrixAuthError("foreign_application", f"app.info {sorted(found)} != {expected}")
        # The ID taken from APP_PORTAL_PATH is only a guess: a mismatch is
        # reported, the key stays proven by user.current. BITRIX_APP_ID makes
        # the check strict.
        write_debug_log("auth_bitrix_app_id_mismatch", {"domain": domain, "expected": expected, "found": sorted(found)})


def verify_bitrix_auth(auth_id: str, domain: str) -> dict:
    """The employee a Bitrix24 key belongs to, else BitrixAuthError."""
    auth_id = str(auth_id or "").strip()
    domain = normalize_domain(domain)
    allowed = bitrix_portal_domains()
    if not allowed:
        raise BitrixAuthError("portal_not_configured", "set BITRIX_PORTAL_DOMAIN or BITRIX_TECH_WEBHOOK_URL")
    if not auth_id:
        raise BitrixAuthError("no_auth_id")
    if domain not in allowed:
        raise BitrixAuthError("domain_not_allowed", domain)

    key = _fingerprint(auth_id) + ":" + domain
    now = time.time()
    with _cache_lock:
        cached = _cache.get(key)
        if cached and cached[0] > now:
            return dict(cached[1])

    user = _call(domain, "user.current", auth_id)
    user_id = str(user.get("ID") or "").strip()
    if not user_id.isdigit():
        raise BitrixAuthError("bitrix_rejected", "user.current: no user ID")
    _check_app(domain, auth_id)

    name = " ".join(
        part for part in (str(user.get("NAME") or "").strip(), str(user.get("LAST_NAME") or "").strip()) if part
    )
    identity = {
        "userId": user_id,
        "userName": name or f"ID {user_id}",
        "source": "bitrix",
        "authAt": int(now),
        "domain": domain,
    }
    with _cache_lock:
        for stale in [k for k, (expires, _) in _cache.items() if expires <= now]:
            _cache.pop(stale, None)
        _cache[key] = (now + CACHE_SECONDS, identity)
    return dict(identity)
