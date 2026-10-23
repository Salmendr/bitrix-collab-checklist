"""Keys of the application.

A key is "v1.<payload>.<signature>": the payload is JSON in base64url, the
signature is HMAC-SHA256 of "v1.<payload>" with the secret of the server, so
nobody without the secret can make or change a key.

Three kinds of keys:
  "s" - the key of a window, 12 hours; sent with every request in a header;
  "l" - a link key, 10 minutes; added to a link that opens a new window or
        reloads a page, accepted only for opening pages and files;
  "b" - a beacon key, 10 minutes; added to the address of the request a
        closing window sends (sendBeacon cannot set headers). Addresses end
        up in server logs, so they never carry the 12 hour key.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import threading
import time

from app.settings import DB_DIR
from app.auth.config import LINK_KEY_SECONDS, session_seconds


SECRET_PATH = DB_DIR / "app_auth.key"
# A window that stays open keeps renewing its key, but never longer than this
# after the employee was checked in Bitrix24.
MAX_AUTH_AGE_SECONDS = 24 * 3600
_secret_lock = threading.Lock()
_secret_cache: bytes | None = None


class TokenError(ValueError):
    """The key is missing, damaged, forged or expired."""


def _secret() -> bytes:
    global _secret_cache
    if _secret_cache:
        return _secret_cache
    with _secret_lock:
        if _secret_cache:
            return _secret_cache
        configured = (os.getenv("APP_AUTH_SECRET", "") or "").strip()
        if configured:
            _secret_cache = configured.encode("utf-8")
            return _secret_cache
        SECRET_PATH.parent.mkdir(parents=True, exist_ok=True)
        try:
            descriptor = os.open(str(SECRET_PATH), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            pass
        else:
            with os.fdopen(descriptor, "wb") as output:
                output.write(secrets.token_hex(48).encode("ascii"))
        key = SECRET_PATH.read_bytes().strip()
        if len(key) < 32:
            raise RuntimeError(f"{SECRET_PATH} is too short to sign keys")
        _secret_cache = key
        return _secret_cache


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _unb64(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def _sign(body: str) -> str:
    return _b64(hmac.new(_secret(), body.encode("ascii"), hashlib.sha256).digest())


def issue(identity: dict, *, kind: str = "s", seconds: int | None = None, sid: str = "") -> str:
    now = int(time.time())
    life = int(seconds or (session_seconds() if kind == "s" else LINK_KEY_SECONDS))
    auth_at = int(identity.get("authAt") or now)
    expires = min(now + life, auth_at + max(MAX_AUTH_AGE_SECONDS, session_seconds()))
    payload = {
        "k": kind,
        "u": str(identity.get("userId") or ""),
        "n": str(identity.get("userName") or ""),
        "src": str(identity.get("source") or "bitrix"),
        "sid": sid or str(identity.get("sid") or "") or secrets.token_hex(6),
        "at": auth_at,
        "iat": now,
        "exp": expires,
    }
    body = "v1." + _b64(json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
    return body + "." + _sign(body)


def verify(token: str, *, kinds: tuple[str, ...] = ("s",)) -> dict:
    """The identity of a valid key, else TokenError with the reason."""
    value = str(token or "").strip()
    if not value:
        raise TokenError("missing")
    parts = value.split(".")
    if len(parts) != 3 or parts[0] != "v1":
        raise TokenError("malformed")
    body = parts[0] + "." + parts[1]
    if not hmac.compare_digest(_sign(body), parts[2]):
        raise TokenError("bad_signature")
    try:
        payload = json.loads(_unb64(parts[1]).decode("utf-8"))
    except Exception:
        raise TokenError("malformed")
    if payload.get("k") not in kinds:
        raise TokenError("wrong_kind")
    if int(payload.get("exp") or 0) < int(time.time()):
        raise TokenError("expired")
    user_id = str(payload.get("u") or "")
    if not user_id:
        raise TokenError("no_user")
    return {
        "userId": user_id,
        "userName": str(payload.get("n") or ""),
        "source": str(payload.get("src") or ""),
        "sid": str(payload.get("sid") or ""),
        "kind": payload.get("k"),
        "authAt": int(payload.get("at") or payload.get("iat") or 0),
        "expiresAt": int(payload.get("exp") or 0),
    }


def client_bundle(identity: dict) -> dict:
    """What a page needs: the key of its window and a link key."""
    sid = str(identity.get("sid") or "") or secrets.token_hex(6)
    identity = {**identity, "sid": sid}
    session_token = issue(identity, kind="s", sid=sid)
    link_token = issue(identity, kind="l", sid=sid)
    return {
        "token": session_token,
        "linkToken": link_token,
        "beaconToken": issue(identity, kind="b", sid=sid),
        "linkExpiresAt": verify(link_token, kinds=("l",))["expiresAt"],
        "user": {"id": str(identity.get("userId") or ""), "name": str(identity.get("userName") or "")},
        "source": str(identity.get("source") or "bitrix"),
        "expiresAt": verify(session_token)["expiresAt"],
    }
