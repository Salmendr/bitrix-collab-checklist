"""Settings of the authorization, read from the environment at start.

AUTH_MODE               on (default) - a key is required;
                        report - everything is checked, but a request that
                        would be closed passes and is written to the log
                        (event auth_would_block);
                        off - no checks, as before.
BITRIX_PORTAL_DOMAIN    domains of the Bitrix24 portal whose keys are
                        accepted, comma separated; by default the host of
                        BITRIX_TECH_WEBHOOK_URL. Only these pages may show
                        the application inside them.
BITRIX_APP_ID           ID of the application in the portal: a key given to
                        another application of the portal is refused
                        (app.info). Without it the number in APP_PORTAL_PATH
                        is compared and a mismatch is only logged.
APP_AUTH_SECRET         secret of the keys of the application; by default
                        created once in Volume/db/app_auth.key.
APP_AUTH_SESSION_HOURS  life of the key of the application, 12 by default.
AUTH_FRAME_ANCESTORS    pages allowed to show the application inside them,
                        by default the portal domains; "*" - any page.

The platform portal (our own, not Bitrix24) opens the application the same
way with its own token:
PORTAL_DOMAIN           addresses of the platform portal whose tokens are
                        accepted, comma separated: "portal.example.ru" (https)
                        or "http://192.168.90.68" (a test address of the local
                        network may go over http). The portal sends this
                        address without the scheme in DOMAIN; only these
                        pages may show the application inside them too.
PORTAL_AUTH_URL         where the token is checked; by default
                        <address of the portal>/rest/user.current.json.
PORTAL_AUTH_METHOD      GET (default, ?auth=<token>) or POST (auth=<token> in
                        a form body).
PORTAL_AUTH_KEY         sent to the portal in the header X-App-Key, if set.
"""
from __future__ import annotations

import os
import re
from urllib.parse import urlparse

from app.settings import APP_PORTAL_PATH, BITRIX_TECH_WEBHOOK_URL


AUTH_MODES = ("on", "report", "off")

# A link key is added to a link at the moment of a click; it only has to
# live until the new window has opened. An open window renews its keys when
# half of this time is gone, so its links always carry a valid key.
LINK_KEY_SECONDS = 10 * 60


def auth_mode() -> str:
    value = (os.getenv("AUTH_MODE", "") or "").strip().lower()
    return value if value in AUTH_MODES else "on"


def session_seconds() -> int:
    try:
        hours = float((os.getenv("APP_AUTH_SESSION_HOURS", "") or "12").strip())
    except ValueError:
        hours = 12.0
    return int(max(1.0, min(hours, 24.0 * 7)) * 3600)


def normalize_domain(value: str) -> str:
    raw = str(value or "").strip().lower()
    if "://" in raw:
        raw = urlparse(raw).netloc
    raw = raw.split("/", 1)[0].strip()
    return raw.rstrip(".")


def bitrix_portal_domains() -> tuple[str, ...]:
    configured = (os.getenv("BITRIX_PORTAL_DOMAIN", "") or "").strip()
    if configured:
        domains = [normalize_domain(part) for part in configured.split(",")]
    else:
        domains = [normalize_domain(urlparse(BITRIX_TECH_WEBHOOK_URL).netloc)] if BITRIX_TECH_WEBHOOK_URL else []
    return tuple(domain for domain in dict.fromkeys(domains) if domain)


def bitrix_app_id() -> tuple[str, bool]:
    """The ID of the application and whether it was set explicitly."""
    configured = (os.getenv("BITRIX_APP_ID", "") or "").strip()
    if configured:
        return configured, True
    match = re.search(r"/app/([^/]+)/?", APP_PORTAL_PATH or "")
    return (match.group(1) if match else ""), False


def frame_ancestors() -> str:
    """Pages that may show the application inside them.

    AUTH_FRAME_ANCESTORS replaces the list (CSP sources, space separated);
    "*" turns the restriction off.
    """
    configured = (os.getenv("AUTH_FRAME_ANCESTORS", "") or "").strip()
    if configured == "*":
        return ""
    if configured:
        return configured
    sources = ["'self'"] + [f"https://{domain}" for domain in bitrix_portal_domains()]
    sources += [origin for origin in portal_origins().values() if origin not in sources]
    return " ".join(sources)


def portal_origins() -> dict[str, str]:
    """Domain of the platform portal -> its origin with the scheme."""
    origins: dict[str, str] = {}
    for part in (os.getenv("PORTAL_DOMAIN", "") or "").split(","):
        raw = part.strip()
        domain = normalize_domain(raw)
        if domain:
            scheme = "http" if raw.lower().startswith("http://") else "https"
            origins.setdefault(domain, f"{scheme}://{domain}")
    return origins


def is_portal_domain(domain: str) -> bool:
    found = normalize_domain(domain)
    return bool(found) and found in portal_origins()


def portal_auth_url(domain: str) -> str:
    configured = (os.getenv("PORTAL_AUTH_URL", "") or "").strip()
    if configured:
        return configured
    origin = portal_origins().get(normalize_domain(domain), "")
    return f"{origin}/rest/user.current.json" if origin else ""


def portal_auth_method() -> str:
    return "POST" if (os.getenv("PORTAL_AUTH_METHOD", "") or "").strip().upper() == "POST" else "GET"


def portal_auth_key() -> str:
    return (os.getenv("PORTAL_AUTH_KEY", "") or "").strip()
