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
    return " ".join(sources)
