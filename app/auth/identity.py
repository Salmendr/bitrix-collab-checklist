"""The employee of a request, as checked by the authorization."""
from __future__ import annotations


SCOPE_KEY = "checklist_auth"


def current_identity(request) -> dict | None:
    identity = request.scope.get(SCOPE_KEY)
    return identity if isinstance(identity, dict) and identity.get("userId") else None


def establish(request, identity: dict) -> None:
    """Called by an entry point that has just checked a Bitrix24 key."""
    request.scope[SCOPE_KEY] = dict(identity)


def is_read_only(request) -> bool:
    """A window opened for viewing only (the platform portal)."""
    identity = current_identity(request)
    return bool(identity and identity.get("readOnly"))


def verified_actor(request, user_id: str = "", user_name: str = "") -> tuple[str, str]:
    """The checked employee instead of the one a form names.

    The middleware already replaces the user in JSON bodies and links; forms
    with files (multipart) are read by their routes, which call this.
    """
    identity = current_identity(request)
    if not identity:
        return user_id, user_name
    return identity["userId"], identity.get("userName") or user_name
