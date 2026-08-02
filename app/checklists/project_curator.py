from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.bitrix.client import bitrix_webhook_call
from app.checklists.bitrix_users import (
    get_cached_bitrix_user,
    synchronize_bitrix_users,
)
from app.checklists.storage import (
    get_project_storage_context,
    save_project_storage_context,
)
from app.checklists.utils import clean_cell_value, normalize_dialog_id


DEFAULT_OBJECT_ENTITY_TYPE_ID = 1064


class ProjectCuratorError(RuntimeError):
    pass


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _positive_int(value: Any, default: int = 0) -> int:
    try:
        number = int(str(value or "").strip())
    except Exception:
        return int(default or 0)
    return number if number > 0 else int(default or 0)


def normalize_project_bitrix_context(value: Any) -> dict:
    raw = value if isinstance(value, dict) else {}
    entity_type_id = _positive_int(
        raw.get("objectEntityTypeId")
        or raw.get("entityTypeId"),
        DEFAULT_OBJECT_ENTITY_TYPE_ID,
    )
    object_item_id = _positive_int(
        raw.get("objectItemId")
        or raw.get("itemId")
    )
    curator_raw = raw.get("curator") if isinstance(raw.get("curator"), dict) else {}
    curator_user_id = clean_cell_value(
        curator_raw.get("userId")
        or curator_raw.get("assignedById")
        or raw.get("curatorUserId")
    )
    curator_name = clean_cell_value(
        curator_raw.get("name")
        or raw.get("curatorName")
    )

    result = dict(raw)
    result.update({
        "objectEntityTypeId": entity_type_id,
        "objectItemId": object_item_id,
        "objectTitle": clean_cell_value(raw.get("objectTitle")),
        "resolutionStatus": clean_cell_value(raw.get("resolutionStatus")),
        "resolutionError": clean_cell_value(raw.get("resolutionError")),
        "lastResolvedAt": clean_cell_value(raw.get("lastResolvedAt")),
        "curator": {
            "userId": curator_user_id,
            "name": curator_name,
            "resolvedAt": clean_cell_value(curator_raw.get("resolvedAt")),
            "source": clean_cell_value(curator_raw.get("source")),
        },
    })
    return result


def public_project_curator(context: dict | None) -> dict:
    project = dict(context or {})
    bitrix = normalize_project_bitrix_context(project.get("bitrix"))
    curator = dict(bitrix.get("curator") or {})
    configured = bool(
        _positive_int(bitrix.get("objectEntityTypeId"))
        and _positive_int(bitrix.get("objectItemId"))
    )
    return {
        "dialogId": normalize_dialog_id(project.get("dialogId")),
        "projectId": clean_cell_value(project.get("projectId")),
        "projectName": clean_cell_value(project.get("projectName")),
        "configured": configured,
        "objectEntityTypeId": _positive_int(bitrix.get("objectEntityTypeId")),
        "objectItemId": _positive_int(bitrix.get("objectItemId")),
        "objectTitle": clean_cell_value(bitrix.get("objectTitle")),
        "curator": {
            "userId": clean_cell_value(curator.get("userId")),
            "name": clean_cell_value(curator.get("name")),
            "resolvedAt": clean_cell_value(curator.get("resolvedAt")),
            "source": clean_cell_value(curator.get("source")),
        },
        "resolutionStatus": clean_cell_value(bitrix.get("resolutionStatus")),
        "resolutionError": clean_cell_value(bitrix.get("resolutionError")),
        "lastResolvedAt": clean_cell_value(bitrix.get("lastResolvedAt")),
    }


def _response_item(response: Any) -> dict:
    if not isinstance(response, dict):
        return {}
    result = response.get("result")
    if isinstance(result, dict) and isinstance(result.get("item"), dict):
        return dict(result.get("item") or {})
    if isinstance(response.get("item"), dict):
        return dict(response.get("item") or {})
    return {}


def _save_resolution(dialog_id: str, context: dict, bitrix: dict) -> dict:
    updated = dict(context or {})
    updated["bitrix"] = normalize_project_bitrix_context(bitrix)
    save_project_storage_context(dialog_id, updated)
    return get_project_storage_context(dialog_id) or updated


def get_project_curator(dialog_id: str) -> dict:
    normalized_dialog_id = normalize_dialog_id(dialog_id)
    context = get_project_storage_context(normalized_dialog_id)
    if not context:
        return {
            "dialogId": normalized_dialog_id,
            "configured": False,
            "objectEntityTypeId": DEFAULT_OBJECT_ENTITY_TYPE_ID,
            "objectItemId": 0,
            "objectTitle": "",
            "curator": {"userId": "", "name": "", "resolvedAt": "", "source": ""},
            "resolutionStatus": "missing_context",
            "resolutionError": "project storage context not found",
            "lastResolvedAt": "",
        }
    return public_project_curator(context)


def resolve_project_curator(
    dialog_id: str,
    *,
    force: bool = False,
    allow_cached: bool = True,
) -> dict:
    normalized_dialog_id = normalize_dialog_id(dialog_id)
    context = get_project_storage_context(normalized_dialog_id)
    if not context:
        raise ProjectCuratorError("project storage context not found")

    bitrix = normalize_project_bitrix_context(context.get("bitrix"))
    entity_type_id = _positive_int(bitrix.get("objectEntityTypeId"))
    object_item_id = _positive_int(bitrix.get("objectItemId"))
    cached_curator = dict(bitrix.get("curator") or {})

    if not force and allow_cached and clean_cell_value(cached_curator.get("userId")):
        return public_project_curator(context)

    if not entity_type_id or not object_item_id:
        bitrix["resolutionStatus"] = "missing_context"
        bitrix["resolutionError"] = "objectEntityTypeId and objectItemId are required"
        bitrix["lastResolvedAt"] = _iso_now()
        saved = _save_resolution(normalized_dialog_id, context, bitrix)
        return public_project_curator(saved)

    response = bitrix_webhook_call(
        "crm.item.get",
        {
            "entityTypeId": entity_type_id,
            "id": object_item_id,
            "useOriginalUfNames": "N",
        },
    )
    if not isinstance(response, dict) or response.get("error"):
        error = clean_cell_value(
            (response or {}).get("error_description")
            or (response or {}).get("error")
            or "Bitrix24 crm.item.get failed"
        )
        bitrix["resolutionStatus"] = "error"
        bitrix["resolutionError"] = error
        bitrix["lastResolvedAt"] = _iso_now()
        saved = _save_resolution(normalized_dialog_id, context, bitrix)
        return public_project_curator(saved)

    item = _response_item(response)
    assigned_by_id = clean_cell_value(
        item.get("assignedById")
        or item.get("ASSIGNED_BY_ID")
    )
    if not assigned_by_id:
        bitrix["objectTitle"] = clean_cell_value(item.get("title") or item.get("TITLE"))
        bitrix["resolutionStatus"] = "missing_assignee"
        bitrix["resolutionError"] = "assignedById is empty for project object"
        bitrix["lastResolvedAt"] = _iso_now()
        saved = _save_resolution(normalized_dialog_id, context, bitrix)
        return public_project_curator(saved)

    cached_user = get_cached_bitrix_user(assigned_by_id)
    if not cached_user:
        synchronize_bitrix_users(force=True)
        cached_user = get_cached_bitrix_user(assigned_by_id)

    resolved_at = _iso_now()
    bitrix["objectTitle"] = clean_cell_value(item.get("title") or item.get("TITLE"))
    bitrix["curator"] = {
        "userId": assigned_by_id,
        "name": clean_cell_value((cached_user or {}).get("name")),
        "resolvedAt": resolved_at,
        "source": "crm.item.get",
    }
    bitrix["resolutionStatus"] = "resolved"
    bitrix["resolutionError"] = ""
    bitrix["lastResolvedAt"] = resolved_at
    saved = _save_resolution(normalized_dialog_id, context, bitrix)
    return public_project_curator(saved)


def resolve_curator_for_external_recipient(
    dialog_id: str,
    *,
    current_user_id: str = "",
    current_name: str = "",
) -> dict:
    manual_user_id = clean_cell_value(current_user_id)
    manual_name = clean_cell_value(current_name)
    if manual_user_id:
        cached = get_cached_bitrix_user(manual_user_id)
        return {
            "userId": clean_cell_value((cached or {}).get("userId")) or manual_user_id,
            "name": clean_cell_value((cached or {}).get("name")) or manual_name,
            "source": "manual",
            "status": "manual",
            "error": "",
        }

    try:
        resolved = resolve_project_curator(dialog_id, allow_cached=True)
    except Exception as exc:
        return {
            "userId": "",
            "name": manual_name,
            "source": "",
            "status": "error",
            "error": str(exc),
        }

    curator = dict(resolved.get("curator") or {})
    return {
        "userId": clean_cell_value(curator.get("userId")),
        "name": clean_cell_value(curator.get("name")) or manual_name,
        "source": clean_cell_value(curator.get("source")),
        "status": clean_cell_value(resolved.get("resolutionStatus")),
        "error": clean_cell_value(resolved.get("resolutionError")),
    }
