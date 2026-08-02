from __future__ import annotations

from collections import OrderedDict
from typing import Any
from urllib.parse import urlparse

from app.checklists.config import get_checklist_config
from app.checklists.document_version_links import (
    build_configured_version_file_url,
)
from app.checklists.documents import normalize_documents_list
from app.checklists.edit_session_changes import (
    list_edit_session_operations,
)
from app.checklists.storage import get_checklist
from app.checklists.utils import (
    clean_cell_value,
    normalize_checklist_key,
    normalize_dialog_id,
    normalize_status,
)


VISIBLE_OPERATION_STATUSES = frozenset({"applied", "committed"})


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return default


def _dict(value: Any) -> dict:
    return value if isinstance(value, dict) else {}


def _item_from(operation: dict, side: str) -> dict:
    return _dict(_dict(operation.get(side)).get("item"))


def _document_from(operation: dict, side: str) -> dict:
    return _dict(_dict(operation.get(side)).get("document"))


def _archive_version_from(operation: dict, side: str) -> dict:
    return _dict(_dict(operation.get(side)).get("archiveVersion"))


def _item_name(operation: dict, final_items: dict[str, dict]) -> str:
    item_id = clean_cell_value(operation.get("itemId"))
    final_item = final_items.get(item_id) or {}
    return (
        clean_cell_value(final_item.get("name"))
        or clean_cell_value(_item_from(operation, "after").get("name"))
        or clean_cell_value(_item_from(operation, "before").get("name"))
        or "Без названия"
    )


def _status_text(value: Any) -> str:
    normalized = normalize_status(clean_cell_value(value))
    return normalized or "Нет"


def _group_title(checklist_key: str, group_id: Any) -> str:
    numeric_group_id = _safe_int(group_id)
    title = get_checklist_config(checklist_key).get_group_title(
        numeric_group_id
    )
    return title or (
        f"Раздел {numeric_group_id}"
        if numeric_group_id
        else "Без раздела"
    )


def _order_text(
    checklist_key: str,
    group_id: Any,
    position: Any,
) -> str:
    title = _group_title(checklist_key, group_id)
    numeric_position = _safe_int(position)
    if numeric_position > 0:
        return f"{title} / позиция {numeric_position}"
    return title


def _absolute_version_url(
    dialog_id: str,
    checklist_key: str,
    item_id: str,
    version_id: str,
) -> str:
    if not all((dialog_id, checklist_key, item_id, version_id)):
        return ""

    url = build_configured_version_file_url(
        dialog_id,
        checklist_key,
        item_id,
        version_id,
    )
    parsed = urlparse(url)
    if parsed.scheme in {"http", "https"} and parsed.netloc:
        return url
    return ""


def _document_identity(operation: dict, document: dict) -> str:
    return (
        clean_cell_value(operation.get("seriesId"))
        or clean_cell_value(document.get("seriesId"))
        or clean_cell_value(operation.get("documentId"))
        or clean_cell_value(document.get("id"))
        or clean_cell_value(operation.get("operationId"))
    )


def _document_name(document: dict, payload: dict, *keys: str) -> str:
    for key in keys:
        value = clean_cell_value(payload.get(key))
        if value:
            return value
    return clean_cell_value(document.get("name")) or "Файл"


def _documents_by_identity(item: dict) -> dict[str, dict]:
    result: dict[str, dict] = {}
    for document in normalize_documents_list(item.get("documents")):
        identity = (
            clean_cell_value(document.get("seriesId"))
            or clean_cell_value(document.get("id"))
        )
        if identity:
            result[identity] = document
    return result


def _upsert_scalar(
    scalar_changes: OrderedDict,
    *,
    key: tuple,
    sequence_no: int,
    field: str,
    item_id: str,
    item_name: str,
    old_value: Any,
    new_value: Any,
    extra: dict | None = None,
) -> None:
    old_text = clean_cell_value(old_value)
    new_text = clean_cell_value(new_value)

    existing = scalar_changes.get(key)
    if existing is None:
        scalar_changes[key] = {
            "sequenceNo": sequence_no,
            "field": field,
            "itemId": item_id,
            "itemName": item_name,
            "oldValue": old_text,
            "newValue": new_text,
            **(extra or {}),
        }
        return

    existing["newValue"] = new_text
    existing["itemName"] = item_name or existing.get("itemName")
    if extra:
        existing.update(extra)


def _touch_document_state(
    document_states: OrderedDict,
    *,
    operation: dict,
    checklist_key: str,
    final_items: dict[str, dict],
    action: str,
    before_document: dict | None = None,
    after_document: dict | None = None,
) -> None:
    before_document = before_document or {}
    after_document = after_document or {}
    payload = _dict(operation.get("payload"))
    identity_document = before_document or after_document
    identity = _document_identity(operation, identity_document)
    if not identity:
        return

    item_id = clean_cell_value(operation.get("itemId"))
    state_key = (item_id, identity)
    state = document_states.get(state_key)
    sequence_no = _safe_int(operation.get("sequenceNo"))

    if state is None:
        initial_exists = action != "upload"
        initial_name = (
            _document_name(
                before_document,
                payload,
                "oldFileName",
                "fileName",
            )
            if initial_exists
            else ""
        )
        state = {
            "sequenceNo": sequence_no,
            "itemId": item_id,
            "itemName": _item_name(operation, final_items),
            "seriesId": identity,
            "initialExists": initial_exists,
            "initialName": initial_name,
            "finalExists": initial_exists,
            "finalName": initial_name,
            "finalVersionId": clean_cell_value(
                before_document.get("id")
            ),
            "archiveVersion": 0,
            "archiveVersionLabel": "",
            "replacementCount": 0,
        }
        document_states[state_key] = state

    state["itemName"] = _item_name(operation, final_items)

    if action == "upload":
        state["finalExists"] = True
        state["finalName"] = _document_name(
            after_document,
            payload,
            "fileName",
            "newFileName",
        )
        state["finalVersionId"] = (
            clean_cell_value(after_document.get("id"))
            or clean_cell_value(operation.get("documentId"))
        )

    elif action == "replace":
        state["replacementCount"] = (
            _safe_int(state.get("replacementCount")) + 1
        )
        state["finalExists"] = True
        state["finalName"] = _document_name(
            after_document,
            payload,
            "newFileName",
            "fileName",
        )
        state["finalVersionId"] = (
            clean_cell_value(after_document.get("id"))
            or clean_cell_value(operation.get("documentId"))
        )
        archive_version = _archive_version_from(operation, "after")
        state["archiveVersion"] = _safe_int(
            archive_version.get("version")
        )
        state["archiveVersionLabel"] = clean_cell_value(
            archive_version.get("versionLabel")
        )

    elif action == "remove":
        state["finalExists"] = False
        state["finalName"] = ""
        state["finalVersionId"] = ""


def _derive_document_diff(
    document_states: OrderedDict,
    *,
    operation: dict,
    checklist_key: str,
    final_items: dict[str, dict],
) -> None:
    before_documents = _documents_by_identity(
        _item_from(operation, "before")
    )
    after_documents = _documents_by_identity(
        _item_from(operation, "after")
    )

    for identity, document in before_documents.items():
        if identity in after_documents:
            continue
        synthetic_operation = dict(operation)
        synthetic_operation["seriesId"] = identity
        synthetic_operation["documentId"] = clean_cell_value(
            document.get("id")
        )
        _touch_document_state(
            document_states,
            operation=synthetic_operation,
            checklist_key=checklist_key,
            final_items=final_items,
            action="remove",
            before_document=document,
        )

    for identity, document in after_documents.items():
        if identity in before_documents:
            continue
        synthetic_operation = dict(operation)
        synthetic_operation["seriesId"] = identity
        synthetic_operation["documentId"] = clean_cell_value(
            document.get("id")
        )
        _touch_document_state(
            document_states,
            operation=synthetic_operation,
            checklist_key=checklist_key,
            final_items=final_items,
            action="upload",
            after_document=document,
        )


def normalize_session_operations(
    operations: list[dict],
    *,
    dialog_id: str,
    checklist_key: str,
    checklist_data: dict,
) -> list[dict]:
    """
    Превращает журнал edit-session в одну нормализованную сводку.

    Повторные изменения одного поля схлопываются до первого старого и
    последнего нового значения. Цепочка замен одной seriesId отображается
    как A → C, хотя все промежуточные версии остаются в архиве.
    """
    checklist_key = normalize_checklist_key(checklist_key)
    dialog_id = normalize_dialog_id(dialog_id)
    final_items = {
        clean_cell_value(item.get("id")): item
        for item in (checklist_data or {}).get("items", []) or []
        if clean_cell_value(item.get("id"))
    }

    scalar_changes: OrderedDict[tuple, dict] = OrderedDict()
    document_states: OrderedDict[tuple, dict] = OrderedDict()
    archive_deletes: OrderedDict[tuple, dict] = OrderedDict()
    added_items: OrderedDict[str, dict] = OrderedDict()

    sorted_operations = sorted(
        [
            operation
            for operation in operations or []
            if clean_cell_value(operation.get("status"))
            in VISIBLE_OPERATION_STATUSES
        ],
        key=lambda operation: _safe_int(
            operation.get("sequenceNo")
        ),
    )

    for operation in sorted_operations:
        operation_type = clean_cell_value(
            operation.get("operationType")
        )
        sequence_no = _safe_int(operation.get("sequenceNo"))
        item_id = clean_cell_value(operation.get("itemId"))
        item_name = _item_name(operation, final_items)
        payload = _dict(operation.get("payload"))
        before_item = _item_from(operation, "before")
        after_item = _item_from(operation, "after")

        if operation_type == "checklist_item_add":
            final_item = final_items.get(item_id) or after_item
            added_items[item_id] = {
                "sequenceNo": sequence_no,
                "field": "add-item",
                "itemId": item_id,
                "itemName": (
                    clean_cell_value(final_item.get("name"))
                    or item_name
                ),
                "oldValue": "",
                "newValue": (
                    clean_cell_value(final_item.get("name"))
                    or item_name
                ),
            }
            continue

        if operation_type == "checklist_item_rename":
            _upsert_scalar(
                scalar_changes,
                key=(item_id, "name"),
                sequence_no=sequence_no,
                field="name",
                item_id=item_id,
                item_name=item_name,
                old_value=(
                    payload.get("oldName")
                    or _dict(operation.get("before")).get("name")
                    or before_item.get("name")
                ),
                new_value=(
                    payload.get("newName")
                    or _dict(operation.get("after")).get("name")
                    or after_item.get("name")
                ),
            )
            continue

        if operation_type == "checklist_item_reorder":
            before = _dict(operation.get("before"))
            after = _dict(operation.get("after"))
            old_order = _order_text(
                checklist_key,
                before.get("groupId") or before_item.get("group"),
                before.get("position") or before_item.get("order"),
            )
            new_order = _order_text(
                checklist_key,
                after.get("groupId") or after_item.get("group"),
                after.get("position") or after_item.get("order"),
            )
            _upsert_scalar(
                scalar_changes,
                key=(item_id, "order"),
                sequence_no=sequence_no,
                field="order",
                item_id=item_id,
                item_name=item_name,
                old_value=old_order,
                new_value=new_order,
            )

            before_status = _status_text(before_item.get("status"))
            after_status = _status_text(after_item.get("status"))
            if before_status != after_status:
                _upsert_scalar(
                    scalar_changes,
                    key=(item_id, "status"),
                    sequence_no=sequence_no,
                    field="status",
                    item_id=item_id,
                    item_name=item_name,
                    old_value=before_status,
                    new_value=after_status,
                )

            _derive_document_diff(
                document_states,
                operation=operation,
                checklist_key=checklist_key,
                final_items=final_items,
            )
            continue

        if operation_type == "checklist_item_update":
            field = clean_cell_value(
                payload.get("field")
                or _dict(operation.get("after")).get("field")
                or _dict(operation.get("before")).get("field")
            )
            if field:
                old_value = before_item.get(field)
                new_value = after_item.get(field)
                if field == "status":
                    old_value = _status_text(old_value)
                    new_value = _status_text(new_value)
                _upsert_scalar(
                    scalar_changes,
                    key=(item_id, field),
                    sequence_no=sequence_no,
                    field=field,
                    item_id=item_id,
                    item_name=item_name,
                    old_value=old_value,
                    new_value=new_value,
                )

            _derive_document_diff(
                document_states,
                operation=operation,
                checklist_key=checklist_key,
                final_items=final_items,
            )
            continue

        if operation_type == "document_upload":
            _touch_document_state(
                document_states,
                operation=operation,
                checklist_key=checklist_key,
                final_items=final_items,
                action="upload",
                after_document=_document_from(operation, "after"),
            )
            continue

        if operation_type == "document_replace":
            _touch_document_state(
                document_states,
                operation=operation,
                checklist_key=checklist_key,
                final_items=final_items,
                action="replace",
                before_document=_document_from(operation, "before"),
                after_document=_document_from(operation, "after"),
            )
            continue

        if operation_type == "document_remove":
            _touch_document_state(
                document_states,
                operation=operation,
                checklist_key=checklist_key,
                final_items=final_items,
                action="remove",
                before_document=_document_from(operation, "before"),
            )
            continue

        if operation_type == "archive_version_delete":
            archive_version = _archive_version_from(
                operation,
                "before",
            )
            archive_id = (
                clean_cell_value(
                    payload.get("archiveVersionId")
                )
                or clean_cell_value(
                    archive_version.get("id")
                )
                or clean_cell_value(
                    operation.get("documentId")
                )
                or clean_cell_value(
                    operation.get("operationId")
                )
            )
            archive_deletes[(item_id, archive_id)] = {
                "sequenceNo": sequence_no,
                "field": "archive-delete",
                "itemId": item_id,
                "itemName": item_name,
                "oldValue": (
                    clean_cell_value(payload.get("fileName"))
                    or clean_cell_value(archive_version.get("name"))
                    or "Архивная версия"
                ),
                "newValue": "Удалена",
            }

    # Для нового пункта показываем его итоговое имя, а не отдельную строку
    # переименования/перестановки, сделанную в той же сессии.
    for added_item_id, added in added_items.items():
        final_item = final_items.get(added_item_id) or {}
        final_name = clean_cell_value(final_item.get("name"))
        if final_name:
            added["itemName"] = final_name
            added["newValue"] = final_name
        scalar_changes.pop((added_item_id, "name"), None)
        scalar_changes.pop((added_item_id, "order"), None)

    normalized: list[dict] = []

    for change in scalar_changes.values():
        if clean_cell_value(change.get("oldValue")) == clean_cell_value(
            change.get("newValue")
        ):
            continue
        normalized.append(change)

    normalized.extend(added_items.values())

    for state in document_states.values():
        initial_exists = bool(state.get("initialExists"))
        final_exists = bool(state.get("finalExists"))
        initial_name = clean_cell_value(state.get("initialName"))
        final_name = clean_cell_value(state.get("finalName"))

        if not initial_exists and not final_exists:
            continue

        if not initial_exists and final_exists:
            field = "document-add"
            old_value = ""
            new_value = final_name or "Файл"
        elif initial_exists and not final_exists:
            field = "document-remove"
            old_value = initial_name or "Файл"
            new_value = "Удалён"
        else:
            if initial_name == final_name:
                continue
            field = "document-replacement"
            old_value = initial_name or "Файл"
            new_value = final_name or "Файл"

        version_id = clean_cell_value(
            state.get("finalVersionId")
        )
        normalized.append({
            "sequenceNo": _safe_int(state.get("sequenceNo")),
            "field": field,
            "itemId": clean_cell_value(state.get("itemId")),
            "itemName": clean_cell_value(state.get("itemName")),
            "oldValue": old_value,
            "newValue": new_value,
            "seriesId": clean_cell_value(state.get("seriesId")),
            "versionId": version_id,
            "newUrl": (
                _absolute_version_url(
                    dialog_id,
                    checklist_key,
                    clean_cell_value(state.get("itemId")),
                    version_id,
                )
                if final_exists
                else ""
            ),
            "archiveVersion": _safe_int(
                state.get("archiveVersion")
            ),
            "archiveVersionLabel": clean_cell_value(
                state.get("archiveVersionLabel")
            ),
            "replacementCount": _safe_int(
                state.get("replacementCount")
            ),
        })

    normalized.extend(archive_deletes.values())
    normalized.sort(key=lambda change: (
        _safe_int(change.get("sequenceNo")),
        clean_cell_value(change.get("field")),
        clean_cell_value(change.get("itemId")),
    ))

    for change in normalized:
        change.pop("sequenceNo", None)

    return normalized


def build_session_summary_sessions(
    *,
    session_id: str,
    fallback_sessions: list[dict] | None = None,
) -> list[dict]:
    fallback_sessions = fallback_sessions or []
    fallback_by_key: dict[tuple[str, str], dict] = {}

    for session in fallback_sessions:
        if not isinstance(session, dict):
            continue
        key = (
            normalize_dialog_id(session.get("dialogId")),
            normalize_checklist_key(session.get("checklistKey")),
        )
        fallback_by_key[key] = session

    operations = list_edit_session_operations(
        session_id=clean_cell_value(session_id),
    )
    operations_by_key: OrderedDict[tuple[str, str], list[dict]] = (
        OrderedDict()
    )

    for operation in operations:
        status = clean_cell_value(operation.get("status"))
        if status not in VISIBLE_OPERATION_STATUSES:
            continue
        key = (
            normalize_dialog_id(operation.get("dialogId")),
            normalize_checklist_key(operation.get("checklistKey")),
        )
        operations_by_key.setdefault(key, []).append(operation)

    if not operations_by_key:
        return fallback_sessions

    all_keys = list(operations_by_key.keys())
    for key in fallback_by_key:
        if key not in operations_by_key:
            all_keys.append(key)

    result: list[dict] = []

    for dialog_id, checklist_key in all_keys:
        fallback = fallback_by_key.get(
            (dialog_id, checklist_key),
            {},
        )
        data = fallback.get("data") or get_checklist(
            dialog_id,
            checklist_key,
        )
        key_operations = operations_by_key.get(
            (dialog_id, checklist_key),
            [],
        )

        changes = (
            normalize_session_operations(
                key_operations,
                dialog_id=dialog_id,
                checklist_key=checklist_key,
                checklist_data=data,
            )
            if key_operations
            else (fallback.get("changes") or [])
        )

        result.append({
            "dialogId": dialog_id,
            "checklistKey": checklist_key,
            "changes": changes,
            "data": data,
        })

    return result
