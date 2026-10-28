"""«Изыскания» in new ИД checklists (layout 2).

A new ИД checklist gets the item «Изыскания» right after «Тех задание»
with the subitems ИГДИ, ИГИ, ИЭИ, ИГМИ and ИГИ СМР instead of the five
standard survey items. The item cannot be deleted, renamed or put into
another item: the «Изыскания» figure of the object analysis counts its
subitems. The subitems are ordinary ones: they are renamed, moved and
deleted like any subitem.

On Yandex Disk the item is the folder 04_Изыскания and its subitems are
the standard survey folders inside it (01_ИГДИ … 05_ИГИ СМР), created by the
standard warmup in every project. A checklist is usually made before the
project root is prepared, so the folders are bound once the root is known:
right after the checklist is made, then after the standard warmup.
"""
from __future__ import annotations

import json

from app.checklists.definitions.id import (
    ID_LAYOUT_SURVEYS,
    STANDARD_ID_YANDEX_FOLDER_SPECS,
    SURVEY_SUBITEM_NAMES,
    SURVEYS_AFTER_ITEM,
    SURVEYS_FOLDER,
    SURVEYS_ITEM_NAME,
    SURVEYS_SYSTEM_ITEM,
)
from app.checklists.utils import clean_cell_value, normalize_dialog_id
from app.logging_utils import write_debug_log


SURVEYS_ITEM_ID = "item_g1_surveys"
LOCKED_ITEM_RENAME_ERROR = (
    "Пункт «Изыскания» нельзя переименовать: по нему считается процент изысканий"
)
LOCKED_ITEM_DELETE_ERROR = "Пункт «Изыскания» нельзя удалить — переведите его в «Не требуется»"
LOCKED_ITEM_MOVE_ERROR = "Пункт «Изыскания» остаётся пунктом раздела «ИД»"
BUSY_FOLDER_STATUSES = frozenset({"queued", "running"})


def layout_of(data: dict | None) -> int:
    """The layout of an ИД checklist; the locked «Изыскания» marks layout 2
    even where a copy of the data lost the flag."""
    try:
        layout = int((data or {}).get("idLayout") or 1)
    except (TypeError, ValueError):
        layout = 1
    if layout < ID_LAYOUT_SURVEYS and any(
        isinstance(item, dict) and is_locked_item(item)
        for item in (data or {}).get("items") or []
    ):
        layout = ID_LAYOUT_SURVEYS
    return layout


def skipped_standard_names(data: dict | None) -> frozenset[str]:
    """Standard ИД items a checklist of this layout does not get back."""
    if layout_of(data) >= ID_LAYOUT_SURVEYS:
        return frozenset(name.casefold() for name in SURVEY_SUBITEM_NAMES)
    return frozenset()


def is_locked_item(item: dict | None) -> bool:
    """The item «Изыскания» of a new checklist: no delete, rename or nesting."""
    return clean_cell_value((item or {}).get("systemItem")) == SURVEYS_SYSTEM_ITEM


def system_item_fields(item: dict | None) -> dict:
    """The fields of this module that a normalized item keeps."""
    source = item or {}
    result = {}
    system_item = clean_cell_value(source.get("systemItem"))
    if system_item:
        result["systemItem"] = system_item
    hint = source.get("yandexFolderHint")
    if isinstance(hint, dict) and clean_cell_value(hint.get("relativePath")):
        result["yandexFolderHint"] = {
            "relativePath": clean_cell_value(hint.get("relativePath")),
            "name": clean_cell_value(hint.get("name")),
        }
    return result


def _survey_relative_path(name: str) -> str:
    spec = STANDARD_ID_YANDEX_FOLDER_SPECS.get(name) or {}
    return clean_cell_value(spec.get("relativePath"))


def _blank_item(item_id: str, name: str, group_id: int, order: int) -> dict:
    from app.checklists.normalization import (
        build_folder_key,
        build_normalized_yandex_structure_fields,
        build_not_required_return_fields,
    )

    return {
        "id": item_id,
        "group": group_id,
        "order": order,
        "name": name,
        "priority": "white",
        "status": "",
        "plan": "",
        "fact": "",
        "folderKey": build_folder_key("id", name, item_id),
        "folderPath": "",
        "folderUrl": "",
        **build_normalized_yandex_structure_fields({}),
        **build_not_required_return_fields({}),
        "documents": [],
        "archivedDocumentSeries": [],
        "documentUrl": "",
        "documentName": "",
        "isCustom": True,
        "definitionName": "",
        "definitionGroupId": 0,
        "nameOverride": "",
        "parentItemId": "",
    }


def apply_layout(items: list[dict]) -> list[dict]:
    """The items of a new ИД checklist: the standard survey items give way
    to «Изыскания» with its subitems."""
    from app.checklists.yandex_subfolders import new_subitem_id

    survey_names = {name.casefold() for name in SURVEY_SUBITEM_NAMES}
    kept = [
        item for item in items
        if clean_cell_value(item.get("name")).casefold() not in survey_names
        or bool(item.get("isCustom"))
    ]
    anchor = next(
        (item for item in kept if clean_cell_value(item.get("name")) == SURVEYS_AFTER_ITEM),
        None,
    )
    group_id = int((anchor or {}).get("group") or 1)
    order = int((anchor or {}).get("order") or 0) + 1
    # Items after the anchor make room for «Изыскания».
    for item in kept:
        if int(item.get("group") or 0) == group_id and int(item.get("order") or 0) >= order:
            item["order"] = int(item.get("order") or 0) + 1
    surveys = _blank_item(SURVEYS_ITEM_ID, SURVEYS_ITEM_NAME, group_id, order)
    surveys["systemItem"] = SURVEYS_SYSTEM_ITEM
    surveys["yandexFolderHint"] = {"relativePath": SURVEYS_FOLDER, "name": SURVEYS_ITEM_NAME}
    result = kept + [surveys]
    for index, name in enumerate(SURVEY_SUBITEM_NAMES, start=1):
        child = _blank_item(new_subitem_id("id", group_id), name, group_id, index)
        child["parentItemId"] = SURVEYS_ITEM_ID
        relative = _survey_relative_path(name)
        if relative:
            child["yandexFolderHint"] = {"relativePath": relative, "name": name}
        result.append(child)
    return result


def find_surveys_item(items: list[dict]) -> dict | None:
    """«Изыскания» of a checklist: the locked item of a new checklist, else
    the top-level item of that name (made by hand in older projects)."""
    from app.checklists.subitems import parent_id_of

    locked = next((item for item in items if is_locked_item(item)), None)
    if locked is not None:
        return locked
    return next(
        (
            item for item in items
            if not parent_id_of(item)
            and clean_cell_value(item.get("name")).casefold() == SURVEYS_ITEM_NAME.casefold()
        ),
        None,
    )


def bind_survey_folders(dialog_id: str) -> int:
    """Give the items of a new checklist their folders once the project root
    is known; an item that was renamed, moved or got a folder otherwise is
    left alone. Returns how many items got a folder."""
    from app.checklists.storage import make_storage_dialog_id
    from app.checklists.yandex_folders import get_root_path_from_context, normalize_yandex_disk_path
    from app.checklists.yandex_scope import project_root
    from app.checklists.yandex_structure_state import persist_item_yandex_structure_state
    from app.db import get_conn

    dialog_id = normalize_dialog_id(dialog_id)
    if not dialog_id:
        return 0
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT data_json FROM checklists WHERE dialog_id = ?",
            (make_storage_dialog_id(dialog_id, "id"),),
        ).fetchone()
    finally:
        conn.close()
    if not row:
        return 0
    try:
        data = json.loads(row["data_json"] or "{}")
    except Exception:
        return 0
    if layout_of(data) < ID_LAYOUT_SURVEYS:
        return 0
    pending = [
        item for item in data.get("items") or []
        if isinstance(item, dict)
        and isinstance(item.get("yandexFolderHint"), dict)
        and not clean_cell_value(item.get("yandexFolderPath"))
        and clean_cell_value(item.get("yandexFolderStatus")).lower() not in BUSY_FOLDER_STATUSES
        and not clean_cell_value(item.get("yandexStructureJobId"))
        and clean_cell_value(item.get("name")) == clean_cell_value(item["yandexFolderHint"].get("name"))
    ]
    if not pending:
        return 0
    try:
        # Only a root the project really has: without it the path of the
        # ИД folder is a guess from the project name.
        project_root(dialog_id)
        root = get_root_path_from_context(dialog_id, "id")
    except Exception:
        root = ""
    if not root:
        return 0
    bound = 0
    for item in pending:
        # A subitem moved to another item keeps its own folder rules.
        if clean_cell_value(item.get("parentItemId")) not in {"", SURVEYS_ITEM_ID}:
            continue
        relative = clean_cell_value(item["yandexFolderHint"].get("relativePath")).strip("/")
        path = normalize_yandex_disk_path(f"{root.rstrip('/')}/{relative}")
        if persist_item_yandex_structure_state(
            dialog_id=dialog_id,
            checklist_key="id",
            item_id=clean_cell_value(item.get("id")),
            extra_fields={
                "yandexFolderPath": path,
                "yandexFolderStatus": "ready",
                "yandexFolderError": "",
            },
        ):
            bound += 1
    if bound:
        write_debug_log("id_survey_folders_bound", {"dialogId": dialog_id, "items": bound, "root": root})
    return bound


def bind_survey_folders_quietly(dialog_id: str) -> int:
    try:
        return bind_survey_folders(dialog_id)
    except Exception as exc:
        write_debug_log("id_survey_folders_bind_failed", {"dialogId": dialog_id, "error": str(exc)})
        return 0
