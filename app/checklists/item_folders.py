"""Items that become folders.

Items are two levels deep (see subitems.py), so a move that would nest them
deeper turns the moved ones into plain folders:

* an item (or a subitem) moved inside a subitem becomes a folder of that
  subitem, its own subitems become folders inside it;
* an item with subitems moved inside another item becomes a subitem, its
  subitems become folders of it.

A folder keeps the files, their versions and their history; the status, plan
and fact of the item are gone. Subitems in «Не требуется» stay there.

The change belongs to the edit session (Cancel restores the snapshot). On
Save, a structure job brings Yandex Disk in line ("adopt_item_folder", see
yandex_item_subfolders.py): the folder of the item moves under its new name,
and the history of the files is moved to the new item
(retarget_converted_records_in_transaction).
"""
from __future__ import annotations

import json

from app.db import get_conn
from app.checklists.document_folders import (
    child_folders,
    document_relative_folder,
    folder_key,
    item_subfolders,
    normalize_relative_folder,
    sanitize_folder_segment,
)
from app.checklists.id_surveys import LOCKED_ITEM_MOVE_ERROR, is_locked_item
from app.checklists.subitems import NOT_REQUIRED_STATUS, children_of, parent_id_of
from app.checklists.utils import clean_cell_value, normalize_status


CONVERSION_OPERATION = "checklist_item_to_folder"
# Changes of a converted item that its removal does not break: at Save they
# are skipped as changes of an item absent from the checklist. Files and
# folders changed in the same session would lose their Yandex jobs.
HARMLESS_SESSION_OPERATIONS = frozenset({
    "checklist_item_add",
    "checklist_item_rename",
    "checklist_item_reorder",
    "checklist_item_update",
})
SYNCING_STATUSES = frozenset({"queued", "running"})


class ItemFolderError(ValueError):
    pass


def _name(item: dict) -> str:
    return clean_cell_value(item.get("name")) or "Пункт"


def _join(*parts: str) -> str:
    return "/".join(clean_cell_value(part) for part in parts if clean_cell_value(part))


def is_not_required(item: dict, not_required_group_id: int) -> bool:
    return (
        normalize_status(item.get("status")) == NOT_REQUIRED_STATUS
        or int(item.get("group") or 0) == int(not_required_group_id or 0)
    )


def active_children(items: list[dict], item_id: str) -> list[dict]:
    return [
        child for child in children_of(items, item_id)
        if normalize_status(child.get("status")) != NOT_REQUIRED_STATUS
    ]


def free_folder_name(folders: list[str], parent: str, wanted: str) -> str:
    """``wanted`` or «wanted (2)», … — never an existing folder of ``parent``."""
    # A slash in an item name is a character, not a level of folders.
    raw = clean_cell_value(wanted).replace("/", "_").replace("\\", "_")
    base = sanitize_folder_segment(raw, strict=False) or "Папка"
    taken = {folder_key(folder) for folder in child_folders(folders, parent)}
    candidate = base
    number = 2
    while folder_key(_join(parent, candidate)) in taken:
        candidate = f"{base} ({number})"
        number += 1
    return candidate


def _absorb(host: dict, source: dict, folder: str) -> list[str]:
    """Documents, folders and archived series of ``source`` go to ``host``
    under ``folder``. Returns the moved series ids (for their history)."""
    series_ids: list[str] = []
    documents = list(host.get("documents") or [])
    for document in source.get("documents") or []:
        if not isinstance(document, dict):
            continue
        moved = dict(document)
        moved["relativeFolder"] = _join(folder, document_relative_folder(document))
        documents.append(moved)
        series_ids.append(clean_cell_value(document.get("seriesId") or document.get("id")))
    host["documents"] = documents

    host["subfolders"] = item_subfolders(host) + [folder] + [
        _join(folder, path) for path in item_subfolders(source)
    ]

    archived = list(host.get("archivedDocumentSeries") or [])
    for series in source.get("archivedDocumentSeries") or []:
        if not isinstance(series, dict):
            continue
        current = normalize_relative_folder(series.get("relativeFolder"), strict=False)
        archived.append({**series, "relativeFolder": _join(folder, current)})
        series_ids.append(clean_cell_value(series.get("seriesId")))
    if archived:
        host["archivedDocumentSeries"] = archived
    return [series_id for series_id in series_ids if series_id]


def _physical_folder(dialog_id: str, checklist_key: str, item: dict) -> str:
    """The folder of the item on Yandex Disk as it is now ("" if unknown)."""
    try:
        from app.checklists.folder_page import resolve_item_yandex_folder
        from app.checklists.yandex_scope import canonical_path
        path = clean_cell_value(
            resolve_item_yandex_folder(dialog_id, checklist_key, item).get("path")
        )
        # One spelling ("disk:/…"): paths are compared by their prefix.
        return canonical_path(path) if path else ""
    except Exception:
        return ""


def _inner_move(child_path: str, parent_path: str, target: str) -> dict:
    """Where the folder of a converted subitem is, relative to its parent's
    folder when it lies inside it (the parent may move first)."""
    if child_path and parent_path and child_path.startswith(parent_path + "/"):
        return {"from": child_path[len(parent_path) + 1:], "to": target}
    return {"fromPath": child_path, "to": target}


def check_convertible(
    *,
    session_id: str,
    dialog_id: str,
    checklist_key: str,
    removed: list[dict],
) -> None:
    """Refuse while a converted item has unsaved file changes or its files
    and folder are still on their way to Yandex Disk."""
    from app.checklists.upload_jobs import resolve_document_mirror_status
    from app.checklists.yandex_structure_jobs import (
        get_blocking_yandex_structure_job_for_item,
    )

    by_id = {clean_cell_value(item.get("id")): item for item in removed}
    if session_id and by_id:
        conn = get_conn()
        try:
            marks = ",".join("?" for _ in by_id)
            rows = conn.execute(
                f"""
                SELECT item_id, operation_type
                FROM edit_session_operations
                WHERE session_id = ?
                  AND status = 'applied'
                  AND item_id IN ({marks})
                """,
                (clean_cell_value(session_id), *by_id.keys()),
            ).fetchall()
        finally:
            conn.close()
        for row in rows:
            if clean_cell_value(row["operation_type"]) in HARMLESS_SESSION_OPERATIONS:
                continue
            item = by_id.get(clean_cell_value(row["item_id"])) or {}
            raise ItemFolderError(
                f"В «{_name(item)}» есть несохранённые изменения файлов или папок. "
                "Сохраните чек-лист и повторите перенос."
            )

    for item in removed:
        for document in item.get("documents") or []:
            status = clean_cell_value(
                resolve_document_mirror_status(document).get("status")
            ).lower()
            if status in SYNCING_STATUSES:
                raise ItemFolderError(
                    f"Файлы «{_name(item)}» ещё выгружаются на Яндекс.Диск — "
                    "повторите через минуту"
                )
        job = get_blocking_yandex_structure_job_for_item(
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            item_id=clean_cell_value(item.get("id")),
        )
        if job:
            if clean_cell_value(job.get("status")) in SYNCING_STATUSES:
                raise ItemFolderError(
                    f"Папка «{_name(item)}» ещё синхронизируется с Яндекс.Диском — "
                    "повторите через минуту"
                )
            raise ItemFolderError(
                f"Папка «{_name(item)}» не синхронизирована с Яндекс.Диском. "
                "Сначала повторите синхронизацию кнопкой Яндекс у пункта."
            )


def convert_children_into_folders(
    items: list[dict],
    host: dict,
    *,
    dialog_id: str,
    checklist_key: str,
    session_id: str,
) -> dict:
    """An item with subitems becomes a subitem: its active subitems become
    its folders. ``items`` and ``host`` are changed in place."""
    host_id = clean_cell_value(host.get("id"))
    children = active_children(items, host_id)
    if not children:
        return {}
    check_convertible(
        session_id=session_id,
        dialog_id=dialog_id,
        checklist_key=checklist_key,
        removed=children,
    )
    host_path = _physical_folder(dialog_id, checklist_key, host)
    converted, inner_moves, moved_series = [], [], []
    for child in children:
        folder = free_folder_name(item_subfolders(host), "", _name(child))
        series = _absorb(host, child, folder)
        child_id = clean_cell_value(child.get("id"))
        converted.append({"id": child_id, "name": _name(child), "folder": folder})
        moved_series.append({"itemId": child_id, "seriesIds": series})
        inner_moves.append(
            _inner_move(_physical_folder(dialog_id, checklist_key, child), host_path, folder)
        )
    removed_ids = {item["id"] for item in converted}
    items[:] = [item for item in items if clean_cell_value(item.get("id")) not in removed_ids]
    return {
        "sourcePath": "",
        "targetFolder": "",
        "innerMoves": inner_moves,
        "convertedItems": converted,
        "movedSeries": moved_series,
        "hostItemName": _name(host),
        "kind": "subitems",
    }


def convert_item_into_folder(
    items: list[dict],
    item: dict,
    host: dict,
    *,
    dialog_id: str,
    checklist_key: str,
    session_id: str,
    not_required_group_id: int,
) -> dict:
    """``item`` (with its active subitems) becomes a folder of the subitem
    ``host``. ``items`` and ``host`` are changed in place."""
    item_id = clean_cell_value(item.get("id"))
    host_id = clean_cell_value(host.get("id"))
    if not item_id or item_id == host_id:
        raise ItemFolderError("Нельзя перенести пункт внутрь самого себя")
    if not parent_id_of(host):
        raise ItemFolderError("Папкой пункт становится только внутри подпункта")
    if parent_id_of(host) == item_id:
        raise ItemFolderError("Нельзя перенести пункт внутрь его собственного подпункта")
    if is_not_required(host, not_required_group_id):
        raise ItemFolderError("Подпункт в «Не требуется» не может принять папку")
    if is_not_required(item, not_required_group_id):
        raise ItemFolderError(f"Сначала верните «{_name(item)}» из «Не требуется»")
    if is_locked_item(item):
        raise ItemFolderError(LOCKED_ITEM_MOVE_ERROR)
    if not parent_id_of(item) and not bool(item.get("isCustom", False)):
        # The template brings a standard item back; it is never removed.
        raise ItemFolderError(
            f"Стандартный пункт «{_name(item)}» нельзя сделать папкой — "
            "переведите его в «Не требуется»"
        )

    children = active_children(items, item_id)
    check_convertible(
        session_id=session_id,
        dialog_id=dialog_id,
        checklist_key=checklist_key,
        removed=[item, *children],
    )
    item_path = _physical_folder(dialog_id, checklist_key, item)
    folder = free_folder_name(item_subfolders(host), "", _name(item))
    converted = [{"id": item_id, "name": _name(item), "folder": folder}]
    moved_series = [{"itemId": item_id, "seriesIds": _absorb(host, item, folder)}]
    inner_moves = []
    for child in children:
        sub = free_folder_name(item_subfolders(host), folder, _name(child))
        target = _join(folder, sub)
        child_id = clean_cell_value(child.get("id"))
        moved_series.append({"itemId": child_id, "seriesIds": _absorb(host, child, target)})
        converted.append({"id": child_id, "name": _name(child), "folder": target})
        move = _inner_move(_physical_folder(dialog_id, checklist_key, child), item_path, target)
        if move.get("from"):
            # After the main move the folder lies inside the new one.
            move["from"] = _join(folder, move["from"])
        inner_moves.append(move)
    removed_ids = {entry["id"] for entry in converted}
    items[:] = [entry for entry in items if clean_cell_value(entry.get("id")) not in removed_ids]
    return {
        "sourcePath": item_path,
        "targetFolder": folder,
        "innerMoves": inner_moves,
        "convertedItems": converted,
        "movedSeries": moved_series,
        "hostItemName": _name(host),
        "kind": "item",
    }


def retarget_converted_records_in_transaction(conn, *, session_id: str) -> int:
    """At Save: the history of the moved files belongs to their new item."""
    tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    rows = conn.execute(
        """
        SELECT dialog_id, checklist_key, item_id, payload_json
        FROM edit_session_operations
        WHERE session_id = ?
          AND status = 'applied'
          AND operation_type = ?
        ORDER BY sequence_no ASC
        """,
        (clean_cell_value(session_id), CONVERSION_OPERATION),
    ).fetchall()
    changed = 0
    for row in rows:
        try:
            payload = json.loads(row["payload_json"] or "{}")
        except Exception:
            continue
        host_id = clean_cell_value(row["item_id"])
        for entry in payload.get("movedSeries") or []:
            old_id = clean_cell_value((entry or {}).get("itemId"))
            series_ids = [clean_cell_value(value) for value in (entry or {}).get("seriesIds") or []]
            series_ids = [value for value in series_ids if value]
            if not old_id or not series_ids or old_id == host_id:
                continue
            marks = ",".join("?" for _ in series_ids)
            for table in ("document_assignment_history", "document_replacements"):
                if table not in tables:
                    continue
                cur = conn.execute(
                    f"UPDATE {table} SET item_id = ? "
                    f"WHERE dialog_id = ? AND checklist_key = ? AND item_id = ? "
                    f"AND series_id IN ({marks})",
                    (host_id, row["dialog_id"], row["checklist_key"], old_id, *series_ids),
                )
                changed += int(cur.rowcount or 0)
    return changed
