import json
import sqlite3
from datetime import datetime

from app.db import get_conn, init_db
from app.checklists.utils import (
    clean_cell_value,
    normalize_dialog_id,
    normalize_checklist_key,
)

from app.checklists.normalization import (
    normalize_checklist_data,
    build_default_checklist_template,
)

from app.checklists.yandex_context import hydrate_project_storage_context_from_configs

def make_storage_dialog_id(dialog_id: str, checklist_key: str = "id") -> str:
    dialog_id = normalize_dialog_id(dialog_id)
    checklist_key = normalize_checklist_key(checklist_key)
    return dialog_id if checklist_key == "id" else f"{dialog_id}::{checklist_key}"


def save_checklist(dialog_id: str, data: dict, checklist_key: str = "id"):
    storage_dialog_id = make_storage_dialog_id(dialog_id, checklist_key)
    checklist_key = normalize_checklist_key(checklist_key)
    data = normalize_checklist_data(data, checklist_key)

    conn = get_conn()
    conn.execute("""
        INSERT INTO checklists(dialog_id, title, data_json)
        VALUES (?, ?, ?)
        ON CONFLICT(dialog_id) DO UPDATE SET
            title=excluded.title,
            data_json=excluded.data_json
    """, (
        storage_dialog_id,
        data.get("title", "Чек-лист"),
        json.dumps(data, ensure_ascii=False),
    ))
    conn.commit()
    conn.close()

    return data


def get_checklist(dialog_id: str, checklist_key: str = "id"):
    checklist_key = normalize_checklist_key(checklist_key)
    normalized_id = normalize_dialog_id(dialog_id)

    aliases = []
    for candidate in [dialog_id, normalized_id]:
        candidate = str(candidate or "").strip()
        if candidate and candidate not in aliases:
            aliases.append(candidate)

    if normalized_id.startswith("chat") and normalized_id[4:].isdigit():
        numeric_id = normalized_id[4:]
        if numeric_id not in aliases:
            aliases.append(numeric_id)

    storage_aliases = [make_storage_dialog_id(alias, checklist_key) for alias in aliases]
    storage_dialog_id = make_storage_dialog_id(normalized_id, checklist_key)

    conn = get_conn()

    row = None
    found_alias = None

    for candidate in storage_aliases:
        try:
            row = conn.execute(
                "SELECT data_json FROM checklists WHERE dialog_id = ?",
                (candidate,)
            ).fetchone()
        except sqlite3.OperationalError as e:
            if "no such table" in str(e).lower():
                conn.close()
                init_db()
                conn = get_conn()
                row = conn.execute(
                    "SELECT data_json FROM checklists WHERE dialog_id = ?",
                    (candidate,)
                ).fetchone()
            else:
                conn.close()
                raise

        if row:
            found_alias = candidate
            break

    conn.close()

    if row:
        data = json.loads(row["data_json"])
        data = normalize_checklist_data(data, checklist_key)

        data["resolvedDialogId"] = normalized_id
        data["lookupAliases"] = aliases
        data["foundAlias"] = found_alias
        return data

    data = build_default_checklist_template(normalized_id, checklist_key)

    save_checklist(
        normalized_id,
        data,
        checklist_key,
    )

    data["resolvedDialogId"] = normalized_id
    data["lookupAliases"] = aliases
    data["foundAlias"] = storage_dialog_id
    return data


def save_project_storage_context(dialog_id: str, payload: dict):
    dialog_id = normalize_dialog_id(dialog_id)
    if not dialog_id:
        raise ValueError("dialogId is required")

    project_id = str(payload.get("projectId") or "").strip()
    project_name = clean_cell_value(payload.get("projectName"))
    storage_mode = payload.get("storageMode") or {}
    yandex_disk = payload.get("yandexDisk") or {}
    item_mappings = payload.get("itemMappings") or []

    conn = get_conn()
    conn.execute("""
        INSERT INTO project_storage_contexts(
            dialog_id,
            project_id,
            project_name,
            provider,
            storage_mode_json,
            yandex_json,
            item_mappings_json,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(dialog_id) DO UPDATE SET
            project_id=excluded.project_id,
            project_name=excluded.project_name,
            provider=excluded.provider,
            storage_mode_json=excluded.storage_mode_json,
            yandex_json=excluded.yandex_json,
            item_mappings_json=excluded.item_mappings_json,
            updated_at=excluded.updated_at
    """, (
        dialog_id,
        project_id,
        project_name,
        str(yandex_disk.get("provider") or "yandex_disk").strip(),
        json.dumps(storage_mode, ensure_ascii=False),
        json.dumps(yandex_disk, ensure_ascii=False),
        json.dumps(item_mappings, ensure_ascii=False),
        datetime.now().isoformat(),
    ))
    conn.commit()
    conn.close()


def get_project_storage_context(dialog_id: str):
    dialog_id = normalize_dialog_id(dialog_id)
    if not dialog_id:
        return None

    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM project_storage_contexts WHERE dialog_id = ?",
        (dialog_id,)
    ).fetchone()
    conn.close()

    if not row:
        return None

    context = {
        "dialogId": row["dialog_id"],
        "projectId": row["project_id"],
        "projectName": row["project_name"],
        "provider": row["provider"],
        "storageMode": json.loads(row["storage_mode_json"] or "{}"),
        "yandexDisk": json.loads(row["yandex_json"] or "{}"),
        "itemMappings": json.loads(row["item_mappings_json"] or "[]"),
        "updatedAt": row["updated_at"],
    }

    return hydrate_project_storage_context_from_configs(context)


def normalize_mapping_group_id(mapping: dict) -> int:
    try:
        return int((mapping or {}).get("groupId") or 0)
    except (TypeError, ValueError):
        return 0


def get_item_yandex_mapping(
    dialog_id: str,
    checklist_key: str,
    item_name: str,
    group_id: int = 0,
):
    context = get_project_storage_context(dialog_id)
    if not context:
        return None

    checklist_key = normalize_checklist_key(checklist_key)
    item_name = clean_cell_value(item_name).lower()

    try:
        target_group_id = int(group_id or 0)
    except (TypeError, ValueError):
        target_group_id = 0

    fallback_mapping = None
    first_name_match = None

    for mapping in context.get("itemMappings", []):
        if not isinstance(mapping, dict):
            continue

        mapping_key = normalize_checklist_key(mapping.get("checklistKey"))
        mapping_name = clean_cell_value(mapping.get("itemName")).lower()

        if mapping_key != checklist_key or mapping_name != item_name:
            continue

        mapping_group_id = normalize_mapping_group_id(mapping)

        if first_name_match is None:
            first_name_match = mapping

        if target_group_id and mapping_group_id == target_group_id:
            return mapping

        if mapping_group_id == 0 and fallback_mapping is None:
            fallback_mapping = mapping

    return fallback_mapping or first_name_match


def get_item_yandex_folder(
    dialog_id: str,
    checklist_key: str,
    item_name: str,
    group_id: int = 0,
):
    context = get_project_storage_context(dialog_id)
    if not context:
        return None

    mapping = get_item_yandex_mapping(
        dialog_id,
        checklist_key,
        item_name,
        group_id=group_id,
    )
    if not mapping:
        return None

    folder_alias = str(mapping.get("folderAlias") or "").strip()
    if not folder_alias:
        return None

    yandex_disk = context.get("yandexDisk") or {}
    folders = yandex_disk.get("folders") or {}
    folder = folders.get(folder_alias)

    if not folder:
        return None

    return {
        "folderAlias": folder_alias,
        "folder": folder,
        "mapping": mapping,
        "context": context,
    }


def list_checklist_summaries() -> list[dict]:
    conn = get_conn()

    try:
        rows = conn.execute(
            "SELECT dialog_id, title FROM checklists ORDER BY dialog_id"
        ).fetchall()

        return [
            {
                "dialog_id": row["dialog_id"],
                "title": row["title"],
            }
            for row in rows
        ]

    except sqlite3.OperationalError as e:
        if "no such table" in str(e).lower():
            return []
        raise

    finally:
        conn.close()

def get_project_root_yandex_folder_info(dialog_id: str) -> dict:
    context = get_project_storage_context(dialog_id)
    if not context:
        return {
            "path": "",
            "url": "",
        }

    yandex_disk = context.get("yandexDisk") or {}

    return {
        "path": clean_cell_value(yandex_disk.get("projectRootPath")),
        "url": clean_cell_value(yandex_disk.get("projectRootUrl")),
        "standardFoldersPrepared": bool(yandex_disk.get("standardFoldersPrepared")),
        "standardFoldersPreparedAt": clean_cell_value(yandex_disk.get("standardFoldersPreparedAt")),
        "standardFoldersPreparedCount": int(yandex_disk.get("standardFoldersPreparedCount") or 0),
    }

def list_project_storage_context_dialog_ids() -> list[str]:
    conn = get_conn()

    try:
        rows = conn.execute("""
            SELECT dialog_id
            FROM project_storage_contexts
            ORDER BY updated_at DESC, dialog_id
        """).fetchall()

        result = []

        for row in rows:
            dialog_id = normalize_dialog_id(row["dialog_id"])
            if dialog_id:
                result.append(dialog_id)

        return result

    except sqlite3.OperationalError as e:
        if "no such table" in str(e).lower():
            return []
        raise

    finally:
        conn.close()