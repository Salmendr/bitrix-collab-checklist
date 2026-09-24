"""Yandex Disk folders of subitems.

A subitem folder lives inside the folder of its parent item:
``.../02_Стадия П/04_КР/01_Подпункт``. The parent folder can be moved or
renamed later, therefore the target of a subitem job is resolved by the
structure worker at execution time, and after a parent relocation the
stored paths of its subitems are rebased.
"""
from __future__ import annotations

import uuid

from app.logging_utils import write_debug_log
from app.checklists.storage import get_checklist
from app.checklists.subitems import children_of, parent_id_of
from app.checklists.utils import (
    clean_cell_value,
    normalize_checklist_key,
    normalize_dialog_id,
    slugify_folder_part,
)


def find_item(dialog_id: str, checklist_key: str, item_id: str) -> dict:
    target = clean_cell_value(item_id)
    if not target:
        return {}
    data = get_checklist(dialog_id, checklist_key)
    return next(
        (
            dict(item or {})
            for item in (data.get("items") or [])
            if clean_cell_value((item or {}).get("id")) == target
        ),
        {},
    )


def known_parent_folder_path(parent: dict | None) -> str:
    """Best known path of a parent folder without touching Yandex Disk."""
    source = parent or {}
    return clean_cell_value(
        source.get("yandexFolderPath")
        or source.get("yandexFolderTargetPath")
    )


def build_subitem_target_path(parent_path: str, item_name: str) -> str:
    from app.checklists.yandex_folders import (
        normalize_yandex_disk_path,
        sanitize_yandex_folder_name,
    )
    parent = clean_cell_value(parent_path).rstrip("/")
    if not parent:
        return ""
    # The numeric prefix is allocated by the worker (custom folder rules).
    return normalize_yandex_disk_path(
        f"{parent}/{sanitize_yandex_folder_name(item_name)}"
    )


def ensure_parent_folder_path(
    dialog_id: str,
    checklist_key: str,
    parent: dict,
) -> str:
    """Return the existing parent folder, creating a standard one if needed.

    Runs inside the structure worker (project lock held).
    """
    from app.checklists.yandex_folders import (
        ensure_folder_and_get_public_url,
        ensure_item_yandex_folder_for_upload,
    )
    from app.checklists.yandex_structure_jobs import (
        get_latest_completed_yandex_structure_folder_path,
    )
    from app.yandex_disk.client import yandex_disk_try_get_resource_meta

    def exists(path: str) -> bool:
        if not path:
            return False
        try:
            return bool(yandex_disk_try_get_resource_meta(path))
        except Exception:
            return False

    parent_id = clean_cell_value(parent.get("id"))
    candidates = [
        clean_cell_value(parent.get("yandexFolderPath")),
        get_latest_completed_yandex_structure_folder_path(
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            item_id=parent_id,
        ),
    ]
    for candidate in candidates:
        if exists(candidate):
            return candidate

    folder_info = ensure_item_yandex_folder_for_upload(
        dialog_id=dialog_id,
        checklist_key=checklist_key,
        item_name=clean_cell_value(parent.get("name")),
        item_id=parent_id,
        item_group=int(parent.get("group") or 0),
        is_custom=bool(parent.get("isCustom", False)),
    ) or {}
    path = clean_cell_value((folder_info.get("folder") or {}).get("path"))
    if path:
        ensure_folder_and_get_public_url(path)
        return path
    raise RuntimeError(
        "Папка пункта на Яндекс.Диске ещё не создана — "
        "подпапка подпункта будет создана после неё"
    )


def retarget_subitem_job(job: dict, item: dict) -> dict:
    """Point a create/move job of a subitem into its parent's current folder."""
    from app.checklists.yandex_structure_jobs import (
        update_yandex_structure_job_target,
    )

    action = clean_cell_value(job.get("action"))
    if action not in {"create_item_folder", "move_item_folder"}:
        return job
    parent_id = parent_id_of(item)
    if not parent_id:
        return job
    dialog_id = normalize_dialog_id(job.get("dialog_id"))
    checklist_key = normalize_checklist_key(job.get("checklist_key"))
    parent = find_item(dialog_id, checklist_key, parent_id)
    if not parent:
        raise RuntimeError("Пункт, к которому относится подпункт, не найден")

    parent_path = ensure_parent_folder_path(dialog_id, checklist_key, parent)
    current_target = clean_cell_value(job.get("target_path"))
    current_parent = current_target.rsplit("/", 1)[0] if "/" in current_target else ""
    if current_parent == parent_path.rstrip("/"):
        return job
    name = clean_cell_value(job.get("item_name")) or clean_cell_value(item.get("name"))
    target = build_subitem_target_path(parent_path, name)
    source_path = clean_cell_value(job.get("source_path"))
    if (
        action == "move_item_folder"
        and "/" in source_path
        and source_path.rsplit("/", 1)[0] == parent_path.rstrip("/")
    ):
        # Already inside this parent folder: keep its numeric prefix.
        from app.checklists.yandex_folders import (
            _preserve_standard_folder_prefix,
        )
        target = source_path.rsplit("/", 1)[0] + "/" + _preserve_standard_folder_prefix(
            source_path.rsplit("/", 1)[1],
            name,
        )
    updated = update_yandex_structure_job_target(
        clean_cell_value(job.get("job_id")),
        target,
    )
    write_debug_log("yandex_subitem_job_retargeted", {
        "jobId": clean_cell_value(job.get("job_id")),
        "action": action,
        "itemId": clean_cell_value(item.get("id")),
        "parentItemId": parent_id,
        "previousTargetPath": current_target,
        "targetPath": target,
    })
    return updated or {**job, "target_path": target}


def rebase_subitem_folders_after_parent_job(job: dict, result: dict) -> int:
    """After a parent folder moved/renamed, its subfolders moved with it.

    Record that for every subitem: a completed structure job (the durable
    source of the item's folder state) and rebased file addresses.
    """
    from app.checklists.yandex_folders import (
        normalize_yandex_disk_path,
        upsert_item_yandex_mapping,
    )
    from app.checklists.yandex_structure_jobs import (
        create_yandex_structure_job,
    )
    from app.checklists.yandex_structure_state import (
        persist_item_yandex_structure_state,
    )

    action = clean_cell_value(job.get("action"))
    if action not in {"move_item_folder", "rename_item_folder"}:
        return 0
    old_parent = clean_cell_value(
        (result or {}).get("sourcePath") or job.get("source_path")
    ).rstrip("/")
    new_parent = clean_cell_value(
        (result or {}).get("folderPath") or job.get("target_path")
    ).rstrip("/")
    if not old_parent or not new_parent or old_parent == new_parent:
        return 0
    old_parent = normalize_yandex_disk_path(old_parent)
    new_parent = normalize_yandex_disk_path(new_parent)

    dialog_id = normalize_dialog_id(job.get("dialog_id"))
    checklist_key = normalize_checklist_key(job.get("checklist_key"))
    parent_id = clean_cell_value(job.get("item_id"))
    data = get_checklist(dialog_id, checklist_key)
    items = data.get("items") or []
    rebased = 0
    for child in children_of(items, parent_id):
        child_id = clean_cell_value(child.get("id"))
        child_path = normalize_yandex_disk_path(
            clean_cell_value(child.get("yandexFolderPath"))
        ) if clean_cell_value(child.get("yandexFolderPath")) else ""
        if not child_path.startswith(old_parent + "/"):
            continue
        new_child_path = new_parent + child_path[len(old_parent):]
        folder_url = clean_cell_value(child.get("yandexFolderUrl"))
        folder_alias = clean_cell_value(child.get("yandexFolderAlias"))
        rebase_job = create_yandex_structure_job(
            idempotency_key=(
                f"parent-rebase:{clean_cell_value(job.get('job_id'))}:"
                f"{child_id}"
            ),
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            item_id=child_id,
            action="move_item_folder",
            source_path=child_path,
            target_path=new_child_path,
            folder_alias=folder_alias,
            item_name=clean_cell_value(child.get("name")),
            group_id=int(child.get("group") or 0),
            initial_status="completed",
            result={
                "ok": True,
                "sourcePath": child_path,
                "folderPath": new_child_path,
                "folderUrl": folder_url,
                "folderName": new_child_path.rsplit("/", 1)[-1],
                "rebasedByParentJobId": clean_cell_value(job.get("job_id")),
            },
        )
        persist_item_yandex_structure_state(
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            item_id=child_id,
            job=rebase_job,
        )
        alias = folder_alias or (
            f"{checklist_key}_{slugify_folder_part(child_id)}"
        )
        try:
            upsert_item_yandex_mapping(
                dialog_id=dialog_id,
                checklist_key=checklist_key,
                item_name=clean_cell_value(child.get("name")),
                folder_alias=alias,
                folder_name=new_child_path.rsplit("/", 1)[-1],
                folder_path=new_child_path,
                folder_url=folder_url,
                group_id=int(child.get("group") or 0),
            )
        except Exception:
            pass
        rebased += 1
    if rebased:
        write_debug_log("yandex_subitem_folders_rebased", {
            "parentItemId": parent_id,
            "jobId": clean_cell_value(job.get("job_id")),
            "oldParentPath": old_parent,
            "newParentPath": new_parent,
            "count": rebased,
        })
    return rebased


def new_subitem_id(checklist_key: str, group_id: int) -> str:
    return (
        f"{normalize_checklist_key(checklist_key)}_g{int(group_id or 0)}"
        f"_sub_{uuid.uuid4().hex[:8]}"
    )


def guess_parent_folder_path(
    dialog_id: str,
    checklist_key: str,
    parent: dict | None,
) -> str:
    """Parent folder path for planning a job, without calling Yandex Disk.

    The structure worker re-resolves the real folder before executing, so an
    approximate path is enough here; it only must be non-empty.
    """
    from app.checklists.yandex_folders import (
        normalize_yandex_disk_path,
        resolve_item_group_parent_yandex_path,
        sanitize_yandex_folder_name,
    )
    from app.checklists.yandex_scope import item_folder

    source = parent or {}
    known = known_parent_folder_path(source)
    if known:
        return normalize_yandex_disk_path(known)
    try:
        resolved = item_folder(dialog_id, checklist_key, source)
        if resolved:
            return resolved
    except Exception:
        pass
    try:
        group_path = resolve_item_group_parent_yandex_path(
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            group_id=int(source.get("group") or 0),
        )
    except Exception:
        group_path = ""
    if not group_path:
        return ""
    return normalize_yandex_disk_path(
        f"{group_path.rstrip('/')}/"
        f"{sanitize_yandex_folder_name(clean_cell_value(source.get('name')))}"
    )
