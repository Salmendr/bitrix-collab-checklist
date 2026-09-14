from __future__ import annotations

import hashlib

from app.checklists.storage import (
    get_checklist,
    get_project_storage_context,
)
from app.checklists.utils import (
    clean_cell_value,
    normalize_checklist_key,
    normalize_dialog_id,
    slugify_folder_part,
)
from app.checklists.yandex_folders import (
    build_stable_custom_folder_target_path,
    can_create_custom_item_yandex_folder,
    custom_folder_base_name,
    resolve_custom_item_parent_yandex_path,
    sanitize_yandex_folder_name,
    split_custom_folder_prefix,
    upsert_item_yandex_mapping,
)
from app.checklists.yandex_resource_locks import (
    yandex_project_resource_guard,
)
from app.checklists.yandex_structure_jobs import (
    create_yandex_structure_job,
    get_latest_yandex_structure_job_for_item,
)
from app.checklists.yandex_structure_state import (
    persist_item_yandex_structure_state,
)
from app.yandex_disk.client import (
    normalize_yandex_disk_path,
    yandex_disk_client_url,
    yandex_disk_get_resource_meta,
    yandex_disk_list_folder_children,
    yandex_disk_publish_path,
    yandex_disk_try_get_resource_meta,
)


CUSTOM_FOLDER_CONFLICT_ERROR = (
    "Конфликт папок Яндекс.Диска — требуется решение пользователя."
)


def _path_key(value: str) -> str:
    return normalize_yandex_disk_path(value).rstrip("/").casefold()


def _path_parent(value: str) -> str:
    normalized = normalize_yandex_disk_path(value).rstrip("/")
    return normalized.rsplit("/", 1)[0] if "/" in normalized else ""


def _current_item(
    dialog_id: str,
    checklist_key: str,
    item_id: str,
) -> dict:
    data = get_checklist(dialog_id, checklist_key)
    normalized_item_id = clean_cell_value(item_id)
    return next(
        (
            dict(item or {})
            for item in (data.get("items") or [])
            if clean_cell_value((item or {}).get("id")) == normalized_item_id
        ),
        {},
    )


def _recovery_identity(
    *,
    dialog_id: str,
    checklist_key: str,
    item_id: str,
    state: str,
    paths: list[str],
) -> str:
    signature = "\n".join(sorted(_path_key(path) for path in paths if path))
    digest = hashlib.sha256(signature.encode("utf-8")).hexdigest()[:16]
    return (
        f"custom-folder-recovery:v2:{dialog_id}:{checklist_key}:"
        f"{item_id}:{state}:{digest}"
    )


def _candidate_payload(raw: dict) -> dict:
    path = normalize_yandex_disk_path(raw.get("path") or "")
    name = clean_cell_value(raw.get("name")) or path.rstrip("/").rsplit("/", 1)[-1]
    public_url = clean_cell_value(
        raw.get("public_url") or raw.get("publicUrl")
    )
    return {
        "name": name,
        "path": path,
        "url": public_url or yandex_disk_client_url(path),
        "publicUrl": public_url,
        "clientUrl": yandex_disk_client_url(path),
        "folderAlias": clean_cell_value(raw.get("folderAlias")),
        "groupId": int(raw.get("groupId") or 0),
    }


def _collect_custom_candidates(
    *,
    dialog_id: str,
    checklist_key: str,
    item: dict,
    expected_parent: str,
    folder_alias: str,
) -> list[dict]:
    context = get_project_storage_context(dialog_id) or {}
    yandex = context.get("yandexDisk") or {}
    folders = yandex.get("folders") or {}
    mappings = context.get("itemMappings") or []
    current_name = clean_cell_value(item.get("name"))
    current_group = int(item.get("group") or 0)
    return_group = int(item.get("notRequiredReturnGroupId") or 0)

    aliases = {clean_cell_value(folder_alias)}
    names = {sanitize_yandex_folder_name(current_name).casefold()}
    exact_paths = {
        clean_cell_value(item.get("yandexFolderPath")),
        clean_cell_value(item.get("yandexFolderTargetPath")),
    }
    parents = {normalize_yandex_disk_path(expected_parent)}
    path_metadata: dict[str, dict] = {}
    explicit_identity = bool(
        clean_cell_value(item.get("yandexFolderPath"))
    ) or any(
        isinstance(mapping, dict)
        and normalize_checklist_key(mapping.get("checklistKey")) == checklist_key
        and clean_cell_value(mapping.get("folderAlias")) in aliases
        for mapping in mappings
    )

    for mapping in mappings:
        if not isinstance(mapping, dict):
            continue
        if normalize_checklist_key(mapping.get("checklistKey")) != checklist_key:
            continue
        mapping_alias = clean_cell_value(mapping.get("folderAlias"))
        mapping_name = clean_cell_value(mapping.get("itemName"))
        mapping_group = int(mapping.get("groupId") or 0)
        relevant = (
            mapping_alias in aliases
            or (
                not explicit_identity
                and
                mapping_name.casefold() == current_name.casefold()
                and mapping_group in {current_group, return_group}
            )
        )
        if not relevant:
            continue
        if mapping_alias:
            aliases.add(mapping_alias)
        if mapping_name:
            names.add(sanitize_yandex_folder_name(mapping_name).casefold())
        if mapping_group:
            parents.add(resolve_custom_item_parent_yandex_path(
                dialog_id,
                checklist_key,
                mapping_group,
            ))

    for alias, raw_folder in (folders.items() if isinstance(folders, dict) else []):
        folder = raw_folder if isinstance(raw_folder, dict) else {}
        folder_item_name = clean_cell_value(folder.get("itemName"))
        folder_key = normalize_checklist_key(folder.get("checklistKey"))
        relevant = (
            clean_cell_value(alias) in aliases
            or (
                not explicit_identity
                and
                folder_key == checklist_key
                and folder_item_name.casefold() == current_name.casefold()
            )
        )
        if not relevant:
            continue
        aliases.add(clean_cell_value(alias))
        if folder_item_name:
            names.add(sanitize_yandex_folder_name(folder_item_name).casefold())
        folder_path = clean_cell_value(folder.get("path"))
        if folder_path:
            exact_paths.add(folder_path)
            path_metadata[_path_key(folder_path)] = {
                "folderAlias": clean_cell_value(alias),
                "groupId": int(folder.get("groupId") or 0),
            }

    for path in list(exact_paths):
        if not path:
            continue
        parents.add(_path_parent(path))
        names.add(
            sanitize_yandex_folder_name(
                custom_folder_base_name(path.rstrip("/").rsplit("/", 1)[-1])
            ).casefold()
        )

    if return_group:
        parents.add(resolve_custom_item_parent_yandex_path(
            dialog_id,
            checklist_key,
            return_group,
        ))

    candidates: dict[str, dict] = {}
    for path in exact_paths:
        if not path:
            continue
        meta = yandex_disk_try_get_resource_meta(path)
        if meta and clean_cell_value(meta.get("type")).lower() in {"", "dir"}:
            payload = _candidate_payload({
                **meta,
                **path_metadata.get(_path_key(path), {}),
            })
            candidates[_path_key(payload["path"])] = payload

    for parent in sorted(
        {normalize_yandex_disk_path(value) for value in parents if value},
        key=str.casefold,
    ):
        parent_meta = yandex_disk_try_get_resource_meta(parent)
        if not parent_meta:
            continue
        for child in yandex_disk_list_folder_children(parent):
            if clean_cell_value(child.get("type")).lower() != "dir":
                continue
            base = sanitize_yandex_folder_name(
                custom_folder_base_name(child.get("name") or "")
            ).casefold()
            if base not in names:
                continue
            payload = _candidate_payload({
                **child,
                **path_metadata.get(_path_key(child.get("path") or ""), {}),
            })
            candidates[_path_key(payload["path"])] = payload

    return sorted(candidates.values(), key=lambda value: _path_key(value["path"]))


def _has_ambiguous_local_identity(
    *,
    dialog_id: str,
    checklist_key: str,
    item: dict,
    folder_alias: str,
) -> bool:
    if clean_cell_value(item.get("yandexFolderPath")):
        return False
    context = get_project_storage_context(dialog_id) or {}
    folders = ((context.get("yandexDisk") or {}).get("folders") or {})
    for mapping in context.get("itemMappings") or []:
        if not isinstance(mapping, dict):
            continue
        alias = clean_cell_value(mapping.get("folderAlias"))
        if alias != clean_cell_value(folder_alias):
            continue
        folder = folders.get(alias) if isinstance(folders, dict) else None
        if isinstance(folder, dict) and clean_cell_value(folder.get("path")):
            return False

    data = get_checklist(dialog_id, checklist_key)
    item_name = clean_cell_value(item.get("name")).casefold()
    group_id = int(item.get("group") or 0)
    matches = [
        candidate
        for candidate in (data.get("items") or [])
        if bool((candidate or {}).get("isCustom", False))
        and int((candidate or {}).get("group") or 0) == group_id
        and clean_cell_value((candidate or {}).get("name")).casefold() == item_name
    ]
    return len(matches) > 1


def reconcile_custom_item_yandex_folder(
    *,
    dialog_id: str,
    checklist_key: str,
    item_id: str,
    item: dict | None = None,
    source: str = "startup",
    enqueue: bool = True,
) -> dict:
    dialog_id = normalize_dialog_id(dialog_id)
    checklist_key = normalize_checklist_key(checklist_key)
    item_id = clean_cell_value(item_id)
    item = dict(item or _current_item(dialog_id, checklist_key, item_id))

    if not item or not bool(item.get("isCustom", False)):
        return {"ok": True, "skipped": True, "reason": "not_custom_item"}
    if not can_create_custom_item_yandex_folder(dialog_id, checklist_key):
        return {
            "ok": True,
            "skipped": True,
            "reason": "custom_item_yandex_folder_disabled",
        }

    item_name = clean_cell_value(item.get("name"))
    group_id = int(item.get("group") or 0)
    expected_parent = resolve_custom_item_parent_yandex_path(
        dialog_id,
        checklist_key,
        group_id,
    )
    if not expected_parent:
        return {"ok": False, "error": "Yandex custom folder parent is unavailable"}
    folder_alias = (
        clean_cell_value(item.get("yandexFolderAlias"))
        or f"{checklist_key}_{slugify_folder_part(item_id or item_name)}"
    )

    latest = get_latest_yandex_structure_job_for_item(
        dialog_id=dialog_id,
        checklist_key=checklist_key,
        item_id=item_id,
    ) or {}
    latest_status = clean_cell_value(latest.get("status")).lower()
    if latest_status in {"queued", "running"}:
        enqueue_result = {}
        if enqueue and latest_status == "queued":
            from app.checklists.yandex_structure_queue import enqueue_yandex_structure_job
            enqueue_result = enqueue_yandex_structure_job(
                latest.get("job_id") or "",
                source="custom_folder_recovery_existing",
            )
        return {
            "ok": True,
            "queued": True,
            "existing": True,
            "job": latest,
            "enqueue": enqueue_result,
        }
    with yandex_project_resource_guard(
        dialog_id,
        checklist_key=checklist_key,
        item_id=item_id,
        operation="custom_folder_recovery_scan",
    ):
        candidates = _collect_custom_candidates(
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            item=item,
            expected_parent=expected_parent,
            folder_alias=folder_alias,
        )
        ambiguous_identity = _has_ambiguous_local_identity(
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            item=item,
            folder_alias=folder_alias,
        )
        expected_parent_key = _path_key(expected_parent)
        expected_base = sanitize_yandex_folder_name(item_name).casefold()

        correct = []
        for candidate in candidates:
            prefix, base = split_custom_folder_prefix(candidate.get("name") or "")
            if (
                _path_key(_path_parent(candidate.get("path") or ""))
                == expected_parent_key
                and prefix > 0
                and sanitize_yandex_folder_name(base).casefold() == expected_base
            ):
                correct.append(candidate)

        # More than one matching legacy/current folder is always a manual
        # conflict. Nothing is moved, merged, overwritten or deleted.
        if len(candidates) >= 2 or (ambiguous_identity and candidates):
            job = create_yandex_structure_job(
                idempotency_key=_recovery_identity(
                    dialog_id=dialog_id,
                    checklist_key=checklist_key,
                    item_id=item_id,
                    state="conflict",
                    paths=[candidate["path"] for candidate in candidates],
                ),
                dialog_id=dialog_id,
                checklist_key=checklist_key,
                item_id=item_id,
                action="move_item_folder",
                source_path=candidates[0]["path"],
                target_path=normalize_yandex_disk_path(
                    f"{expected_parent.rstrip('/')}/{sanitize_yandex_folder_name(item_name)}"
                ),
                folder_alias=folder_alias,
                item_name=item_name,
                group_id=group_id,
                initial_status="conflict",
                error=CUSTOM_FOLDER_CONFLICT_ERROR,
                result={
                    "isCustom": True,
                    "recoverySource": source,
                    "conflictCandidates": candidates,
                    "expectedParentPath": expected_parent,
                    "identityAmbiguity": bool(ambiguous_identity),
                },
            )
            persist_item_yandex_structure_state(
                dialog_id=dialog_id,
                checklist_key=checklist_key,
                item_id=item_id,
                job=job,
            )
            return {"ok": False, "conflict": True, "job": job, "candidates": candidates}

        if len(candidates) == 1 and correct:
            candidate = correct[0]
            yandex_disk_publish_path(candidate["path"])
            meta = yandex_disk_get_resource_meta(candidate["path"])
            final_candidate = _candidate_payload({
                **meta,
                "folderAlias": candidate.get("folderAlias") or folder_alias,
                "groupId": group_id,
            })
            resolved_alias = clean_cell_value(
                final_candidate.get("folderAlias")
            ) or folder_alias
            upsert_item_yandex_mapping(
                dialog_id=dialog_id,
                checklist_key=checklist_key,
                item_name=item_name,
                folder_alias=resolved_alias,
                folder_name=final_candidate["name"],
                folder_path=final_candidate["path"],
                folder_url=final_candidate.get("publicUrl") or final_candidate["url"],
                group_id=group_id,
            )
            result = {
                "ok": True,
                "isCustom": True,
                "recoveredExisting": True,
                "recoverySource": source,
                "folderAlias": resolved_alias,
                "folderName": final_candidate["name"],
                "folderPath": final_candidate["path"],
                "folderUrl": final_candidate.get("publicUrl") or final_candidate["url"],
            }
            job = create_yandex_structure_job(
                idempotency_key=_recovery_identity(
                    dialog_id=dialog_id,
                    checklist_key=checklist_key,
                    item_id=item_id,
                    state="resolved",
                    paths=[final_candidate["path"]],
                ),
                dialog_id=dialog_id,
                checklist_key=checklist_key,
                item_id=item_id,
                action="create_item_folder",
                target_path=final_candidate["path"],
                folder_alias=resolved_alias,
                item_name=item_name,
                group_id=group_id,
                initial_status="completed",
                result=result,
            )
            persist_item_yandex_structure_state(
                dialog_id=dialog_id,
                checklist_key=checklist_key,
                item_id=item_id,
                job=job,
            )
            return {
                "ok": True,
                "completed": True,
                "job": job,
                "result": result,
                "candidates": candidates,
            }

        source_candidate = candidates[0] if candidates else {}
        source_path = source_candidate.get("path") or ""
        source_parent = _path_parent(source_path) if source_path else ""
        source_name = source_path.rstrip("/").rsplit("/", 1)[-1] if source_path else ""
        preserve_name = source_name if _path_key(source_parent) == expected_parent_key else ""
        target_path = build_stable_custom_folder_target_path(
            parent_path=expected_parent,
            item_name=item_name,
            preserve_source_name=preserve_name,
        )
        action = (
            "create_item_folder"
            if not source_path
            else "rename_item_folder"
            if _path_key(source_parent) == expected_parent_key
            else "move_item_folder"
        )
        operation_alias = clean_cell_value(
            source_candidate.get("folderAlias")
        ) or folder_alias
        source_group_id = int(
            source_candidate.get("groupId")
            or item.get("notRequiredReturnGroupId")
            or 0
        )
        job = create_yandex_structure_job(
            idempotency_key=_recovery_identity(
                dialog_id=dialog_id,
                checklist_key=checklist_key,
                item_id=item_id,
                state=action,
                paths=[source_path, target_path],
            ),
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            item_id=item_id,
            action=action,
            source_path=source_path,
            target_path=target_path,
            folder_alias=operation_alias,
            item_name=item_name,
            group_id=group_id,
            initial_status="queued",
            result={
                "isCustom": True,
                "startupRepair": True,
                "recoverySource": source,
                "sourceGroupId": source_group_id,
                "targetGroupId": group_id,
                "oldName": custom_folder_base_name(source_name) or item_name,
                "finalName": item_name,
                "candidateCount": len(candidates),
            },
        )
        persist_item_yandex_structure_state(
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            item_id=item_id,
            job=job,
        )

    enqueue_result = {}
    if enqueue and clean_cell_value(job.get("status")) == "queued":
        from app.checklists.yandex_structure_queue import enqueue_yandex_structure_job
        enqueue_result = enqueue_yandex_structure_job(
            job.get("job_id") or "",
            source="custom_folder_recovery",
        )
    return {
        "ok": True,
        "queued": clean_cell_value(job.get("status")) == "queued",
        "job": job,
        "enqueue": enqueue_result,
        "candidateCount": len(candidates),
        "candidates": candidates,
    }
