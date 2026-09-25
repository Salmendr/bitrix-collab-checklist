"""Folders inside a subitem: create, rename, move, delete, download as ZIP.

Every mutation belongs to the active edit session (Cancel restores the
snapshot). Yandex Disk follows when the session is committed, through
structure jobs (see yandex_item_subfolders.py). Local files never move:
the folder of a document is metadata (``relativeFolder``).
"""
from __future__ import annotations

import copy
import tempfile
import zipfile
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import FileResponse, JSONResponse
from starlette.background import BackgroundTask

from app.logging_utils import write_debug_log
from app.checklists.checklist_mutation_guard import checklist_mutation_guard
from app.checklists.checklist_routes import (
    begin_optional_edit_session_change,
    checklist_edit_session_error_response,
    record_or_restore_checklist_change,
)
from app.checklists.config import get_checklist_config
from app.checklists.document_folders import (
    FolderPathError,
    canonical_folder,
    child_folders,
    document_relative_folder,
    documents_below,
    find_folder,
    folder_key,
    folder_name,
    is_within,
    item_allows_plain_folders,
    item_subfolders,
    normalize_relative_folder,
    parent_folder,
    rebase_folder,
    sanitize_folder_segment,
)
from app.checklists.documents import (
    get_upload_file_path_from_url,
    normalize_documents_list,
)
from app.checklists.edit_session_changes import ensure_checklist_snapshot
from app.checklists.normalization import normalize_checklist_data
from app.checklists.permissions import can_user_delete_files
from app.checklists.storage import get_checklist, save_checklist
from app.checklists.subitems import children_of, parent_id_of
from app.checklists.utils import (
    clean_cell_value,
    normalize_checklist_key,
    normalize_dialog_id,
)


router = APIRouter()

SYNCING_STATUSES = {"queued", "running"}


class FolderOperationError(ValueError):
    pass


def _find_item(items: list[dict], item_id: str) -> dict | None:
    target = clean_cell_value(item_id)
    return next(
        (item for item in items if clean_cell_value(item.get("id")) == target),
        None,
    )


def _ensure_not_syncing(item: dict, folder: str) -> None:
    from app.checklists.upload_jobs import resolve_document_mirror_status

    for document in documents_below(item.get("documents") or [], folder):
        status = clean_cell_value(
            resolve_document_mirror_status(document).get("status")
        ).lower()
        if status in SYNCING_STATUSES:
            raise FolderOperationError(
                "Файлы папки ещё синхронизируются с Яндекс.Диском — "
                "повторите через минуту"
            )


def _relocate_folder(item: dict, source: str, target: str) -> int:
    folders = item_subfolders(item)
    item["subfolders"] = [rebase_folder(folder, source, target) for folder in folders]
    moved = 0
    documents = normalize_documents_list(item.get("documents"))
    for document in documents:
        current = document_relative_folder(document)
        if is_within(current, source):
            document["relativeFolder"] = rebase_folder(current, source, target)
            moved += 1
    item["documents"] = documents
    return moved


async def _run_folder_change(request: Request, change) -> JSONResponse:
    payload = await request.json()
    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    config = get_checklist_config(normalize_checklist_key(payload.get("checklistKey")))
    item_id = clean_cell_value(payload.get("itemId"))
    if not dialog_id or not item_id:
        return JSONResponse({"ok": False, "error": "dialogId and itemId are required"}, status_code=400)
    payload = {**payload, "requireEditSession": True}
    try:
        transaction = begin_optional_edit_session_change(
            payload,
            dialog_id=dialog_id,
            checklist_key=config.key,
        )
        async with checklist_mutation_guard(dialog_id, config.key):
            data = get_checklist(dialog_id, config.key)
            before_checklist = copy.deepcopy(data)
            items = data.get("items") or []
            item = _find_item(items, item_id)
            if item is None:
                return JSONResponse({"ok": False, "error": "item not found"}, status_code=404)
            ensure_checklist_snapshot(
                session_id=transaction["sessionId"],
                dialog_id=dialog_id,
                checklist_key=config.key,
                data=before_checklist,
            )
            before_item = copy.deepcopy(item)
            outcome = change(payload, item)
            if outcome.get("unchanged"):
                return JSONResponse({"ok": True, "unchanged": True, "item": item, **outcome.get("response", {})})
            data["items"] = items
            saved = save_checklist(
                dialog_id,
                normalize_checklist_data(data, config.key),
                config.key,
            )
            saved_item = _find_item(saved.get("items") or [], item_id) or item
            operation = record_or_restore_checklist_change(
                transaction=transaction,
                dialog_id=dialog_id,
                checklist_key=config.key,
                operation_type=outcome["operationType"],
                before={"item": before_item},
                after={"item": saved_item},
                before_checklist=before_checklist,
                final_checklist=saved,
                item_id=item_id,
                payload=outcome["payload"],
            )
        return JSONResponse({
            "ok": True,
            "dialogId": dialog_id,
            "checklistKey": config.key,
            "item": saved_item,
            "operation": operation,
            **outcome.get("response", {}),
        })
    except (FolderOperationError, FolderPathError) as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    except Exception as exc:
        write_debug_log("checklist_folder_operation_failed", {
            "dialogId": dialog_id,
            "itemId": item_id,
            "path": str(request.url.path),
            "error": str(exc),
        })
        return checklist_edit_session_error_response(exc)


@router.post("/api/checklist/folders/create")
async def api_folder_create(request: Request):
    def change(payload: dict, item: dict) -> dict:
        if not item_allows_plain_folders(item):
            raise FolderOperationError(
                "Папки пункта — это его подпункты. Добавьте подпункт."
            )
        folders = item_subfolders(item)
        # A missing parent is created too (empty folders of an uploaded tree).
        parent = canonical_folder(folders, normalize_relative_folder(payload.get("parentFolder")))
        name = sanitize_folder_segment(payload.get("name"))
        path = canonical_folder(folders, f"{parent}/{name}" if parent else name)
        existing = find_folder(folders, path)
        if existing:
            # Idempotent: folder uploads re-create the same structure.
            return {"unchanged": True, "response": {"relativeFolder": existing, "created": False}}
        item["subfolders"] = folders + [path]
        return {
            "operationType": "checklist_folder_create",
            "payload": {
                "relativeFolder": path,
                "folderUploadRoot": normalize_relative_folder(
                    payload.get("folderUploadRoot")
                ),
            },
            "response": {"relativeFolder": path, "created": True},
        }

    return await _run_folder_change(request, change)


def _move_change(payload: dict, item: dict, *, rename: bool) -> dict:
    folders = item_subfolders(item)
    source = find_folder(folders, normalize_relative_folder(payload.get("folder")))
    if not source:
        raise FolderOperationError("Папка не найдена")
    if rename:
        new_name = sanitize_folder_segment(payload.get("name"))
        parent = parent_folder(source)
    else:
        new_name = folder_name(source)
        requested_parent = normalize_relative_folder(payload.get("targetFolder"))
        parent = find_folder(folders, requested_parent) if requested_parent else ""
        if requested_parent and not parent:
            raise FolderOperationError("Папка назначения не найдена")
        if parent and is_within(parent, source):
            raise FolderOperationError("Нельзя переместить папку внутрь неё самой")
    target = f"{parent}/{new_name}" if parent else new_name
    if target == source:
        return {"unchanged": True, "response": {"relativeFolder": source}}
    clash = find_folder(folders, target)
    if clash and folder_key(clash) != folder_key(source):
        raise FolderOperationError(f"Папка «{target}» уже существует")
    _ensure_not_syncing(item, source)
    moved = _relocate_folder(item, source, target)
    return {
        "operationType": "checklist_folder_move",
        "payload": {
            "sourceFolder": source,
            "targetFolder": target,
            "kind": "rename" if rename else "move",
            "documentCount": moved,
        },
        "response": {"relativeFolder": target, "previousFolder": source},
    }


@router.post("/api/checklist/folders/rename")
async def api_folder_rename(request: Request):
    return await _run_folder_change(
        request,
        lambda payload, item: _move_change(payload, item, rename=True),
    )


@router.post("/api/checklist/folders/move")
async def api_folder_move(request: Request):
    return await _run_folder_change(
        request,
        lambda payload, item: _move_change(payload, item, rename=False),
    )


@router.post("/api/checklist/folders/delete")
async def api_folder_delete(request: Request):
    payload = await request.json()
    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    config = get_checklist_config(normalize_checklist_key(payload.get("checklistKey")))
    item_id = clean_cell_value(payload.get("itemId"))
    acting_user_id = clean_cell_value(payload.get("actingUserId"))
    acting_user_name = clean_cell_value(payload.get("actingUserName")) or "Пользователь"
    session_id = clean_cell_value(payload.get("sessionId"))
    if not session_id:
        return JSONResponse({"ok": False, "error": "Активная сессия редактирования не готова", "editSessionError": True}, status_code=409)
    if not can_user_delete_files(acting_user_id):
        return JSONResponse({"ok": False, "error": "У вас недостаточно прав на удаление файлов"}, status_code=403)

    data = get_checklist(dialog_id, config.key)
    item = _find_item(data.get("items") or [], item_id)
    if item is None:
        return JSONResponse({"ok": False, "error": "item not found"}, status_code=404)
    try:
        folder = find_folder(item_subfolders(item), normalize_relative_folder(payload.get("folder")))
        if not folder:
            raise FolderOperationError("Папка не найдена")
        _ensure_not_syncing(item, folder)
    except (FolderOperationError, FolderPathError) as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)

    # Files go through the ordinary transactional removal (archive-safe,
    # restored by Cancel, deleted on Yandex after Save).
    from app.checklists.edit_session_documents import transactional_remove_document
    removed = 0
    removed_series: list[str] = []
    for document in documents_below(item.get("documents") or [], folder):
        try:
            transactional_remove_document(
                session_id=session_id,
                dialog_id=dialog_id,
                checklist_key=config.key,
                item_id=item_id,
                document_id=clean_cell_value(document.get("id")),
                acting_user_id=acting_user_id,
                acting_user_name=acting_user_name,
            )
            removed += 1
            removed_series.append(
                clean_cell_value(document.get("seriesId") or document.get("id"))
            )
        except Exception as exc:
            write_debug_log("checklist_folder_delete_document_failed", {
                "dialogId": dialog_id,
                "itemId": item_id,
                "folder": folder,
                "documentId": clean_cell_value(document.get("id")),
                "error": str(exc),
            })
            return checklist_edit_session_error_response(exc)

    def change(payload: dict, current: dict) -> dict:
        folders = item_subfolders(current)
        current["subfolders"] = [value for value in folders if not is_within(value, folder)]
        return {
            "operationType": "checklist_folder_delete",
            "payload": {
                "relativeFolder": folder,
                "documentCount": removed,
                "documentSeriesIds": removed_series,
            },
            "response": {"relativeFolder": folder, "removedDocuments": removed},
        }

    class _Replay:
        # _run_folder_change reads the JSON body; reuse the parsed payload.
        def __init__(self, source: Request, body: dict):
            self.url = source.url
            self._body = body

        async def json(self):
            return self._body

    return await _run_folder_change(_Replay(request, payload), change)


def _zip_entries(data: dict, item: dict, folder: str) -> list[tuple[Path, str]]:
    """(local file, archive path) of current files below ``folder``.

    For a top-level item its subitems are included as folders.
    """
    entries: list[tuple[Path, str]] = []

    def add_documents(owner: dict, base_folder: str, prefix: str) -> None:
        for document in documents_below(owner.get("documents") or [], base_folder):
            local = get_upload_file_path_from_url(
                clean_cell_value(document.get("fileUrl") or document.get("path"))
            )
            if local is None or not local.is_file():
                continue
            relative = document_relative_folder(document)
            if base_folder:
                relative = relative[len(base_folder):].lstrip("/")
            name = clean_cell_value(document.get("name")) or local.name
            parts = [part for part in [prefix, relative, name] if part]
            entries.append((local, "/".join(parts)))

    add_documents(item, folder, "")
    if not folder and not parent_id_of(item):
        for child in children_of(data.get("items") or [], clean_cell_value(item.get("id"))):
            child_name = sanitize_folder_segment(child.get("name"), strict=False) or "Подпункт"
            add_documents(child, "", child_name)
    return entries


@router.get("/api/checklist/folders/zip")
def api_folder_zip(
    dialogId: str = "",
    checklistKey: str = "id",
    itemId: str = "",
    folder: str = "",
):
    dialog_id = normalize_dialog_id(dialogId)
    config = get_checklist_config(normalize_checklist_key(checklistKey))
    data = get_checklist(dialog_id, config.key)
    item = _find_item(data.get("items") or [], itemId)
    if item is None:
        return JSONResponse({"ok": False, "error": "item not found"}, status_code=404)
    try:
        relative = normalize_relative_folder(folder)
    except FolderPathError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    if relative and not find_folder(item_subfolders(item), relative):
        return JSONResponse({"ok": False, "error": "Папка не найдена"}, status_code=404)

    entries = _zip_entries(data, item, relative)
    handle = tempfile.NamedTemporaryFile(prefix="checklist_folder_", suffix=".zip", delete=False)
    handle.close()
    used: set[str] = set()
    with zipfile.ZipFile(handle.name, "w", compression=zipfile.ZIP_DEFLATED, allowZip64=True) as archive:
        for local, archive_path in entries:
            candidate = archive_path
            number = 2
            while candidate.casefold() in used:
                stem, dot, ext = archive_path.rpartition(".")
                candidate = f"{stem} ({number}).{ext}" if dot else f"{archive_path} ({number})"
                number += 1
            used.add(candidate.casefold())
            archive.write(local, candidate)
        # Empty folders are part of the structure too.
        empty_candidates = [
            (value[len(relative):].lstrip("/") if relative else value)
            for value in item_subfolders(item)
            if is_within(value, relative) and folder_key(value) != folder_key(relative)
        ]
        if not relative and not parent_id_of(item):
            for child in children_of(data.get("items") or [], clean_cell_value(item.get("id"))):
                child_name = sanitize_folder_segment(child.get("name"), strict=False) or "Подпункт"
                empty_candidates.append(child_name)
                empty_candidates.extend(
                    f"{child_name}/{value}" for value in item_subfolders(child)
                )
        for inner in empty_candidates:
            prefix = inner.casefold() + "/"
            if not any(path.startswith(prefix) for path in used):
                archive.writestr(inner + "/", "")

    download_name = (
        folder_name(relative)
        or sanitize_folder_segment(item.get("name"), strict=False)
        or "folder"
    ) + ".zip"
    return FileResponse(
        handle.name,
        media_type="application/zip",
        filename=download_name,
        background=BackgroundTask(lambda: Path(handle.name).unlink(missing_ok=True)),
    )


def folder_tree(item: dict, folder: str = "") -> list[dict]:
    """Nested structure below ``folder``: [{path, name, documents, children}]."""
    folders = item_subfolders(item)
    documents = normalize_documents_list(item.get("documents"))

    def build(parent: str) -> list[dict]:
        nodes = []
        for path in child_folders(folders, parent):
            nodes.append({
                "path": path,
                "name": folder_name(path),
                "documents": [
                    document for document in documents
                    if folder_key(document_relative_folder(document)) == folder_key(path)
                ],
                "children": build(path),
            })
        return nodes

    return build(folder)
