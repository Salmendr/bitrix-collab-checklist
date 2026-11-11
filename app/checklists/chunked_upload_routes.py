"""Uploads of big files in parts.

IIS in front of the application takes a request of 4 294 967 295 bytes at
most (its limit is a 32-bit number). A file from 1 GiB on goes in parts of
64 MiB: the browser cuts it by bytes (any format: PDF, DWG, ZIP), the server
writes every part at its place in one temporary file and, when the size is
complete, the file joins the checklist like an ordinary upload (the same
operation of the edit session, chat message and Yandex). A break sends again
only the part in progress, not the whole file.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import shutil
import threading
import time
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from starlette.requests import ClientDisconnect

from app.auth.identity import verified_actor
from app.logging_utils import write_debug_log
from app.settings import VOLUME_DIR

from app.checklists.document_folders import FolderPathError, normalize_relative_folder
from app.checklists.utils import (
    clean_cell_value,
    normalize_checklist_key,
    normalize_dialog_id,
)


CHUNK_THRESHOLD_BYTES = 1024 ** 3
CHUNK_SIZE_BYTES = 64 * 1024 ** 2
MAX_CHUNK_BYTES = CHUNK_SIZE_BYTES + 1024 ** 2
MAX_FILE_BYTES = 200 * 1024 ** 3
FREE_SPACE_RESERVE_BYTES = 512 * 1024 ** 2
STALE_UPLOAD_SECONDS = 6 * 3600
COMPLETED_KEEP_SECONDS = 3600
CHUNK_ROOT = VOLUME_DIR / "chunked_uploads"

_UPLOAD_ID_RE = re.compile(r"^[0-9a-f]{32}$")

router = APIRouter()

_locks_guard = threading.Lock()
_upload_locks: dict[str, asyncio.Lock] = {}


class AssembledUpload:
    """A file received in parts: the upload code moves it into the folder of
    the item instead of reading it once more."""

    def __init__(self, path: Path, filename: str, content_type: str = ""):
        self.prepared_path = Path(path)
        self.filename = filename
        self.content_type = content_type

    async def read(self, size: int = -1) -> bytes:
        raise RuntimeError("an assembled upload is moved, not read")

    async def close(self) -> None:
        return None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _valid_id(upload_id: str) -> str:
    value = clean_cell_value(upload_id).lower()
    return value if _UPLOAD_ID_RE.match(value) else ""


def _meta_path(upload_id: str) -> Path:
    return CHUNK_ROOT / f"{upload_id}.json"


def _part_path(upload_id: str) -> Path:
    return CHUNK_ROOT / f"{upload_id}.part"


def _read_meta(upload_id: str) -> dict | None:
    upload_id = _valid_id(upload_id)
    if not upload_id:
        return None
    try:
        return json.loads(_meta_path(upload_id).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _write_meta(meta: dict) -> None:
    CHUNK_ROOT.mkdir(parents=True, exist_ok=True)
    path = _meta_path(meta["uploadId"])
    temp = path.with_suffix(".json.tmp")
    temp.write_text(json.dumps(meta, ensure_ascii=False), encoding="utf-8")
    os.replace(temp, path)


def _drop(upload_id: str, keep_meta: bool = False) -> None:
    for path in (_part_path(upload_id),) + (() if keep_meta else (_meta_path(upload_id),)):
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        except OSError as exc:
            write_debug_log("chunked_upload_cleanup_failed", {"uploadId": upload_id, "path": str(path), "error": str(exc)})


def _upload_lock(upload_id: str) -> asyncio.Lock:
    with _locks_guard:
        lock = _upload_locks.get(upload_id)
        if lock is None:
            lock = asyncio.Lock()
            _upload_locks[upload_id] = lock
        return lock


def _forget_lock(upload_id: str) -> None:
    with _locks_guard:
        _upload_locks.pop(upload_id, None)


def _session_active(session_id: str) -> bool:
    from app.checklists.edit_sessions import get_edit_session

    record = get_edit_session(clean_cell_value(session_id)) or {}
    return clean_cell_value(record.get("status")) == "active"


def _age_seconds(value: str) -> float:
    try:
        moment = datetime.fromisoformat(clean_cell_value(value))
    except ValueError:
        return 10 ** 9
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - moment).total_seconds()


def cleanup_stale_chunked_uploads() -> int:
    """Parts left by a closed window or an ended session are removed."""
    if not CHUNK_ROOT.exists():
        return 0
    removed = 0
    for meta_file in CHUNK_ROOT.glob("*.json"):
        upload_id = meta_file.stem
        meta = _read_meta(upload_id)
        if meta is None:
            continue
        if meta.get("status") == "completed":
            if _age_seconds(meta.get("updatedAt", "")) > COMPLETED_KEEP_SECONDS:
                _drop(upload_id)
            continue
        stale = _age_seconds(meta.get("updatedAt", "")) > STALE_UPLOAD_SECONDS
        if stale or not _session_active(meta.get("sessionId", "")):
            _drop(upload_id)
            removed += 1
    for part in CHUNK_ROOT.glob("*.part"):
        if not _meta_path(part.stem).exists():
            try:
                part.unlink()
                removed += 1
            except OSError:
                pass
    if removed:
        write_debug_log("chunked_upload_cleanup", {"removed": removed})
    return removed


def _error(status: int, message: str, **extra) -> JSONResponse:
    return JSONResponse({"ok": False, "error": message, **extra}, status_code=status)


def _owner_mismatch(request: Request, meta: dict) -> bool:
    user_id, _ = verified_actor(request, "", "")
    user_id = clean_cell_value(user_id)
    return bool(user_id and user_id != clean_cell_value(meta.get("userId")))


def _public(meta: dict) -> dict:
    return {
        "uploadId": meta.get("uploadId"),
        "sessionId": meta.get("sessionId"),
        "dialogId": meta.get("dialogId"),
        "checklistKey": meta.get("checklistKey"),
        "itemId": meta.get("itemId"),
        "operationKind": meta.get("operationKind"),
        "documentId": meta.get("documentId", ""),
        "fileName": meta.get("fileName"),
        "fileSize": meta.get("fileSize"),
        "receivedBytes": meta.get("receivedBytes"),
        "userId": meta.get("userId"),
    }


@router.post("/api/checklist/chunked-upload/start")
async def api_chunked_upload_start(request: Request):
    from app.checklists.config import get_checklist_config
    from app.checklists.document_routes import (
        document_edit_session_error_response,
        enforce_required_edit_session,
        get_document_replacement_guard,
        parse_bool_form_value,
    )
    from app.checklists.documents import migrate_legacy_document_fields, normalize_documents_list
    from app.checklists.edit_session_changes import acquire_checklist_for_edit_session
    from app.checklists.storage import get_checklist

    try:
        payload = await request.json()
    except Exception:
        return _error(400, "Некорректный запрос")
    if not isinstance(payload, dict):
        return _error(400, "Некорректный запрос")

    kind = "replace" if clean_cell_value(payload.get("operationKind")) == "replace" else "upload"
    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    checklist_key = normalize_checklist_key(payload.get("checklistKey") or "id")
    item_id = clean_cell_value(payload.get("itemId"))
    document_id = clean_cell_value(payload.get("documentId"))
    file_name = Path(str(payload.get("fileName") or "")).name
    user_id, user_name = verified_actor(
        request,
        clean_cell_value(payload.get("actingUserId")),
        clean_cell_value(payload.get("actingUserName")),
    )
    user_id = clean_cell_value(user_id)
    user_name = clean_cell_value(user_name) or "Пользователь"
    try:
        file_size = int(payload.get("fileSize") or 0)
    except (TypeError, ValueError):
        file_size = 0

    if not dialog_id or not item_id:
        return _error(400, "dialogId и itemId обязательны")
    if not file_name:
        return _error(400, "Не указано имя файла")
    if file_size <= 0 or file_size > MAX_FILE_BYTES:
        return _error(400, "Недопустимый размер файла")
    if kind == "replace" and not document_id:
        return _error(400, "documentId is required")

    try:
        relative_folder = normalize_relative_folder(clean_cell_value(payload.get("relativeFolder")))
        folder_upload_root = normalize_relative_folder(clean_cell_value(payload.get("folderUploadRoot")))
    except FolderPathError as exc:
        return _error(400, str(exc))

    try:
        cleanup_stale_chunked_uploads()
    except Exception as exc:
        write_debug_log("chunked_upload_cleanup_failed", {"error": str(exc)})

    CHUNK_ROOT.mkdir(parents=True, exist_ok=True)
    free = shutil.disk_usage(CHUNK_ROOT).free
    if free < file_size + FREE_SPACE_RESERVE_BYTES:
        write_debug_log("chunked_upload_no_space", {
            "dialogId": dialog_id, "fileName": file_name, "fileSize": file_size, "freeBytes": free,
        })
        return _error(507, "Недостаточно места на сервере для этого файла")

    try:
        session_id = enforce_required_edit_session(payload.get("sessionId"), True)
        # The first change takes the lock: a checklist held by another
        # employee refuses before a single part is sent.
        acquire_checklist_for_edit_session(
            session_id=session_id,
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            user_id=user_id,
            user_name=user_name,
        )
        config = get_checklist_config(checklist_key)
        data = get_checklist(dialog_id, config.key)
        target_item = None
        for raw_item in data.get("items", []) or []:
            if clean_cell_value(raw_item.get("id")) == item_id:
                target_item = migrate_legacy_document_fields(raw_item)
                break
        if target_item is None:
            return _error(404, "Пункт не найден")
        if kind == "replace":
            force_replace = parse_bool_form_value(payload.get("forceReplace"))
            documents = normalize_documents_list(target_item.get("documents"))
            old_document = next(
                (doc for doc in documents if clean_cell_value(doc.get("id")) == document_id),
                None,
            )
            if old_document is None:
                return _error(404, "Заменяемый файл не найден")
            # Checked before any part is sent, not after gigabytes.
            guard = get_document_replacement_guard(old_document, force_replace)
            if guard.get("blocked"):
                return _error(
                    409,
                    guard.get("error") or "Замена недоступна",
                    requiresForceReplace=bool(guard.get("requiresForceReplace")),
                    mirrorStatus=guard.get("mirrorStatus", ""),
                )
        else:
            force_replace = False
    except Exception as exc:
        write_debug_log("chunked_upload_start_failed", {
            "dialogId": dialog_id, "checklistKey": checklist_key, "itemId": item_id,
            "fileName": file_name, "fileSize": file_size, "sessionId": clean_cell_value(payload.get("sessionId")),
            "error": str(exc),
        })
        return document_edit_session_error_response(exc)

    upload_id = uuid.uuid4().hex
    _part_path(upload_id).write_bytes(b"")
    now = _now_iso()
    meta = {
        "uploadId": upload_id,
        "status": "receiving",
        "operationKind": kind,
        "sessionId": session_id,
        "dialogId": dialog_id,
        "checklistKey": checklist_key,
        "itemId": item_id,
        "itemGroup": clean_cell_value(payload.get("itemGroup")),
        "documentId": document_id,
        "forceReplace": bool(force_replace),
        "relativeFolder": relative_folder,
        "folderUploadRoot": folder_upload_root,
        "folderReplaceId": clean_cell_value(payload.get("folderReplaceId")),
        "fileName": file_name,
        "fileSize": file_size,
        "receivedBytes": 0,
        "userId": user_id,
        "userName": user_name,
        "createdAt": now,
        "updatedAt": now,
        "startedMonotonic": time.time(),
    }
    _write_meta(meta)
    write_debug_log("chunked_upload_started", _public(meta))
    return JSONResponse({
        "ok": True,
        "uploadId": upload_id,
        "chunkSize": CHUNK_SIZE_BYTES,
        "receivedBytes": 0,
    })


@router.put("/api/checklist/chunked-upload/chunk")
async def api_chunked_upload_chunk(request: Request, uploadId: str = "", offset: int = -1):
    upload_id = _valid_id(uploadId)
    meta = _read_meta(upload_id) if upload_id else None
    if meta is None or meta.get("status") != "receiving":
        return _error(404, "Загрузка не найдена — она будет начата заново", uploadMissing=True)
    if _owner_mismatch(request, meta):
        return _error(403, "Загрузка принадлежит другому сотруднику")
    if not _session_active(meta.get("sessionId", "")):
        _drop(upload_id)
        return _error(409, "Сессия редактирования уже завершена", uploadMissing=True, editSessionError=True)

    declared = request.headers.get("content-length", "")
    if declared.isdigit() and int(declared) > MAX_CHUNK_BYTES:
        return _error(400, "Слишком большой кусок файла")

    async with _upload_lock(upload_id):
        meta = _read_meta(upload_id) or meta
        received = int(meta.get("receivedBytes") or 0)
        file_size = int(meta.get("fileSize") or 0)
        part = _part_path(upload_id)
        if not part.exists():
            _drop(upload_id)
            return _error(404, "Загрузка не найдена — она будет начата заново", uploadMissing=True)
        if part.stat().st_size != received:
            # A part cut off before: only what was confirmed stays.
            with part.open("r+b") as handle:
                handle.truncate(received)
        if offset != received:
            # A part sent again after a lost answer, or resumed after a break.
            return _error(409, "Продолжаем с полученного места", offsetMismatch=True, receivedBytes=received)

        written = 0
        try:
            with part.open("r+b") as handle:
                handle.seek(received)
                async for piece in request.stream():
                    if not piece:
                        continue
                    written += len(piece)
                    if written > MAX_CHUNK_BYTES or received + written > file_size:
                        raise ValueError("Кусок файла больше ожидаемого")
                    handle.write(piece)
        except ClientDisconnect:
            with part.open("r+b") as handle:
                handle.truncate(received)
            write_debug_log("chunked_upload_chunk_interrupted", {
                "uploadId": upload_id, "offset": received, "writtenBeforeBreak": written,
                "fileName": meta.get("fileName"), "fileSize": file_size,
            })
            return _error(400, "Передача куска прервана", retryable=True)
        except ValueError as exc:
            with part.open("r+b") as handle:
                handle.truncate(received)
            return _error(400, str(exc))
        except Exception as exc:
            with part.open("r+b") as handle:
                handle.truncate(received)
            write_debug_log("chunked_upload_chunk_failed", {
                "uploadId": upload_id, "offset": received, "error": str(exc),
                "traceback": traceback.format_exc()[-3000:],
            })
            return _error(500, "Не удалось записать кусок файла")

        if written <= 0:
            return _error(400, "Пустой кусок файла")
        meta["receivedBytes"] = received + written
        meta["updatedAt"] = _now_iso()
        _write_meta(meta)

    return JSONResponse({
        "ok": True,
        "receivedBytes": meta["receivedBytes"],
        "complete": meta["receivedBytes"] == file_size,
    })


@router.post("/api/checklist/chunked-upload/complete")
async def api_chunked_upload_complete(request: Request):
    from app.checklists.document_routes import document_edit_session_error_response
    from app.checklists.edit_session_documents import (
        transactional_replace_document,
        transactional_upload_document,
    )

    try:
        payload = await request.json()
    except Exception:
        payload = {}
    upload_id = _valid_id((payload or {}).get("uploadId", "") if isinstance(payload, dict) else "")
    meta = _read_meta(upload_id) if upload_id else None
    if meta is None:
        return _error(404, "Загрузка не найдена — она будет начата заново", uploadMissing=True)
    if _owner_mismatch(request, meta):
        return _error(403, "Загрузка принадлежит другому сотруднику")
    if meta.get("status") == "completed":
        # The answer was lost: the file is already in the checklist.
        return JSONResponse(meta.get("result") or {"ok": True})

    async with _upload_lock(upload_id):
        meta = _read_meta(upload_id) or meta
        if meta.get("status") == "completed":
            return JSONResponse(meta.get("result") or {"ok": True})
        received = int(meta.get("receivedBytes") or 0)
        file_size = int(meta.get("fileSize") or 0)
        part = _part_path(upload_id)
        if received != file_size or not part.exists() or part.stat().st_size != file_size:
            return _error(409, "Файл получен не полностью", offsetMismatch=True, receivedBytes=received)

        assembled = AssembledUpload(part, meta.get("fileName") or "file.bin")
        started = time.monotonic()
        try:
            if meta.get("operationKind") == "replace":
                result = await transactional_replace_document(
                    session_id=meta["sessionId"],
                    dialog_id=meta["dialogId"],
                    checklist_key=meta["checklistKey"],
                    item_id=meta["itemId"],
                    document_id=meta.get("documentId", ""),
                    file=assembled,
                    acting_user_id=meta.get("userId", ""),
                    acting_user_name=meta.get("userName", ""),
                    force_replace=bool(meta.get("forceReplace")),
                )
            else:
                result = await transactional_upload_document(
                    session_id=meta["sessionId"],
                    dialog_id=meta["dialogId"],
                    checklist_key=meta["checklistKey"],
                    item_id=meta["itemId"],
                    item_group=int(str(meta.get("itemGroup") or "0").strip() or 0),
                    file=assembled,
                    acting_user_id=meta.get("userId", ""),
                    acting_user_name=meta.get("userName", ""),
                    relative_folder=meta.get("relativeFolder", ""),
                    folder_upload_root=meta.get("folderUploadRoot", ""),
                    folder_replace_id=meta.get("folderReplaceId", ""),
                )
        except Exception as exc:
            write_debug_log("chunked_upload_complete_failed", {
                **_public(meta),
                "error": str(exc),
                "traceback": traceback.format_exc()[-4000:],
            })
            _drop(upload_id)
            _forget_lock(upload_id)
            return document_edit_session_error_response(exc)

        meta["status"] = "completed"
        meta["updatedAt"] = _now_iso()
        meta["result"] = result
        _write_meta(meta)
        _drop(upload_id, keep_meta=True)
        write_debug_log("chunked_upload_completed", {
            **_public(meta),
            "documentId": clean_cell_value((result.get("document") or {}).get("id")) if isinstance(result, dict) else "",
            "moveMs": int((time.monotonic() - started) * 1000),
            "totalSeconds": round(time.time() - float(meta.get("startedMonotonic") or time.time()), 1),
        })
    _forget_lock(upload_id)
    return JSONResponse(result)


@router.post("/api/checklist/chunked-upload/abort")
async def api_chunked_upload_abort(request: Request):
    try:
        payload = await request.json()
    except Exception:
        try:
            payload = json.loads((await request.body()).decode("utf-8") or "{}")
        except Exception:
            payload = {}
    upload_id = _valid_id((payload or {}).get("uploadId", "") if isinstance(payload, dict) else "")
    meta = _read_meta(upload_id) if upload_id else None
    if meta is None:
        return JSONResponse({"ok": True, "removed": False})
    if _owner_mismatch(request, meta):
        return _error(403, "Загрузка принадлежит другому сотруднику")
    if meta.get("status") == "completed":
        return JSONResponse({"ok": True, "removed": False, "completed": True})
    _drop(upload_id)
    _forget_lock(upload_id)
    write_debug_log("chunked_upload_aborted", {
        **_public(meta),
        "reason": clean_cell_value((payload or {}).get("reason")),
    })
    return JSONResponse({"ok": True, "removed": True})
