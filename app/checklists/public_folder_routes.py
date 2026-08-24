from __future__ import annotations

import html
import json
import mimetypes
from pathlib import Path
from urllib.parse import quote

from fastapi import APIRouter, File, Form, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse

from app.checklists.document_version_links import (
    find_pinned_version_in_item,
    get_archive_pinned_version_id,
    resolve_pinned_version_local_path,
)
from app.checklists.documents import (
    migrate_legacy_document_fields,
    normalize_archive_versions,
    normalize_detached_archive_series,
    normalize_documents_list,
)
from app.checklists.edit_sessions import (
    EditSessionConflictError,
    get_edit_session_for_actor,
)
from app.checklists.public_folder_links import (
    get_or_create_public_folder_link,
    public_folder_link_path,
    public_folder_link_payload,
    reissue_public_folder_link,
    resolve_public_folder_token,
)
from app.checklists.public_folder_operations import (
    PublicFolderOperationConflict,
    fail_public_folder_operation_receiving,
    finalize_received_public_folder_operation,
    get_public_folder_operation,
    prepare_public_folder_operation,
    process_public_folder_operation,
    public_folder_operation_payload,
    require_public_staging_path,
)
from app.checklists.storage import get_checklist
from app.checklists.upload_jobs import resolve_document_mirror_status
from app.checklists.utils import (
    can_preview_in_browser,
    clean_cell_value,
    normalize_checklist_key,
    normalize_dialog_id,
)
from app.ui.shell import (
    get_public_app_base_path,
    get_public_app_base_url,
    normalize_base_path,
)
from app.ui.template_engine import render_ui_template


router = APIRouter()


def _safe_json(value) -> str:
    return (
        json.dumps(value, ensure_ascii=False)
        .replace("</", "<\\/")
        .replace("\u2028", "\\u2028")
        .replace("\u2029", "\\u2029")
    )


def _find_item(data: dict, item_id: str) -> dict | None:
    normalized_item_id = clean_cell_value(item_id)
    for raw_item in data.get("items", []) or []:
        if clean_cell_value(raw_item.get("id")) == normalized_item_id:
            return migrate_legacy_document_fields(raw_item)
    return None


def _require_link_item(link: dict) -> tuple[dict, dict]:
    data = get_checklist(
        link.get("dialog_id") or "",
        link.get("checklist_key") or "id",
    )
    item = _find_item(data, link.get("item_id") or "")
    if not item:
        raise KeyError("item not found")
    return data, item


def _require_internal_link_session(payload: dict) -> dict:
    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    session_id = clean_cell_value(payload.get("sessionId"))
    user_id = clean_cell_value(payload.get("actingUserId"))
    if not dialog_id or not session_id:
        raise EditSessionConflictError(
            "Активная сессия редактирования не готова"
        )
    session = get_edit_session_for_actor(
        session_id=session_id,
        dialog_id=dialog_id,
        user_id=user_id,
    )
    if session.get("status") != "active":
        raise EditSessionConflictError(
            "Сессия редактирования больше не активна"
        )
    return session


def _internal_link_response(
    request: Request,
    record: dict,
    *,
    item_name: str,
) -> JSONResponse:
    path = public_folder_link_path(record)
    absolute_url = get_public_app_base_url(request).rstrip("/") + path
    payload = public_folder_link_payload(record)
    payload.update({
        "ok": True,
        "itemName": item_name,
        "url": absolute_url,
        "relativeUrl": get_public_app_base_path(request) + path,
        "permanent": True,
    })
    return JSONResponse(payload)


@router.post("/api/checklist/public-folder-link")
async def api_get_or_create_public_folder_link(request: Request):
    payload = await request.json()
    try:
        _require_internal_link_session(payload)
        dialog_id = normalize_dialog_id(payload.get("dialogId"))
        checklist_key = normalize_checklist_key(payload.get("checklistKey"))
        item_id = clean_cell_value(payload.get("itemId"))
        data = get_checklist(dialog_id, checklist_key)
        item = _find_item(data, item_id)
        if not item:
            return JSONResponse(
                {"ok": False, "error": "item not found"},
                status_code=404,
            )
        record = get_or_create_public_folder_link(
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            item_id=item_id,
            acting_user_id=payload.get("actingUserId") or "",
            acting_user_name=payload.get("actingUserName") or "",
        )
        return _internal_link_response(
            request,
            record,
            item_name=clean_cell_value(item.get("name")) or "Пункт",
        )
    except EditSessionConflictError as exc:
        return JSONResponse(
            {"ok": False, "error": str(exc), "editSessionError": True},
            status_code=409,
        )
    except Exception as exc:
        return JSONResponse(
            {"ok": False, "error": str(exc).strip("'")},
            status_code=400,
        )


@router.post("/api/checklist/public-folder-link/reissue")
async def api_reissue_public_folder_link(request: Request):
    payload = await request.json()
    try:
        _require_internal_link_session(payload)
        dialog_id = normalize_dialog_id(payload.get("dialogId"))
        checklist_key = normalize_checklist_key(payload.get("checklistKey"))
        item_id = clean_cell_value(payload.get("itemId"))
        data = get_checklist(dialog_id, checklist_key)
        item = _find_item(data, item_id)
        if not item:
            return JSONResponse(
                {"ok": False, "error": "item not found"},
                status_code=404,
            )
        record = reissue_public_folder_link(
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            item_id=item_id,
            acting_user_id=payload.get("actingUserId") or "",
            acting_user_name=payload.get("actingUserName") or "",
        )
        return _internal_link_response(
            request,
            record,
            item_name=clean_cell_value(item.get("name")) or "Пункт",
        )
    except EditSessionConflictError as exc:
        return JSONResponse(
            {"ok": False, "error": str(exc), "editSessionError": True},
            status_code=409,
        )
    except Exception as exc:
        return JSONResponse(
            {"ok": False, "error": str(exc).strip("'")},
            status_code=400,
        )


def _public_file_path(
    token: str,
    version_id: str,
    *,
    base_path: str = "",
    download: bool = False,
) -> str:
    path = normalize_base_path(base_path) + (
        "/public/folder/"
        + quote(token, safe="")
        + "/file/"
        + quote(version_id, safe="")
    )
    return path + ("?download=1" if download else "")


def _archive_payload(
    *,
    token: str,
    series_id: str,
    label: str,
    versions,
    detached: bool,
    base_path: str = "",
) -> dict | None:
    normalized = normalize_archive_versions(versions, series_id=series_id)
    if not normalized:
        return None
    rows = []
    for version in reversed(normalized):
        version_id = get_archive_pinned_version_id(version)
        if not version_id:
            continue
        rows.append({
            "versionId": version_id,
            "versionLabel": (
                clean_cell_value(version.get("versionLabel"))
                or f"v{int(version.get('version') or 0)}"
            ),
            "name": (
                clean_cell_value(version.get("originalName"))
                or clean_cell_value(version.get("name"))
                or "Архивный файл"
            ),
            "size": int(version.get("size") or 0),
            "uploadedAt": (
                clean_cell_value(version.get("uploadedAt"))
                or clean_cell_value(version.get("archivedAt"))
            ),
            "uploadedByName": (
                clean_cell_value(version.get("uploadedByName")) or "—"
            ),
            "archivedAt": clean_cell_value(version.get("archivedAt")),
            "openUrl": _public_file_path(
                token,
                version_id,
                base_path=base_path,
            ),
            "downloadUrl": _public_file_path(
                token,
                version_id,
                base_path=base_path,
                download=True,
            ),
        })
    if not rows:
        return None
    return {
        "seriesId": series_id,
        "label": label or "Архив версий",
        "detached": bool(detached),
        "versions": rows,
    }


def _public_folder_state(
    token: str,
    link: dict,
    *,
    base_path: str = "",
) -> dict:
    data, item = _require_link_item(link)
    documents = normalize_documents_list(item.get("documents"))
    current_rows = []
    archive_groups = []

    for document in documents:
        document_id = clean_cell_value(document.get("id"))
        series_id = clean_cell_value(document.get("seriesId")) or document_id
        mirror = resolve_document_mirror_status(document)
        mirror_status = clean_cell_value(mirror.get("status")).lower()
        current_rows.append({
            "documentId": document_id,
            "seriesId": series_id,
            "name": clean_cell_value(document.get("name")) or "Файл",
            "size": int(document.get("size") or 0),
            "uploadedAt": (
                clean_cell_value(document.get("uploadedAt"))
                or clean_cell_value(document.get("modifiedAt"))
            ),
            "uploadedByName": (
                clean_cell_value(document.get("uploadedByName")) or "—"
            ),
            "mirrorStatus": mirror_status,
            "replaceBlocked": mirror_status in {"queued", "running"},
            "requiresForceReplace": mirror_status == "error",
            "openUrl": _public_file_path(
                token,
                document_id,
                base_path=base_path,
            ),
            "downloadUrl": _public_file_path(
                token,
                document_id,
                base_path=base_path,
                download=True,
            ),
        })
        archive_group = _archive_payload(
            token=token,
            series_id=series_id,
            label=clean_cell_value(document.get("name")) or "Архив версий",
            versions=document.get("archiveVersions"),
            detached=False,
            base_path=base_path,
        )
        if archive_group:
            archive_groups.append(archive_group)

    for series in normalize_detached_archive_series(
        item.get("archivedDocumentSeries")
    ):
        archive_group = _archive_payload(
            token=token,
            series_id=clean_cell_value(series.get("seriesId")),
            label=(
                clean_cell_value(series.get("lastCurrentName"))
                or "Удалённая серия"
            ),
            versions=series.get("archiveVersions"),
            detached=True,
            base_path=base_path,
        )
        if archive_group:
            archive_groups.append(archive_group)

    return {
        "ok": True,
        "checklistTitle": clean_cell_value(data.get("title")) or "Чек-лист",
        "itemName": clean_cell_value(item.get("name")) or "Папка пункта",
        "currentFiles": current_rows,
        "archiveGroups": archive_groups,
        "permissions": {
            "view": True,
            "download": True,
            "upload": True,
            "replace": True,
            "archive": True,
            "delete": False,
        },
    }


def _secure_public_response(response):
    response.headers["Cache-Control"] = "private, no-store, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    return response


@router.get("/public/folder/{token}", response_class=HTMLResponse)
def public_folder_page(request: Request, token: str):
    link = resolve_public_folder_token(token)
    if not link:
        return _secure_public_response(HTMLResponse(
            "<h2>Ссылка недействительна или была перевыпущена</h2>",
            status_code=404,
        ))
    try:
        data, item = _require_link_item(link)
    except KeyError:
        return _secure_public_response(HTMLResponse(
            "<h2>Папка пункта больше недоступна</h2>",
            status_code=410,
        ))

    base_path = get_public_app_base_path(request)
    asset_version = "8.15.7-public-folder-dnd"
    response = HTMLResponse(render_ui_template(
        "public_folder.html",
        {
            "PUBLIC_FOLDER_PAGE_TITLE": html.escape(
                clean_cell_value(item.get("name")) or "Папка пункта"
            ),
            "PUBLIC_FOLDER_CSS_URL": html.escape(
                f"{base_path}/ui-static/css/public-folder.css?v={asset_version}"
            ),
            "PUBLIC_FOLDER_JS_URL": html.escape(
                f"{base_path}/ui-static/js/public-folder.js?v={asset_version}"
            ),
        },
    ))
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; script-src 'self'; style-src 'self'; "
        "img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; "
        "base-uri 'none'; form-action 'self'"
    )
    return _secure_public_response(response)


@router.get("/api/public-folder/{token}")
def api_public_folder_state(request: Request, token: str):
    link = resolve_public_folder_token(token)
    if not link:
        return _secure_public_response(JSONResponse(
            {"ok": False, "error": "Ссылка недействительна или была перевыпущена"},
            status_code=404,
        ))
    try:
        return _secure_public_response(JSONResponse(
            _public_folder_state(
                token,
                link,
                base_path=get_public_app_base_path(request),
            )
        ))
    except KeyError:
        return _secure_public_response(JSONResponse(
            {"ok": False, "error": "Папка пункта больше недоступна"},
            status_code=410,
        ))


async def _save_public_upload(file: UploadFile, target: Path) -> int:
    target = require_public_staging_path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    total = 0
    try:
        with target.open("xb") as output:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                output.write(chunk)
                total += len(chunk)
        if total <= 0:
            raise ValueError("Загружен пустой файл")
        return total
    except Exception:
        if target.exists():
            target.unlink()
        raise


async def _accept_public_file_operation(
    *,
    token: str,
    operation_type: str,
    file: UploadFile,
    first_name: str,
    last_name: str,
    document_id: str = "",
    force_replace: bool = False,
):
    link = resolve_public_folder_token(token)
    if not link:
        return _secure_public_response(JSONResponse(
            {"ok": False, "error": "Ссылка недействительна или была перевыпущена"},
            status_code=404,
        ))

    operation = None
    try:
        _require_link_item(link)
        operation = prepare_public_folder_operation(
            link=link,
            operation_type=operation_type,
            original_file_name=file.filename or "file.bin",
            first_name=first_name,
            last_name=last_name,
            expected_document_id=document_id,
            force_replace=force_replace,
        )
        operation_id = operation.get("operation_id") or ""
        target = require_public_staging_path(operation.get("staging_path") or "")
        size = await _save_public_upload(file, target)
        finalize_received_public_folder_operation(operation_id, size)
        result = process_public_folder_operation(operation_id)
        public_result = public_folder_operation_payload(result)
        status = clean_cell_value(result.get("status"))
        if status == "completed":
            http_status = 201
        elif status in {"conflict", "error"}:
            http_status = 409 if status == "conflict" else 500
        else:
            http_status = 202
        return _secure_public_response(JSONResponse({
            "ok": status not in {"conflict", "error"},
            "accepted": True,
            "operation": public_result,
        }, status_code=http_status))
    except PublicFolderOperationConflict as exc:
        return _secure_public_response(JSONResponse(
            {"ok": False, "error": str(exc), "conflict": True},
            status_code=409,
        ))
    except (ValueError, KeyError) as exc:
        if operation:
            fail_public_folder_operation_receiving(
                operation.get("operation_id") or "",
                str(exc),
            )
        return _secure_public_response(JSONResponse(
            {"ok": False, "error": str(exc).strip("'")},
            status_code=400,
        ))
    except Exception as exc:
        if operation:
            fail_public_folder_operation_receiving(
                operation.get("operation_id") or "",
                str(exc),
            )
        return _secure_public_response(JSONResponse(
            {"ok": False, "error": "Не удалось принять файл"},
            status_code=500,
        ))


@router.post("/api/public-folder/{token}/upload")
async def api_public_folder_upload(
    token: str,
    file: UploadFile = File(...),
    firstName: str = Form(""),
    lastName: str = Form(""),
):
    return await _accept_public_file_operation(
        token=token,
        operation_type="upload",
        file=file,
        first_name=firstName,
        last_name=lastName,
    )


@router.post("/api/public-folder/{token}/replace")
async def api_public_folder_replace(
    token: str,
    file: UploadFile = File(...),
    documentId: str = Form(...),
    firstName: str = Form(""),
    lastName: str = Form(""),
    forceReplace: str = Form(""),
):
    return await _accept_public_file_operation(
        token=token,
        operation_type="replace",
        file=file,
        first_name=firstName,
        last_name=lastName,
        document_id=documentId,
        force_replace=clean_cell_value(forceReplace).lower()
        in {"1", "true", "yes", "да"},
    )


@router.get("/api/public-folder/{token}/operations/{operation_id}")
def api_public_folder_operation_status(token: str, operation_id: str):
    link = resolve_public_folder_token(token)
    if not link:
        return _secure_public_response(JSONResponse(
            {"ok": False, "error": "Ссылка недействительна или была перевыпущена"},
            status_code=404,
        ))
    operation = get_public_folder_operation(operation_id)
    if (
        not operation
        or clean_cell_value(operation.get("link_id"))
        != clean_cell_value(link.get("link_id"))
        or int(operation.get("link_generation") or 0)
        != int(link.get("generation") or 0)
    ):
        return _secure_public_response(JSONResponse(
            {"ok": False, "error": "Операция не найдена"},
            status_code=404,
        ))
    return _secure_public_response(JSONResponse({
        "ok": True,
        "operation": public_folder_operation_payload(operation),
    }))


@router.get("/public/folder/{token}/file/{version_id}")
def public_folder_file(
    token: str,
    version_id: str,
    download: int = 0,
):
    link = resolve_public_folder_token(token)
    if not link:
        return _secure_public_response(JSONResponse(
            {"ok": False, "error": "Ссылка недействительна или была перевыпущена"},
            status_code=404,
        ))
    try:
        _, item = _require_link_item(link)
    except KeyError:
        return _secure_public_response(JSONResponse(
            {"ok": False, "error": "Папка пункта больше недоступна"},
            status_code=410,
        ))

    version = find_pinned_version_in_item(item, version_id)
    if not version:
        return _secure_public_response(JSONResponse(
            {"ok": False, "error": "Версия файла не найдена"},
            status_code=404,
        ))
    file_path = resolve_pinned_version_local_path(version)
    if not file_path or not file_path.is_file():
        return _secure_public_response(JSONResponse(
            {"ok": False, "error": "Файл версии больше недоступен"},
            status_code=410,
        ))

    filename = version.file_name or file_path.name
    media_type, _ = mimetypes.guess_type(str(file_path))
    media_type = media_type or "application/octet-stream"
    disposition = (
        "attachment"
        if download or not can_preview_in_browser(filename, media_type)
        else "inline"
    )
    response = FileResponse(path=str(file_path), media_type=media_type)
    response.headers["Content-Disposition"] = (
        f"{disposition}; filename*=UTF-8''{quote(filename)}"
    )
    return _secure_public_response(response)
