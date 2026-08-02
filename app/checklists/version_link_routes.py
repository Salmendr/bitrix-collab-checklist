from __future__ import annotations

import mimetypes
from urllib.parse import quote

from fastapi import APIRouter, Request
from fastapi.responses import FileResponse, JSONResponse

from app.checklists.document_version_links import (
    build_version_file_route,
    find_pinned_version_in_checklist,
    resolve_pinned_version_local_path,
)
from app.checklists.storage import get_checklist
from app.checklists.utils import (
    can_preview_in_browser,
    clean_cell_value,
    normalize_dialog_id,
    normalize_checklist_key,
)
from app.ui.shell import (
    get_public_app_base_path,
    get_public_app_base_url,
)


router = APIRouter()


def _resolve_requested_version(
    dialog_id: str,
    checklist_key: str,
    item_id: str,
    version_id: str,
):
    checklist_data = get_checklist(
        dialog_id,
        checklist_key,
    )

    item, version = find_pinned_version_in_checklist(
        checklist_data,
        item_id,
        version_id,
    )

    return checklist_data, item, version


@router.get("/api/checklist/version-link")
def api_checklist_version_link(
    request: Request,
    dialogId: str = "",
    itemId: str = "",
    versionId: str = "",
    checklistKey: str = "id",
):
    """
    Возвращает стабильный URL конкретной физической версии.

    Endpoint предназначен для будущего backend оповещений. Отдельная кнопка
    копирования ссылки во frontend на этапе 8.5.2 не добавляется.
    """
    dialog_id = normalize_dialog_id(dialogId)
    checklist_key = normalize_checklist_key(
        checklistKey
    )
    item_id = clean_cell_value(itemId)
    version_id = clean_cell_value(versionId)

    if not dialog_id or not item_id or not version_id:
        return JSONResponse({
            "ok": False,
            "error": (
                "dialogId, itemId and versionId are required"
            ),
        }, status_code=400)

    _, item, version = _resolve_requested_version(
        dialog_id,
        checklist_key,
        item_id,
        version_id,
    )

    if item is None:
        return JSONResponse({
            "ok": False,
            "error": "item not found",
        }, status_code=404)

    if version is None:
        return JSONResponse({
            "ok": False,
            "error": "document version not found",
        }, status_code=404)

    route = build_version_file_route(
        dialog_id,
        checklist_key,
        item_id,
        version.version_id,
    )

    public_base_url = get_public_app_base_url(
        request
    ).rstrip("/")
    public_base_path = get_public_app_base_path(
        request
    )

    return JSONResponse({
        "ok": True,
        "dialogId": dialog_id,
        "checklistKey": checklist_key,
        "itemId": item_id,
        "itemName": clean_cell_value(item.get("name")),
        "version": version.as_dict(),
        "url": public_base_url + route,
        "relativeUrl": public_base_path + route,
        "immutable": True,
        "expiresAt": None,
    })


@router.get("/api/checklist/version-file")
def api_checklist_version_file(
    dialogId: str = "",
    itemId: str = "",
    versionId: str = "",
    checklistKey: str = "id",
    download: int = 0,
):
    """
    Открывает строго указанную физическую версию.

    После замены текущего файла старая ссылка разрешается через
    archiveVersion.originalDocumentId. Переход на новую текущую версию той же
    seriesId намеренно запрещён.
    """
    dialog_id = normalize_dialog_id(dialogId)
    checklist_key = normalize_checklist_key(
        checklistKey
    )
    item_id = clean_cell_value(itemId)
    version_id = clean_cell_value(versionId)

    if not dialog_id or not item_id or not version_id:
        return JSONResponse({
            "ok": False,
            "error": (
                "dialogId, itemId and versionId are required"
            ),
        }, status_code=400)

    _, item, version = _resolve_requested_version(
        dialog_id,
        checklist_key,
        item_id,
        version_id,
    )

    if item is None:
        return JSONResponse({
            "ok": False,
            "error": "item not found",
        }, status_code=404)

    if version is None:
        return JSONResponse({
            "ok": False,
            "error": "document version not found",
        }, status_code=404)

    file_path = resolve_pinned_version_local_path(
        version
    )

    if not file_path or not file_path.exists():
        # Метаданные версии существуют, но физический файл удалён.
        # Возвращаем Gone и никогда не подменяем его новой версией серии.
        return JSONResponse({
            "ok": False,
            "error": "document version file is gone",
            "versionId": version.version_id,
            "location": version.location,
        }, status_code=410)

    filename = version.file_name or file_path.name
    media_type, _ = mimetypes.guess_type(
        str(file_path)
    )
    media_type = media_type or "application/octet-stream"

    inline_allowed = can_preview_in_browser(
        filename,
        media_type,
    )
    disposition = (
        "attachment"
        if download
        else ("inline" if inline_allowed else "attachment")
    )

    response = FileResponse(
        path=str(file_path),
        media_type=media_type,
    )
    response.headers["Content-Disposition"] = (
        f"{disposition}; filename*=UTF-8''{quote(filename)}"
    )
    response.headers["Cache-Control"] = "private, no-store"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Checklist-Version-Id"] = (
        version.version_id
    )
    response.headers["X-Checklist-Version-Location"] = (
        version.location
    )

    return response
