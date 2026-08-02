from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from urllib.parse import quote, urlencode

from app.settings import (
    APP_BASE_PATH,
    PUBLIC_APP_BASE_URL,
)

from app.checklists.documents import (
    get_upload_file_path_from_url,
    migrate_legacy_document_fields,
    normalize_archive_versions,
    normalize_detached_archive_series,
    normalize_documents_list,
)

from app.checklists.utils import (
    clean_cell_value,
    normalize_dialog_id,
    normalize_checklist_key,
)


VERSION_FILE_ROUTE = "/api/checklist/version-file"
VERSION_LINK_ROUTE = "/api/checklist/version-link"


@dataclass(frozen=True)
class PinnedDocumentVersion:
    version_id: str
    series_id: str
    document_id: str
    archive_version_id: str
    location: str
    file_name: str
    file_url: str
    size: int
    uploaded_at: str
    uploaded_by_id: str
    uploaded_by_name: str

    def as_dict(self) -> dict:
        return {
            "versionId": self.version_id,
            "seriesId": self.series_id,
            "documentId": self.document_id,
            "archiveVersionId": self.archive_version_id,
            "location": self.location,
            "fileName": self.file_name,
            "fileUrl": self.file_url,
            "size": self.size,
            "uploadedAt": self.uploaded_at,
            "uploadedById": self.uploaded_by_id,
            "uploadedByName": self.uploaded_by_name,
        }


def normalize_base_path(value: str) -> str:
    value = clean_cell_value(value)

    if not value or value == "/":
        return ""

    if not value.startswith("/"):
        value = "/" + value

    return value.rstrip("/")


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return default


def get_current_version_id(document: dict) -> str:
    """
    Возвращает ID именно текущей физической версии.

    seriesId здесь намеренно не используется: одна логическая серия может
    содержать несколько физических версий, а ссылка должна закрепляться за
    той версией, которая была актуальна в момент формирования сообщения.
    """
    return clean_cell_value((document or {}).get("id"))


def get_archive_pinned_version_id(archive_version: dict) -> str:
    """
    После замены прежний documentId сохраняется в originalDocumentId.
    Для legacy-архива без этого поля используем собственный archive id.
    """
    archive_version = archive_version or {}
    return (
        clean_cell_value(
            archive_version.get("originalDocumentId")
        )
        or clean_cell_value(archive_version.get("id"))
    )


def _current_version_record(document: dict) -> PinnedDocumentVersion | None:
    version_id = get_current_version_id(document)

    if not version_id:
        return None

    file_url = (
        clean_cell_value(document.get("fileUrl"))
        or clean_cell_value(document.get("path"))
        or clean_cell_value(document.get("previewUrl"))
    )

    return PinnedDocumentVersion(
        version_id=version_id,
        series_id=clean_cell_value(document.get("seriesId")),
        document_id=version_id,
        archive_version_id="",
        location="current_document",
        file_name=(
            clean_cell_value(document.get("name"))
            or "document.bin"
        ),
        file_url=file_url,
        size=_safe_int(document.get("size")),
        uploaded_at=(
            clean_cell_value(document.get("uploadedAt"))
            or clean_cell_value(document.get("modifiedAt"))
        ),
        uploaded_by_id=clean_cell_value(
            document.get("uploadedById")
        ),
        uploaded_by_name=clean_cell_value(
            document.get("uploadedByName")
        ),
    )


def _archive_version_record(
    archive_version: dict,
    *,
    location: str,
) -> PinnedDocumentVersion | None:
    version_id = get_archive_pinned_version_id(
        archive_version
    )

    if not version_id:
        return None

    file_url = (
        clean_cell_value(archive_version.get("fileUrl"))
        or clean_cell_value(archive_version.get("path"))
        or clean_cell_value(
            archive_version.get("previewUrl")
        )
    )

    # Для ссылки, созданной на текущую версию до её замены, сохраняем имя,
    # которое файл имел в момент отправки. Архивный суффикс _vN остаётся
    # внутренним именем хранения и не меняет старое сообщение Bitrix.
    file_name = (
        clean_cell_value(
            archive_version.get("originalName")
        )
        or clean_cell_value(archive_version.get("name"))
        or "document.bin"
    )

    return PinnedDocumentVersion(
        version_id=version_id,
        series_id=clean_cell_value(
            archive_version.get("seriesId")
        ),
        document_id=clean_cell_value(
            archive_version.get("originalDocumentId")
        ),
        archive_version_id=clean_cell_value(
            archive_version.get("id")
        ),
        location=location,
        file_name=file_name,
        file_url=file_url,
        size=_safe_int(archive_version.get("size")),
        uploaded_at=(
            clean_cell_value(
                archive_version.get("uploadedAt")
            )
            or clean_cell_value(
                archive_version.get("archivedAt")
            )
        ),
        uploaded_by_id=clean_cell_value(
            archive_version.get("uploadedById")
        ),
        uploaded_by_name=clean_cell_value(
            archive_version.get("uploadedByName")
        ),
    )


def find_pinned_version_in_item(
    raw_item: dict,
    version_id: str,
) -> PinnedDocumentVersion | None:
    """
    Находит строго одну физическую версию внутри пункта.

    Поиск не использует seriesId как замену versionId и поэтому никогда не
    переадресует старую ссылку на новую текущую версию той же серии.
    """
    version_id = clean_cell_value(version_id)

    if not version_id:
        return None

    item = migrate_legacy_document_fields(
        deepcopy(raw_item or {})
    )

    for document in normalize_documents_list(
        item.get("documents")
    ):
        current_record = _current_version_record(document)

        if (
            current_record
            and current_record.version_id == version_id
        ):
            return current_record

        series_id = clean_cell_value(
            document.get("seriesId")
        )

        for archive_version in normalize_archive_versions(
            document.get("archiveVersions"),
            series_id=series_id,
        ):
            archive_record = _archive_version_record(
                archive_version,
                location="current_document_archive",
            )

            if (
                archive_record
                and archive_record.version_id == version_id
            ):
                return archive_record

    for series in normalize_detached_archive_series(
        item.get("archivedDocumentSeries")
    ):
        series_id = clean_cell_value(series.get("seriesId"))

        for archive_version in normalize_archive_versions(
            series.get("archiveVersions"),
            series_id=series_id,
        ):
            archive_record = _archive_version_record(
                archive_version,
                location="detached_archive_series",
            )

            if (
                archive_record
                and archive_record.version_id == version_id
            ):
                return archive_record

    return None


def find_pinned_version_in_checklist(
    checklist_data: dict,
    item_id: str,
    version_id: str,
) -> tuple[dict | None, PinnedDocumentVersion | None]:
    item_id = clean_cell_value(item_id)

    for raw_item in (checklist_data or {}).get("items", []) or []:
        if clean_cell_value(raw_item.get("id")) != item_id:
            continue

        return (
            raw_item,
            find_pinned_version_in_item(
                raw_item,
                version_id,
            ),
        )

    return None, None


def resolve_pinned_version_local_path(
    version: PinnedDocumentVersion,
):
    return get_upload_file_path_from_url(
        version.file_url
    )


def build_version_file_route(
    dialog_id: str,
    checklist_key: str,
    item_id: str,
    version_id: str,
    *,
    download: bool = False,
) -> str:
    query = {
        "dialogId": normalize_dialog_id(dialog_id),
        "checklistKey": normalize_checklist_key(
            checklist_key
        ),
        "itemId": clean_cell_value(item_id),
        "versionId": clean_cell_value(version_id),
    }

    if download:
        query["download"] = "1"

    return f"{VERSION_FILE_ROUTE}?{urlencode(query, quote_via=quote)}"


def build_configured_version_file_url(
    dialog_id: str,
    checklist_key: str,
    item_id: str,
    version_id: str,
    *,
    download: bool = False,
) -> str:
    route = build_version_file_route(
        dialog_id,
        checklist_key,
        item_id,
        version_id,
        download=download,
    )

    public_base = clean_cell_value(PUBLIC_APP_BASE_URL).rstrip("/")

    if public_base:
        return public_base + route

    return normalize_base_path(APP_BASE_PATH) + route
