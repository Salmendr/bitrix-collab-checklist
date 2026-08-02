import re
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote, urlparse

from app.settings import APP_BASE_PATH, UPLOAD_ROOT
from app.logging_utils import write_debug_log
from app.yandex_disk.client import (
    is_yandex_disk_enabled,
    yandex_disk_delete_path,
)

from app.checklists.utils import (
    clean_cell_value,
    normalize_dialog_id,
    normalize_checklist_key,
)


def normalize_base_path(value: str) -> str:
    value = str(value or "").strip()
    if not value or value == "/":
        return ""
    if not value.startswith("/"):
        value = "/" + value
    return value.rstrip("/")


def build_upload_rel_path(dialog_id: str, item_id: str, filename: str) -> str:
    dialog_id = normalize_dialog_id(dialog_id)
    safe_name = Path(filename or "file.bin").name
    unique_name = f"{uuid.uuid4().hex}_{safe_name}"
    return f"checklists/{dialog_id}/{item_id}/{unique_name}"


def remove_item_document_file(item: dict):
    doc_url = str(item.get("documentUrl") or "").strip()
    if not doc_url.startswith("/uploads/"):
        return

    rel_path = doc_url.replace("/uploads/", "", 1)
    file_path = UPLOAD_ROOT / rel_path

    try:
        if file_path.exists():
            file_path.unlink()
    except Exception:
        pass


DOCUMENT_VERSIONING_MODEL_VERSION = 1
ARCHIVE_FILENAME_MAX_LENGTH = 180


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def safe_positive_int(value, default: int = 0) -> int:
    try:
        result = int(value or 0)
    except (TypeError, ValueError):
        result = int(default or 0)

    return result if result > 0 else int(default or 0)


def sanitize_archive_filename(value: str) -> str:
    safe_name = Path(value or "file.bin").name.strip()
    safe_name = re.sub(r'[<>:"/\\|?*]+', "_", safe_name)
    safe_name = re.sub(r"\s+", " ", safe_name).strip()
    safe_name = safe_name.rstrip(". ")

    return safe_name or "file.bin"


def build_versioned_archive_filename(
    filename: str,
    version: int,
    max_length: int = ARCHIVE_FILENAME_MAX_LENGTH,
) -> str:
    safe_name = sanitize_archive_filename(filename)
    version = max(1, safe_positive_int(version, 1))

    path_name = Path(safe_name)
    suffixes = "".join(path_name.suffixes)

    if suffixes and safe_name.endswith(suffixes):
        stem = safe_name[:-len(suffixes)]
    else:
        stem = safe_name
        suffixes = ""

    version_suffix = f"_v{version}"

    max_length = max(32, int(max_length or ARCHIVE_FILENAME_MAX_LENGTH))
    max_stem_length = max(
        1,
        max_length - len(version_suffix) - len(suffixes),
    )

    safe_stem = stem[:max_stem_length].rstrip(" ._")
    if not safe_stem:
        safe_stem = "file"

    return f"{safe_stem}{version_suffix}{suffixes}"


def sanitize_archive_path_part(value: str, fallback: str) -> str:
    normalized = re.sub(
        r"[^a-zA-Z0-9_-]+",
        "_",
        str(value or "").strip(),
    ).strip("_")

    return normalized or fallback


def build_archive_rel_path(
    dialog_id: str,
    item_id: str,
    series_id: str,
    archive_filename: str,
) -> str:
    dialog_id = normalize_dialog_id(dialog_id)

    safe_item_id = sanitize_archive_path_part(item_id, "item")
    safe_series_id = sanitize_archive_path_part(series_id, uuid.uuid4().hex)
    safe_filename = sanitize_archive_filename(archive_filename)

    return (
        f"checklists/{dialog_id}/{safe_item_id}/"
        f"archive/{safe_series_id}/{safe_filename}"
    )


def normalize_archive_version_record(
    raw_version: dict,
    series_id: str = "",
) -> dict:
    raw_version = dict(raw_version or {})

    version = safe_positive_int(raw_version.get("version"), 0)

    if not version:
        version_label = clean_cell_value(raw_version.get("versionLabel"))
        match = re.search(r"v(\d+)", version_label, flags=re.IGNORECASE)

        if not match:
            match = re.search(
                r"_v(\d+)(?:\.[^.]+(?:\.[^.]+)*)?$",
                clean_cell_value(raw_version.get("name")),
                flags=re.IGNORECASE,
            )

        if match:
            version = safe_positive_int(match.group(1), 0)

    file_url = clean_cell_value(
        raw_version.get("fileUrl")
        or raw_version.get("url")
        or raw_version.get("path")
    )

    archive_name = clean_cell_value(
        raw_version.get("name")
        or raw_version.get("archiveName")
    )

    original_name = clean_cell_value(
        raw_version.get("originalName")
        or raw_version.get("sourceName")
        or archive_name
    )

    normalized_series_id = (
        clean_cell_value(raw_version.get("seriesId"))
        or clean_cell_value(series_id)
    )

    return {
        "id": clean_cell_value(raw_version.get("id")) or uuid.uuid4().hex,
        "seriesId": normalized_series_id,
        "originalDocumentId": clean_cell_value(
            raw_version.get("originalDocumentId")
            or raw_version.get("documentId")
        ),
        "version": version,
        "versionLabel": f"v{version}" if version else "",
        "name": archive_name,
        "originalName": original_name,
        "path": clean_cell_value(raw_version.get("path")) or file_url,
        "fileUrl": file_url,
        "previewUrl": clean_cell_value(
            raw_version.get("previewUrl")
            or file_url
        ),
        "size": int(raw_version.get("size") or 0),
        "uploadedAt": clean_cell_value(
            raw_version.get("uploadedAt")
            or raw_version.get("modifiedAt")
        ),
        "uploadedById": clean_cell_value(
            raw_version.get("uploadedById")
            or raw_version.get("uploadedUserId")
            or raw_version.get("createdById")
        ),
        "uploadedByName": clean_cell_value(
            raw_version.get("uploadedByName")
            or raw_version.get("uploadedBy")
            or raw_version.get("createdByName")
        ),
        "archivedAt": clean_cell_value(raw_version.get("archivedAt")),
        "archivedById": clean_cell_value(raw_version.get("archivedById")),
        "archivedByName": clean_cell_value(raw_version.get("archivedByName")),
        "source": clean_cell_value(raw_version.get("source")) or "local_archive",

        # Сохраняем исходный путь Яндекс.Диска до завершения удаления.
        "originalYandexPath": clean_cell_value(
            raw_version.get("originalYandexPath")
            or raw_version.get("yandexPath")
        ),
        "yandexDeleteStatus": clean_cell_value(
            raw_version.get("yandexDeleteStatus")
        ),
        "yandexDeleteJobId": clean_cell_value(
            raw_version.get("yandexDeleteJobId")
        ),
        "yandexDeleteError": clean_cell_value(
            raw_version.get("yandexDeleteError")
        ),
    }


def normalize_archive_versions(
    value,
    series_id: str = "",
) -> list[dict]:
    if not isinstance(value, list):
        return []

    result = []
    used_versions = set()

    for raw_version in value:
        normalized = normalize_archive_version_record(
            raw_version,
            series_id=series_id,
        )

        version = safe_positive_int(normalized.get("version"), 0)

        if not version or version in used_versions:
            version = 1
            while version in used_versions:
                version += 1

            normalized["version"] = version
            normalized["versionLabel"] = f"v{version}"

        if not normalized.get("seriesId"):
            normalized["seriesId"] = clean_cell_value(series_id)

        if not normalized.get("name"):
            normalized["name"] = build_versioned_archive_filename(
                normalized.get("originalName") or "file.bin",
                version,
            )

        used_versions.add(version)

        if not normalized.get("fileUrl") and not normalized.get("name"):
            continue

        result.append(normalized)

    result.sort(
        key=lambda item: safe_positive_int(item.get("version"), 0)
    )

    return result


def get_next_archive_version_number(document: dict) -> int:
    document = dict(document or {})

    versions = normalize_archive_versions(
        document.get("archiveVersions"),
        series_id=clean_cell_value(document.get("seriesId")),
    )

    highest_version = max(
        [
            safe_positive_int(version.get("version"), 0)
            for version in versions
        ],
        default=0,
    )

    return highest_version + 1


def archive_current_document_local_file(
    dialog_id: str,
    item_id: str,
    document: dict,
    archived_by_id: str = "",
    archived_by_name: str = "",
    archived_at: str = "",
) -> dict:
    current_document = normalize_document_record(document)

    series_id = (
        clean_cell_value(current_document.get("seriesId"))
        or clean_cell_value(current_document.get("id"))
        or uuid.uuid4().hex
    )

    source_url = (
        clean_cell_value(current_document.get("fileUrl"))
        or clean_cell_value(current_document.get("path"))
    )

    source_path = get_upload_file_path_from_url(source_url)

    if source_path is None:
        raise RuntimeError(
            "Текущий файл не расположен в локальном хранилище /uploads"
        )

    if not source_path.exists():
        raise RuntimeError(
            f"Текущий локальный файл не найден: {source_path}"
        )

    version = get_next_archive_version_number(current_document)

    archive_filename = build_versioned_archive_filename(
        current_document.get("name") or source_path.name,
        version,
    )

    archive_rel_path = build_archive_rel_path(
        dialog_id=dialog_id,
        item_id=item_id,
        series_id=series_id,
        archive_filename=archive_filename,
    )

    archive_abs_path = UPLOAD_ROOT / archive_rel_path
    archive_abs_path.parent.mkdir(parents=True, exist_ok=True)

    if archive_abs_path.exists():
        raise RuntimeError(
            f"Архивный файл уже существует: {archive_abs_path}"
        )

    shutil.move(str(source_path), str(archive_abs_path))

    archive_file_url = (
        "/uploads/" + archive_rel_path.replace("\\", "/")
    )

    original_yandex_path = clean_cell_value(
        current_document.get("yandexPath")
    )

    return normalize_archive_version_record({
        "id": uuid.uuid4().hex,
        "seriesId": series_id,
        "originalDocumentId": clean_cell_value(
            current_document.get("id")
        ),
        "version": version,
        "versionLabel": f"v{version}",
        "name": archive_filename,
        "originalName": clean_cell_value(
            current_document.get("name")
        ) or source_path.name,
        "path": archive_file_url,
        "fileUrl": archive_file_url,
        "previewUrl": archive_file_url,
        "size": archive_abs_path.stat().st_size,
        "uploadedAt": clean_cell_value(
            current_document.get("uploadedAt")
            or current_document.get("modifiedAt")
        ),
        "uploadedById": clean_cell_value(
            current_document.get("uploadedById")
        ),
        "uploadedByName": clean_cell_value(
            current_document.get("uploadedByName")
        ),
        "archivedAt": clean_cell_value(archived_at) or utc_now_iso(),
        "archivedById": clean_cell_value(archived_by_id),
        "archivedByName": clean_cell_value(archived_by_name),
        "source": "local_archive",
        "originalYandexPath": original_yandex_path,
        "yandexDeleteStatus": (
            "pending_after_replacement_sync"
            if original_yandex_path
            else "not_required"
        ),
        "yandexDeleteJobId": "",
        "yandexDeleteError": "",
    }, series_id=series_id)


def normalize_document_record(doc: dict) -> dict:
    doc = dict(doc or {})

    file_url = clean_cell_value(
        doc.get("fileUrl")
        or doc.get("url")
        or doc.get("documentUrl")
    )
    preview_url = clean_cell_value(
        doc.get("previewUrl")
        or file_url
    )
    path = clean_cell_value(
        doc.get("path")
        or file_url
    )
    name = clean_cell_value(
        doc.get("name")
        or doc.get("documentName")
    )

    if not name and file_url:
        try:
            name = Path(urlparse(file_url).path).name
        except Exception:
            name = ""

    document_id = (
        clean_cell_value(doc.get("id"))
        or uuid.uuid4().hex
    )

    # Для старых документов ID текущего документа становится seriesId.
    # При дальнейшей замене documentId изменится, seriesId останется прежним.
    series_id = (
        clean_cell_value(doc.get("seriesId"))
        or clean_cell_value(doc.get("series_id"))
        or document_id
    )

    uploaded_at = (
        clean_cell_value(doc.get("uploadedAt"))
        or clean_cell_value(doc.get("modifiedAt"))
    )
    uploaded_by_id = clean_cell_value(
        doc.get("uploadedById")
        or doc.get("uploadedUserId")
        or doc.get("createdById")
    )
    uploaded_by_name = clean_cell_value(
        doc.get("uploadedByName")
        or doc.get("uploadedBy")
        or doc.get("createdByName")
    )

    archive_versions = normalize_archive_versions(
        doc.get("archiveVersions")
        or doc.get("versions")
        or [],
        series_id=series_id,
    )

    return {
        "id": document_id,
        "seriesId": series_id,
        "versioningModel": DOCUMENT_VERSIONING_MODEL_VERSION,

        "name": name,
        "path": path,
        "fileUrl": file_url,
        "previewUrl": preview_url,
        "size": int(doc.get("size") or 0),

        "modifiedAt": (
            clean_cell_value(doc.get("modifiedAt"))
            or uploaded_at
        ),
        "uploadedAt": uploaded_at,
        "uploadedById": uploaded_by_id,
        "uploadedByName": uploaded_by_name,
        "source": clean_cell_value(doc.get("source")) or "local",

        "archiveVersions": archive_versions,

        "lastReplacedAt": clean_cell_value(
            doc.get("lastReplacedAt")
        ),
        "lastReplacedById": clean_cell_value(
            doc.get("lastReplacedById")
        ),
        "lastReplacedByName": clean_cell_value(
            doc.get("lastReplacedByName")
        ),
        "replacementOperationId": clean_cell_value(
            doc.get("replacementOperationId")
        ),

        "mirrorStatus": clean_cell_value(
            doc.get("mirrorStatus")
        ),
        "mirrorError": clean_cell_value(
            doc.get("mirrorError")
        ),
        "mirrorJobId": clean_cell_value(
            doc.get("mirrorJobId")
        ),
        "yandexPath": clean_cell_value(
            doc.get("yandexPath")
        ),
        "yandexFileUrl": clean_cell_value(
            doc.get("yandexFileUrl")
        ),
        "yandexFolderAlias": clean_cell_value(
            doc.get("yandexFolderAlias")
        ),
    }


def normalize_documents_list(value) -> list[dict]:
    if not isinstance(value, list):
        return []

    result = []
    for raw_doc in value:
        normalized = normalize_document_record(raw_doc)
        if not normalized.get("name") and not normalized.get("fileUrl"):
            continue
        result.append(normalized)

    # Порядок документов является частью пользовательского интерфейса.
    # Не сортируем список по имени: новая загрузка остаётся в конце,
    # а новая версия при замене сохраняет индекс прежнего документа.
    return result


def migrate_legacy_document_fields(item: dict) -> dict:
    item = dict(item or {})

    documents = item.get("documents")
    if isinstance(documents, list):
        item["documents"] = normalize_documents_list(documents)
        return item

    legacy_url = clean_cell_value(item.get("documentUrl"))
    legacy_name = clean_cell_value(item.get("documentName"))

    if legacy_url or legacy_name:
        item["documents"] = normalize_documents_list([
            {
                "id": uuid.uuid4().hex,
                "name": legacy_name,
                "path": legacy_url,
                "fileUrl": legacy_url,
                "previewUrl": legacy_url,
                "size": 0,
                "modifiedAt": "",
                "source": "local",
            }
        ])
    else:
        item["documents"] = []

    return item



def normalize_detached_archive_series_record(raw_series: dict) -> dict | None:
    raw_series = dict(raw_series or {})

    series_id = clean_cell_value(
        raw_series.get("seriesId")
        or raw_series.get("series_id")
    )

    archive_versions = normalize_archive_versions(
        raw_series.get("archiveVersions") or [],
        series_id=series_id,
    )

    if not series_id and archive_versions:
        series_id = clean_cell_value(
            archive_versions[0].get("seriesId")
        )

    if not series_id or not archive_versions:
        return None

    return {
        "seriesId": series_id,
        "lastCurrentDocumentId": clean_cell_value(
            raw_series.get("lastCurrentDocumentId")
            or raw_series.get("documentId")
        ),
        "lastCurrentName": clean_cell_value(
            raw_series.get("lastCurrentName")
            or raw_series.get("documentName")
        ),
        "removedAt": clean_cell_value(
            raw_series.get("removedAt")
        ),
        "removedById": clean_cell_value(
            raw_series.get("removedById")
        ),
        "removedByName": clean_cell_value(
            raw_series.get("removedByName")
        ),
        "archiveVersions": archive_versions,
    }


def normalize_detached_archive_series(value) -> list[dict]:
    if not isinstance(value, list):
        return []

    result = []

    for raw_series in value:
        normalized = normalize_detached_archive_series_record(
            raw_series
        )

        if normalized:
            result.append(normalized)

    result.sort(
        key=lambda item: (
            clean_cell_value(item.get("lastCurrentName")).lower(),
            clean_cell_value(item.get("seriesId")),
        )
    )

    return result


def build_detached_archive_series(
    document: dict,
    removed_by_id: str = "",
    removed_by_name: str = "",
    removed_at: str = "",
) -> dict | None:
    document = normalize_document_record(document)

    archive_versions = normalize_archive_versions(
        document.get("archiveVersions"),
        series_id=clean_cell_value(document.get("seriesId")),
    )

    if not archive_versions:
        return None

    return normalize_detached_archive_series_record({
        "seriesId": clean_cell_value(document.get("seriesId")),
        "lastCurrentDocumentId": clean_cell_value(
            document.get("id")
        ),
        "lastCurrentName": clean_cell_value(
            document.get("name")
        ),
        "removedAt": clean_cell_value(removed_at) or utc_now_iso(),
        "removedById": clean_cell_value(removed_by_id),
        "removedByName": clean_cell_value(removed_by_name),
        "archiveVersions": archive_versions,
    })


def merge_detached_archive_series(
    existing_value,
    added_value,
) -> list[dict]:
    existing = normalize_detached_archive_series(existing_value)
    added = normalize_detached_archive_series(added_value)

    result_by_series_id = {}

    for raw_series in existing + added:
        series = normalize_detached_archive_series_record(raw_series)

        if not series:
            continue

        series_id = clean_cell_value(series.get("seriesId"))

        if series_id not in result_by_series_id:
            result_by_series_id[series_id] = series
            continue

        current = result_by_series_id[series_id]

        unique_versions = []
        seen_version_keys = set()

        for version in (
            current.get("archiveVersions", [])
            + series.get("archiveVersions", [])
        ):
            version_id = clean_cell_value(version.get("id"))
            version_url = clean_cell_value(version.get("fileUrl"))
            version_number = int(version.get("version") or 0)

            identity = (
                version_id
                or version_url
                or f"version:{version_number}"
            )

            if identity in seen_version_keys:
                continue

            seen_version_keys.add(identity)
            unique_versions.append(version)

        result_by_series_id[series_id] = {
            **current,
            **series,
            "archiveVersions": normalize_archive_versions(
                unique_versions,
                series_id=series_id,
            ),
        }

    return normalize_detached_archive_series(
        list(result_by_series_id.values())
    )


def remove_all_item_documents(
    dialog_id: str,
    checklist_key: str,
    item_id: str,
    item: dict,
) -> dict:
    cleaned_item = migrate_legacy_document_fields(
        dict(item or {})
    )

    documents = normalize_documents_list(
        cleaned_item.get("documents")
    )

    detached_archive_series = normalize_detached_archive_series(
        cleaned_item.get("archivedDocumentSeries")
    )

    detached_additions = []

    for doc in documents:
        detached_series = build_detached_archive_series(doc)

        if detached_series:
            detached_additions.append(detached_series)

        local_document_url = (
            clean_cell_value(doc.get("fileUrl"))
            or clean_cell_value(doc.get("previewUrl"))
            or clean_cell_value(doc.get("path"))
        )

        remove_item_document_file({
            "documentUrl": local_document_url
        })

    cleaned_item["archivedDocumentSeries"] = (
        merge_detached_archive_series(
            detached_archive_series,
            detached_additions,
        )
    )

    cleaned_item["documents"] = []
    cleaned_item["documentUrl"] = ""
    cleaned_item["documentName"] = ""
    cleaned_item["folderPath"] = ""
    cleaned_item["folderUrl"] = ""

    return cleaned_item


def build_folder_view_url(dialog_id: str, checklist_key: str, item_id: str) -> str:
    dialog_id = normalize_dialog_id(dialog_id)
    checklist_key = normalize_checklist_key(checklist_key)
    item_id = str(item_id or "").strip()
    base_path = normalize_base_path(APP_BASE_PATH)

    return (
        f"{base_path}/api/checklist/folder"
        f"?dialogId={quote(dialog_id)}"
        f"&checklistKey={quote(checklist_key)}"
        f"&itemId={quote(item_id)}"
    )


def build_document_view_url(dialog_id: str, checklist_key: str, item_id: str, document_id: str) -> str:
    dialog_id = normalize_dialog_id(dialog_id)
    checklist_key = normalize_checklist_key(checklist_key)
    item_id = str(item_id or "").strip()
    document_id = str(document_id or "").strip()
    base_path = normalize_base_path(APP_BASE_PATH)

    return (
        f"{base_path}/api/checklist/file"
        f"?dialogId={quote(dialog_id)}"
        f"&checklistKey={quote(checklist_key)}"
        f"&itemId={quote(item_id)}"
        f"&documentId={quote(document_id)}"
    )


def get_upload_file_path_from_url(document_url: str):
    document_url = clean_cell_value(document_url)
    if not document_url.startswith("/uploads/"):
        return None

    rel_path = document_url.replace("/uploads/", "", 1)
    return UPLOAD_ROOT / rel_path