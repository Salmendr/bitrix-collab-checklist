import uuid
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


def normalize_document_record(doc: dict) -> dict:
    doc = dict(doc or {})

    file_url = clean_cell_value(doc.get("fileUrl") or doc.get("url") or doc.get("documentUrl"))
    preview_url = clean_cell_value(doc.get("previewUrl") or file_url)
    path = clean_cell_value(doc.get("path") or file_url)
    name = clean_cell_value(doc.get("name") or doc.get("documentName"))

    if not name and file_url:
        try:
            name = Path(urlparse(file_url).path).name
        except Exception:
            name = ""

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

    return {
        "id": clean_cell_value(doc.get("id")) or uuid.uuid4().hex,
        "name": name,
        "path": path,
        "fileUrl": file_url,
        "previewUrl": preview_url,
        "size": int(doc.get("size") or 0),
        "modifiedAt": clean_cell_value(doc.get("modifiedAt")) or uploaded_at,
        "uploadedAt": uploaded_at,
        "uploadedById": uploaded_by_id,
        "uploadedByName": uploaded_by_name,
        "source": clean_cell_value(doc.get("source")) or "local",

        "mirrorStatus": clean_cell_value(doc.get("mirrorStatus")) or "",
        "mirrorError": clean_cell_value(doc.get("mirrorError")) or "",
        "mirrorJobId": clean_cell_value(doc.get("mirrorJobId")) or "",
        "yandexPath": clean_cell_value(doc.get("yandexPath")) or "",
        "yandexFileUrl": clean_cell_value(doc.get("yandexFileUrl")) or "",
        "yandexFolderAlias": clean_cell_value(doc.get("yandexFolderAlias")) or "",
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

    result.sort(key=lambda x: (x.get("name") or "").lower())
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


def remove_all_item_documents(dialog_id: str, checklist_key: str, item_id: str, item: dict) -> dict:
    cleaned_item = migrate_legacy_document_fields(dict(item or {}))
    documents = normalize_documents_list(cleaned_item.get("documents"))

    for doc in documents:
        local_document_url = (
            clean_cell_value(doc.get("fileUrl"))
            or clean_cell_value(doc.get("previewUrl"))
            or clean_cell_value(doc.get("path"))
        )

        remove_item_document_file({
            "documentUrl": local_document_url
        })

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