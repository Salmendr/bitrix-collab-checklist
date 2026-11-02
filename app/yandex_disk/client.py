from app.checklists.yandex_scope import assert_operation_path

import requests
import time
from pathlib import Path
from urllib.parse import quote
from app.settings import (
    YANDEX_DISK_OAUTH_TOKEN,
    YANDEX_DISK_API_BASE,
)
from app.checklists.yandex_resource_locks import (
    run_with_yandex_resource_retry,
)
from app.logging_utils import write_debug_log
import hashlib


def clean_disk_value(value) -> str:
    value = str(value or "").strip()
    if value == "—":
        return ""
    return value


def is_yandex_disk_enabled() -> bool:
    return bool(YANDEX_DISK_OAUTH_TOKEN)


def get_yandex_disk_headers() -> dict:
    return {
        "Authorization": f"OAuth {YANDEX_DISK_OAUTH_TOKEN}"
    }


def normalize_yandex_disk_path(path: str) -> str:
    value = clean_disk_value(path)
    if not value:
        return ""

    if value.startswith("disk:/"):
        return value

    if value.startswith("/"):
        return "disk:" + value

    return "disk:/" + value


def yandex_disk_client_url(target_path: str) -> str:
    normalized = normalize_yandex_disk_path(target_path)
    relative = normalized[len("disk:/"):].strip("/") if normalized else ""
    return "https://disk.yandex.ru/client/disk/" + quote(relative, safe="/")


def _response_payload(response) -> dict:
    try:
        payload = response.json() or {}
        return payload if isinstance(payload, dict) else {"payload": payload}
    except Exception:
        return {"text": clean_disk_value(getattr(response, "text", ""))}


def _request_with_resource_retry(
    request_factory,
    *,
    operation_name: str,
    accepted_statuses: tuple[int, ...] | None = None,
):
    def perform():
        response = request_factory()
        accepted = (
            response.status_code in accepted_statuses
            if accepted_statuses is not None
            else 200 <= int(response.status_code or 0) < 300
        )
        if not accepted:
            raise RuntimeError(
                f"Yandex Disk {operation_name} failed "
                f"(status {response.status_code}): "
                f"{_response_payload(response)}"
            )
        return response

    return run_with_yandex_resource_retry(
        perform,
        operation_name=operation_name,
    )


def yandex_disk_get_upload_href(target_path: str, overwrite: bool = True) -> str:
    assert_operation_path(target_path)
    normalized_path = normalize_yandex_disk_path(target_path)

    response = _request_with_resource_retry(
        lambda: requests.get(
            f"{YANDEX_DISK_API_BASE}/resources/upload",
            headers=get_yandex_disk_headers(),
            params={
                "path": normalized_path,
                "overwrite": "true" if overwrite else "false",
            },
            timeout=30,
        ),
        operation_name="get upload href",
    )
    data = response.json() or {}

    href = clean_disk_value(data.get("href"))
    if not href:
        raise RuntimeError("Yandex Disk upload href not found")

    return href


def yandex_disk_upload_bytes(target_path: str, file_bytes: bytes, *, overwrite: bool = False) -> dict:
    upload_href = yandex_disk_get_upload_href(target_path, overwrite=overwrite)

    _request_with_resource_retry(
        lambda: requests.put(
            upload_href,
            data=file_bytes,
            timeout=120,
        ),
        operation_name="upload bytes",
    )

    return {
        "ok": True,
        "path": normalize_yandex_disk_path(target_path),
    }

class ProgressFileReader:
    def __init__(self, raw, total_bytes: int, progress_callback=None):
        self.raw = raw
        self.total_bytes = int(total_bytes or 0)
        self.progress_callback = progress_callback
        self.uploaded_bytes = 0

    def read(self, size=-1):
        chunk = self.raw.read(size)
        if chunk:
            self.uploaded_bytes += len(chunk)
            if self.progress_callback:
                self.progress_callback(self.uploaded_bytes, self.total_bytes)
        return chunk

    def __len__(self):
        return self.total_bytes

    def __getattr__(self, name):
        return getattr(self.raw, name)


# Yandex Disk caps the REST API upload of archives at 128 KiB/s: a 1 GB
# archive then takes more than two hours. The type is taken from the file name
# when the upload link is requested; a name without an extension is not
# capped. An archive is uploaded under a temporary name without extension in
# the same folder and renamed on Yandex afterwards. Other files go as before.
YANDEX_ARCHIVE_EXTENSIONS = frozenset({
    "zip", "zipx", "rar", "7z", "gz", "tgz", "tar", "bz2", "tbz", "tbz2",
    "xz", "txz", "lz", "lzma", "lzh", "z", "zst", "cab", "arj", "ace",
})
YANDEX_UPLOAD_LOG_INTERVAL_SECONDS = 60.0


def yandex_upload_needs_neutral_name(file_name: str) -> bool:
    name = clean_disk_value(file_name).rsplit("/", 1)[-1]
    extension = name.rsplit(".", 1)[-1].casefold() if "." in name else ""
    return extension in YANDEX_ARCHIVE_EXTENSIONS


def yandex_neutral_upload_path(target_path: str) -> str:
    """The same folder, a name without any dot: no extension, no cap."""
    normalized = normalize_yandex_disk_path(target_path)
    folder, _, name = normalized.rpartition("/")
    token = hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:12]
    readable = "".join(
        "_" if ch in "./\\" or ord(ch) < 32 else ch for ch in name
    ).strip(" _")[:60]
    return f"{folder}/{readable + ' ' if readable else ''}загружается {token}"


def yandex_disk_upload_file(
    target_path: str,
    local_path,
    progress_callback=None,
    chunk_size: int = 1024 * 1024,
    overwrite: bool = False,
) -> dict:
    normalized_path = normalize_yandex_disk_path(target_path)
    source_path = Path(local_path)

    if not source_path.exists():
        raise RuntimeError(f"Local file not found: {source_path}")

    total_bytes = source_path.stat().st_size
    neutral = yandex_upload_needs_neutral_name(normalized_path)
    upload_path = yandex_neutral_upload_path(normalized_path) if neutral else normalized_path
    assert_operation_path(normalized_path)
    if neutral and not overwrite and yandex_disk_try_get_resource_meta(normalized_path) is not None:
        raise RuntimeError(
            f"Yandex Disk upload failed (status 409): {normalized_path} already exists"
        )
    upload_href = yandex_disk_get_upload_href(upload_path, overwrite=True if neutral else overwrite)

    started = time.monotonic()
    last_log = [started]
    write_debug_log("yandex_file_upload_started", {
        "path": normalized_path,
        "uploadPath": upload_path,
        "temporaryName": neutral,
        "totalBytes": total_bytes,
    })

    def report(uploaded_bytes: int, size: int):
        now = time.monotonic()
        if now - last_log[0] >= YANDEX_UPLOAD_LOG_INTERVAL_SECONDS:
            last_log[0] = now
            elapsed = max(0.001, now - started)
            write_debug_log("yandex_file_upload_progress", {
                "path": normalized_path,
                "uploadedBytes": uploaded_bytes,
                "totalBytes": size,
                "elapsedSeconds": round(elapsed),
                "speedKBps": round(uploaded_bytes / 1024 / elapsed),
            })
        if progress_callback:
            progress_callback(uploaded_bytes, size)

    def upload_once():
        with open(source_path, "rb") as source:
            reader = ProgressFileReader(
                raw=source,
                total_bytes=total_bytes,
                progress_callback=report,
            )
            return requests.put(
                upload_href,
                data=reader,
                headers={
                    "Content-Length": str(total_bytes),
                },
                timeout=300,
            )

    _request_with_resource_retry(
        upload_once,
        operation_name="upload file",
    )
    elapsed = max(0.001, time.monotonic() - started)

    if neutral:
        try:
            yandex_disk_move_path(upload_path, normalized_path, overwrite=True)
        except Exception as exc:
            write_debug_log("yandex_file_upload_rename_failed", {
                "path": normalized_path,
                "uploadPath": upload_path,
                "error": str(exc),
            })
            try:
                yandex_disk_delete_path(upload_path, permanently=True)
            except Exception as cleanup_exc:
                write_debug_log("yandex_file_upload_temporary_delete_failed", {
                    "uploadPath": upload_path,
                    "error": str(cleanup_exc),
                })
            raise

    write_debug_log("yandex_file_upload_finished", {
        "path": normalized_path,
        "temporaryName": neutral,
        "totalBytes": total_bytes,
        "elapsedSeconds": round(elapsed, 1),
        "speedKBps": round(total_bytes / 1024 / elapsed),
    })

    return {
        "ok": True,
        "path": normalized_path,
        "size": total_bytes,
    }

def yandex_disk_delete_path(
    target_path: str,
    permanently: bool = True,
    wait_timeout: int = 45,
) -> dict:
    assert_operation_path(target_path)
    normalized_path = normalize_yandex_disk_path(target_path)

    if not normalized_path:
        raise RuntimeError("Yandex Disk delete path is required")

    response = _request_with_resource_retry(
        lambda: requests.delete(
            f"{YANDEX_DISK_API_BASE}/resources",
            headers=get_yandex_disk_headers(),
            params={
                "path": normalized_path,
                "permanently": "true" if permanently else "false",
            },
            timeout=30,
        ),
        operation_name="delete",
        # Delete is idempotent: a stale replacement path that is already absent
        # is the same successful end state and must not leave a red archive
        # error forever.
        accepted_statuses=(200, 202, 204, 404),
    )

    if response.status_code == 404:
        return {
            "ok": True,
            "path": normalized_path,
            "deleted": False,
            "alreadyMissing": True,
            "async": False,
        }

    operation_href = ""
    if response.status_code == 202:
        try:
            operation_href = clean_disk_value((response.json() or {}).get("href"))
        except Exception:
            operation_href = ""

    if operation_href:
        deadline = time.monotonic() + max(1, int(wait_timeout or 45))
        while time.monotonic() < deadline:
            operation_response = _request_with_resource_retry(
                lambda: requests.get(
                    operation_href,
                    headers=get_yandex_disk_headers(),
                    timeout=30,
                ),
                operation_name="poll delete operation",
            )
            operation = operation_response.json() or {}
            status = clean_disk_value(operation.get("status")).lower()
            if status == "success":
                break
            if status == "failed":
                raise RuntimeError(
                    f"Yandex Disk delete operation failed: {operation}"
                )
            time.sleep(0.5)
        else:
            raise RuntimeError("Yandex Disk delete operation timed out")

    return {
        "ok": True,
        "path": normalized_path,
        "deleted": True,
        "alreadyMissing": False,
        "async": bool(operation_href),
    }


def yandex_disk_ensure_folder(target_path: str) -> dict:
    assert_operation_path(target_path, allow_ancestor=True)
    normalized_path = normalize_yandex_disk_path(target_path)

    response = _request_with_resource_retry(
        lambda: requests.put(
            f"{YANDEX_DISK_API_BASE}/resources",
            headers=get_yandex_disk_headers(),
            params={"path": normalized_path},
            timeout=30,
        ),
        operation_name="create folder",
        accepted_statuses=(201, 409),
    )

    return {
        "ok": True,
        "path": normalized_path,
        "alreadyExists": response.status_code == 409,
    }


def yandex_disk_publish_path(target_path: str) -> dict:
    assert_operation_path(target_path)
    normalized_path = normalize_yandex_disk_path(target_path)

    _request_with_resource_retry(
        lambda: requests.put(
            f"{YANDEX_DISK_API_BASE}/resources/publish",
            headers=get_yandex_disk_headers(),
            params={"path": normalized_path},
            timeout=30,
        ),
        operation_name="publish",
        accepted_statuses=(200, 201, 202),
    )

    return {
        "ok": True,
        "path": normalized_path,
    }


def yandex_disk_move_path(
    source_path: str,
    target_path: str,
    overwrite: bool = False,
    wait_timeout: int = 45,
) -> dict:
    assert_operation_path(source_path)
    assert_operation_path(target_path)
    normalized_source = normalize_yandex_disk_path(source_path)
    normalized_target = normalize_yandex_disk_path(target_path)

    if not normalized_source or not normalized_target:
        raise RuntimeError("Yandex Disk source and target paths are required")

    if normalized_source == normalized_target:
        return {
            "ok": True,
            "sourcePath": normalized_source,
            "targetPath": normalized_target,
            "unchanged": True,
        }

    response = _request_with_resource_retry(
        lambda: requests.post(
            f"{YANDEX_DISK_API_BASE}/resources/move",
            headers=get_yandex_disk_headers(),
            params={
                "from": normalized_source,
                "path": normalized_target,
                "overwrite": "true" if overwrite else "false",
                "force_async": "false",
            },
            timeout=30,
        ),
        operation_name="move",
        accepted_statuses=(201, 202),
    )

    operation_href = ""
    if response.status_code == 202:
        try:
            operation_href = clean_disk_value((response.json() or {}).get("href"))
        except Exception:
            operation_href = ""

    if operation_href:
        deadline = time.monotonic() + max(1, int(wait_timeout or 45))
        while time.monotonic() < deadline:
            operation_response = _request_with_resource_retry(
                lambda: requests.get(
                    operation_href,
                    headers=get_yandex_disk_headers(),
                    timeout=30,
                ),
                operation_name="poll move operation",
            )
            operation = operation_response.json() or {}
            status = clean_disk_value(operation.get("status")).lower()
            if status == "success":
                break
            if status == "failed":
                raise RuntimeError(
                    f"Yandex Disk move operation failed: {operation}"
                )
            time.sleep(0.5)
        else:
            raise RuntimeError("Yandex Disk move operation timed out")

    return {
        "ok": True,
        "sourcePath": normalized_source,
        "targetPath": normalized_target,
        "unchanged": False,
        "async": bool(operation_href),
    }


def yandex_disk_get_resource_meta(target_path: str) -> dict:
    assert_operation_path(target_path)
    normalized_path = normalize_yandex_disk_path(target_path)

    response = _request_with_resource_retry(
        lambda: requests.get(
            f"{YANDEX_DISK_API_BASE}/resources",
            headers=get_yandex_disk_headers(),
            params={
                "path": normalized_path,
                "fields": "name,path,type,public_url,size,sha256,md5",
            },
            timeout=30,
        ),
        operation_name="get resource metadata",
    )
    data = response.json() or {}

    return {
        "name": clean_disk_value(data.get("name")),
        "path": clean_disk_value(data.get("path")) or normalized_path,
        "type": clean_disk_value(data.get("type")),
        "public_url": clean_disk_value(data.get("public_url")),
        "size": data.get("size"),
        "sha256": clean_disk_value(data.get("sha256")),
        "md5": clean_disk_value(data.get("md5")),
    }


def yandex_disk_try_get_resource_meta(target_path: str) -> dict | None:
    assert_operation_path(target_path)
    normalized_path = normalize_yandex_disk_path(target_path)
    if not normalized_path:
        return None
    response = _request_with_resource_retry(
        lambda: requests.get(
            f"{YANDEX_DISK_API_BASE}/resources",
            headers=get_yandex_disk_headers(),
            params={
                "path": normalized_path,
                "fields": "name,path,type,public_url,size,sha256,md5",
            },
            timeout=30,
        ),
        operation_name="probe resource metadata",
        accepted_statuses=(200, 404),
    )
    if response.status_code == 404:
        return None
    data = response.json() or {}
    return {
        "name": clean_disk_value(data.get("name")),
        "path": clean_disk_value(data.get("path")) or normalized_path,
        "type": clean_disk_value(data.get("type")),
        "public_url": clean_disk_value(data.get("public_url")),
        "size": data.get("size"),
        "sha256": clean_disk_value(data.get("sha256")),
        "md5": clean_disk_value(data.get("md5")),
    }


def yandex_disk_list_folder_children(target_path: str) -> list[dict]:
    assert_operation_path(target_path)
    normalized_path = normalize_yandex_disk_path(target_path)
    if not normalized_path:
        return []

    items: list[dict] = []
    offset = 0
    page_size = 200
    while True:
        response = _request_with_resource_retry(
            lambda current_offset=offset: requests.get(
                f"{YANDEX_DISK_API_BASE}/resources",
                headers=get_yandex_disk_headers(),
                params={
                    "path": normalized_path,
                    "limit": page_size,
                    "offset": current_offset,
                    "fields": (
                        "_embedded.items.name,_embedded.items.path,"
                        "_embedded.items.type,_embedded.items.public_url,"
                        "_embedded.total"
                    ),
                },
                timeout=30,
            ),
            operation_name="list folder children",
        )
        data = response.json() or {}
        embedded = data.get("_embedded") or {}
        page = embedded.get("items") or []
        for raw in page:
            if not isinstance(raw, dict):
                continue
            path = clean_disk_value(raw.get("path"))
            items.append({
                "name": clean_disk_value(raw.get("name")),
                "path": path,
                "type": clean_disk_value(raw.get("type")),
                "public_url": clean_disk_value(raw.get("public_url")),
                "client_url": yandex_disk_client_url(path),
            })

        offset += len(page)
        total = int(embedded.get("total") or len(items))
        if not page or offset >= total:
            break
    return items
