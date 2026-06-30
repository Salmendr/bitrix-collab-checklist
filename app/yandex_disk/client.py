import requests
from pathlib import Path
from app.settings import (
    YANDEX_DISK_OAUTH_TOKEN,
    YANDEX_DISK_API_BASE,
)


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


def yandex_disk_get_upload_href(target_path: str, overwrite: bool = True) -> str:
    normalized_path = normalize_yandex_disk_path(target_path)

    response = requests.get(
        f"{YANDEX_DISK_API_BASE}/resources/upload",
        headers=get_yandex_disk_headers(),
        params={
            "path": normalized_path,
            "overwrite": "true" if overwrite else "false",
        },
        timeout=30,
    )

    response.raise_for_status()
    data = response.json() or {}

    href = clean_disk_value(data.get("href"))
    if not href:
        raise RuntimeError("Yandex Disk upload href not found")

    return href


def yandex_disk_upload_bytes(target_path: str, file_bytes: bytes) -> dict:
    upload_href = yandex_disk_get_upload_href(target_path, overwrite=True)

    upload_response = requests.put(
        upload_href,
        data=file_bytes,
        timeout=120,
    )

    upload_response.raise_for_status()

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


def yandex_disk_upload_file(
    target_path: str,
    local_path,
    progress_callback=None,
    chunk_size: int = 1024 * 1024,
) -> dict:
    normalized_path = normalize_yandex_disk_path(target_path)
    source_path = Path(local_path)

    if not source_path.exists():
        raise RuntimeError(f"Local file not found: {source_path}")

    total_bytes = source_path.stat().st_size
    upload_href = yandex_disk_get_upload_href(normalized_path, overwrite=True)

    with open(source_path, "rb") as f:
        reader = ProgressFileReader(
            raw=f,
            total_bytes=total_bytes,
            progress_callback=progress_callback,
        )

        upload_response = requests.put(
            upload_href,
            data=reader,
            headers={
                "Content-Length": str(total_bytes),
            },
            timeout=300,
        )

    upload_response.raise_for_status()

    return {
        "ok": True,
        "path": normalized_path,
        "size": total_bytes,
    }

def yandex_disk_delete_path(target_path: str, permanently: bool = True):
    normalized_path = normalize_yandex_disk_path(target_path)

    response = requests.delete(
        f"{YANDEX_DISK_API_BASE}/resources",
        headers=get_yandex_disk_headers(),
        params={
            "path": normalized_path,
            "permanently": "true" if permanently else "false",
        },
        timeout=30,
    )

    if response.status_code not in (200, 202, 204):
        try:
            payload = response.json()
        except Exception:
            payload = {"text": response.text}

        raise RuntimeError(f"Yandex Disk delete failed: {payload}")


def yandex_disk_ensure_folder(target_path: str) -> dict:
    normalized_path = normalize_yandex_disk_path(target_path)

    response = requests.put(
        f"{YANDEX_DISK_API_BASE}/resources",
        headers=get_yandex_disk_headers(),
        params={"path": normalized_path},
        timeout=30,
    )

    if response.status_code not in (201, 409):
        try:
            payload = response.json()
        except Exception:
            payload = {"text": response.text}

        raise RuntimeError(f"Yandex Disk create folder failed: {payload}")

    return {
        "ok": True,
        "path": normalized_path,
        "alreadyExists": response.status_code == 409,
    }


def yandex_disk_publish_path(target_path: str) -> dict:
    normalized_path = normalize_yandex_disk_path(target_path)

    response = requests.put(
        f"{YANDEX_DISK_API_BASE}/resources/publish",
        headers=get_yandex_disk_headers(),
        params={"path": normalized_path},
        timeout=30,
    )

    if response.status_code not in (200, 201, 202):
        try:
            payload = response.json()
        except Exception:
            payload = {"text": response.text}

        raise RuntimeError(f"Yandex Disk publish failed: {payload}")

    return {
        "ok": True,
        "path": normalized_path,
    }


def yandex_disk_get_resource_meta(target_path: str) -> dict:
    normalized_path = normalize_yandex_disk_path(target_path)

    response = requests.get(
        f"{YANDEX_DISK_API_BASE}/resources",
        headers=get_yandex_disk_headers(),
        params={
            "path": normalized_path,
            "fields": "name,path,public_url",
        },
        timeout=30,
    )

    response.raise_for_status()
    data = response.json() or {}

    return {
        "name": clean_disk_value(data.get("name")),
        "path": clean_disk_value(data.get("path")) or normalized_path,
        "public_url": clean_disk_value(data.get("public_url")),
    }