import requests

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