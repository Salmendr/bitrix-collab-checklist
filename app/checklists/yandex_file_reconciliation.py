from __future__ import annotations

import hashlib
from pathlib import Path

from app.checklists.storage import get_project_storage_context
from app.checklists.utils import (
    clean_cell_value,
    normalize_checklist_key,
    normalize_dialog_id,
)
from app.checklists.yandex_structure_jobs import (
    get_latest_yandex_structure_job_for_item,
)
from app.yandex_disk.client import (
    normalize_yandex_disk_path,
    yandex_disk_client_url,
    yandex_disk_try_get_resource_meta,
)


REMOTE_FILE_CONFLICT_ERROR = (
    "Конфликт файла Яндекс.Диска: файл с таким именем найден, "
    "но его содержимое отличается от локальной копии. "
    "Автоматическая перезапись остановлена."
)


def _path_key(value: str) -> str:
    return normalize_yandex_disk_path(value).rstrip("/").casefold()


def collect_known_item_yandex_folder_paths(
    *,
    dialog_id: str,
    checklist_key: str,
    item: dict,
    document: dict | None = None,
    context: dict | None = None,
    repair_spec: dict | None = None,
    custom_recovery: dict | None = None,
) -> list[str]:
    """Only the current item's directory inside its authoritative project root.

    A persisted document path or an old job is not proof of ownership. Folder
    moves are reconciled separately; file discovery must not follow history.
    """
    from app.checklists.yandex_scope import item_folder
    return [item_folder(dialog_id, checklist_key, dict(item or {}), context)]


def _hash_local_file(
    path: Path,
    algorithm: str,
    cache: dict[str, str],
) -> str:
    normalized_algorithm = clean_cell_value(algorithm).lower()
    if normalized_algorithm in cache:
        return cache[normalized_algorithm]
    digest = hashlib.new(normalized_algorithm)
    with path.open("rb") as source:
        while True:
            chunk = source.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    cache[normalized_algorithm] = digest.hexdigest().lower()
    return cache[normalized_algorithm]


def _remote_identity_result(
    *,
    local_path: Path,
    expected_name: str,
    remote_meta: dict,
    hash_cache: dict[str, str],
) -> tuple[bool, str]:
    remote_name = clean_cell_value(remote_meta.get("name"))
    if remote_name.casefold() != expected_name.casefold():
        return False, "name_mismatch"

    local_size = int(local_path.stat().st_size)
    remote_size_value = remote_meta.get("size")
    try:
        remote_size = int(remote_size_value)
    except (TypeError, ValueError):
        return False, "remote_size_missing"
    if remote_size != local_size:
        return False, "size_mismatch"

    remote_sha256 = clean_cell_value(remote_meta.get("sha256")).lower()
    if remote_sha256:
        if _hash_local_file(local_path, "sha256", hash_cache) != remote_sha256:
            return False, "sha256_mismatch"
        return True, "sha256_match"

    remote_md5 = clean_cell_value(remote_meta.get("md5")).lower()
    if remote_md5:
        if _hash_local_file(local_path, "md5", hash_cache) != remote_md5:
            return False, "md5_mismatch"
        return True, "md5_match"

    # Name and size alone are not sufficient proof that two files are the
    # same.  If Yandex omits both checksums, stop automatic reconciliation
    # instead of adopting or overwriting an unverified remote file.
    return False, "remote_checksum_missing"


def find_existing_yandex_document(
    *,
    dialog_id: str,
    checklist_key: str,
    item: dict,
    document: dict,
    local_path: Path,
    context: dict | None = None,
    repair_spec: dict | None = None,
    custom_recovery: dict | None = None,
) -> dict:
    """Return matched/conflict/missing after probing every known file path."""
    local_path = Path(local_path)
    expected_name = (
        clean_cell_value(document.get("name"))
        or local_path.name
    )
    known_folders = collect_known_item_yandex_folder_paths(
        dialog_id=dialog_id,
        checklist_key=checklist_key,
        item=item,
        document=document,
        context=context,
        repair_spec=repair_spec,
        custom_recovery=custom_recovery,
    )

    candidate_paths: dict[str, str] = {}
    stored_path = normalize_yandex_disk_path(
        clean_cell_value(document.get("yandexPath"))
    )
    # Ignore persisted file paths: even an identically named file in another
    # object must never influence this item's synchronization.
    safe_name = Path(expected_name or local_path.name).name
    from app.checklists.document_folders import document_yandex_folder
    for folder_path in known_folders:
        file_path = normalize_yandex_disk_path(
            f"{document_yandex_folder(folder_path, document).rstrip('/')}/{safe_name}"
        )
        candidate_paths.setdefault(_path_key(file_path), file_path)

    matches: list[dict] = []
    conflicts: list[dict] = []
    probe_errors: list[dict] = []
    checked_paths: list[str] = []
    hash_cache: dict[str, str] = {}

    for candidate_path in candidate_paths.values():
        checked_paths.append(candidate_path)
        try:
            remote_meta = yandex_disk_try_get_resource_meta(candidate_path)
        except Exception as exc:
            probe_errors.append({
                "path": candidate_path,
                "error": str(exc),
            })
            continue
        if remote_meta is None:
            continue
        from app.checklists.yandex_scope import canonical_path
        returned_path = clean_cell_value(remote_meta.get("path"))
        if returned_path and canonical_path(returned_path) != canonical_path(candidate_path):
            probe_errors.append({"path": candidate_path, "error": "Remote response path differs from requested path"})
            continue
        if clean_cell_value(remote_meta.get("type")).lower() != "file":
            conflicts.append({
                "path": candidate_path,
                "url": yandex_disk_client_url(candidate_path),
                "reason": "not_a_file",
                "meta": remote_meta,
            })
            continue
        matched, reason = _remote_identity_result(
            local_path=local_path,
            expected_name=safe_name,
            remote_meta=remote_meta,
            hash_cache=hash_cache,
        )
        payload = {
            "name": clean_cell_value(remote_meta.get("name")) or safe_name,
            "path": normalize_yandex_disk_path(
                clean_cell_value(remote_meta.get("path")) or candidate_path
            ),
            "url": yandex_disk_client_url(candidate_path),
            "size": int(remote_meta.get("size") or 0),
            "sha256": clean_cell_value(remote_meta.get("sha256")),
            "md5": clean_cell_value(remote_meta.get("md5")),
            "reason": reason,
        }
        if matched:
            matches.append(payload)
        else:
            conflicts.append(payload)

    if probe_errors:
        return {
            "status": "unavailable",
            "error": (
                "Не удалось проверить все допустимые пути файла на "
                "Яндекс.Диске. Автоматическая загрузка остановлена."
            ),
            "match": {},
            "matches": matches,
            "conflicts": conflicts,
            "probeErrors": probe_errors,
            "checkedPaths": checked_paths,
        }
    if len(matches) == 1 and not conflicts:
        return {
            "status": "matched",
            "match": matches[0],
            "matches": matches,
            "conflicts": [],
            "probeErrors": [],
            "checkedPaths": checked_paths,
        }
    if matches or conflicts:
        return {
            "status": "conflict",
            "error": REMOTE_FILE_CONFLICT_ERROR,
            "match": {},
            "matches": matches,
            "conflicts": conflicts,
            "probeErrors": [],
            "checkedPaths": checked_paths,
        }
    return {
        "status": "missing",
        "match": {},
        "matches": [],
        "conflicts": [],
        "probeErrors": [],
        "checkedPaths": checked_paths,
    }
