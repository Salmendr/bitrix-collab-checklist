"""Folders inside an item (or subitem): any depth, files at every level.

The structure is metadata. A document keeps ``relativeFolder`` ("" is the
item root, "Альбом 1/Разделы" is nested); the item keeps ``subfolders``, the
list of every existing folder, including empty ones. Local files stay where
they were uploaded (unique names), so renaming or moving a folder never moves
local files; only the Yandex Disk copy follows the structure.
"""
from __future__ import annotations

import re
from typing import Iterable

from app.checklists.utils import clean_cell_value


# Yandex Disk limits a file or folder name to 255 characters. Characters that
# Windows refuses in names are replaced, so the synchronized copy stays usable.
MAX_SEGMENT_LENGTH = 255
FORBIDDEN_SEGMENT_CHARS_RE = re.compile(r'[<>:"|?*\x00-\x1f\x7f]')


class FolderPathError(ValueError):
    pass


def sanitize_folder_segment(value: object, *, strict: bool = True) -> str:
    segment = clean_cell_value(value)
    segment = FORBIDDEN_SEGMENT_CHARS_RE.sub("_", segment)
    segment = re.sub(r"\s+", " ", segment).strip()
    # Windows drops trailing dots and spaces of folder names.
    segment = segment.rstrip(". ").strip()
    if segment in {"", ".", ".."}:
        if strict:
            raise FolderPathError("Недопустимое имя папки")
        return ""
    if len(segment) > MAX_SEGMENT_LENGTH:
        if strict:
            raise FolderPathError(
                f"Имя папки длиннее {MAX_SEGMENT_LENGTH} символов: "
                f"{segment[:40]}…"
            )
        segment = segment[:MAX_SEGMENT_LENGTH].rstrip(". ")
    return segment


def normalize_relative_folder(value: object, *, strict: bool = True) -> str:
    raw = clean_cell_value(value).replace("\\", "/")
    parts = []
    for part in raw.split("/"):
        if not clean_cell_value(part):
            continue
        segment = sanitize_folder_segment(part, strict=strict)
        if segment:
            parts.append(segment)
    return "/".join(parts)


def folder_key(value: str) -> str:
    return clean_cell_value(value).casefold()


def folder_ancestors(path: str) -> list[str]:
    parts = [part for part in clean_cell_value(path).split("/") if part]
    return ["/".join(parts[: index + 1]) for index in range(len(parts))]


def parent_folder(path: str) -> str:
    parts = [part for part in clean_cell_value(path).split("/") if part]
    return "/".join(parts[:-1])


def folder_name(path: str) -> str:
    parts = [part for part in clean_cell_value(path).split("/") if part]
    return parts[-1] if parts else ""


def is_within(path: str, folder: str) -> bool:
    """True when ``path`` is ``folder`` itself or lies below it."""
    path_key = folder_key(path)
    base_key = folder_key(folder)
    if not base_key:
        return True
    return path_key == base_key or path_key.startswith(base_key + "/")


def rebase_folder(path: str, old: str, new: str) -> str:
    if not is_within(path, old):
        return path
    rest = clean_cell_value(path)[len(clean_cell_value(old)):].lstrip("/")
    return "/".join(part for part in [clean_cell_value(new), rest] if part)


def document_relative_folder(document: dict | None) -> str:
    return normalize_relative_folder(
        (document or {}).get("relativeFolder"),
        strict=False,
    )


def join_yandex_folder(base_path: str, relative_folder: str) -> str:
    base = clean_cell_value(base_path).rstrip("/")
    relative = normalize_relative_folder(relative_folder, strict=False)
    return f"{base}/{relative}" if relative else base


def document_yandex_folder(item_folder_path: str, document: dict | None) -> str:
    return join_yandex_folder(item_folder_path, document_relative_folder(document))


def item_subfolders(item: dict | None) -> list[str]:
    """Every folder of the item: explicit ones and those holding documents."""
    source = item or {}
    by_key: dict[str, str] = {}
    candidates: list[str] = []
    for raw in source.get("subfolders") or []:
        candidates.append(normalize_relative_folder(raw, strict=False))
    for document in source.get("documents") or []:
        if isinstance(document, dict):
            candidates.append(document_relative_folder(document))
    for candidate in candidates:
        for ancestor in folder_ancestors(candidate):
            by_key.setdefault(folder_key(ancestor), ancestor)
    return sorted(by_key.values(), key=lambda value: [part.casefold() for part in value.split("/")])


def child_folders(folders: Iterable[str], parent: str) -> list[str]:
    parent_key = folder_key(parent)
    result = []
    for folder in folders or []:
        if folder_key(parent_folder(folder)) == parent_key and folder:
            result.append(folder)
    return sorted(result, key=lambda value: folder_name(value).casefold())


def documents_in_folder(documents: Iterable[dict], folder: str) -> list[dict]:
    key = folder_key(folder)
    return [
        document for document in documents or []
        if isinstance(document, dict)
        and folder_key(document_relative_folder(document)) == key
    ]


def documents_below(documents: Iterable[dict], folder: str) -> list[dict]:
    return [
        document for document in documents or []
        if isinstance(document, dict)
        and is_within(document_relative_folder(document), folder)
    ]


def names_in_folder(documents: Iterable[dict], folder: str, exclude_id: str = "") -> list[str]:
    excluded = clean_cell_value(exclude_id)
    return [
        clean_cell_value(document.get("name"))
        for document in documents_in_folder(documents, folder)
        if clean_cell_value(document.get("id")) != excluded
    ]


def item_allows_plain_folders(item: dict | None) -> bool:
    """Folders of a top-level item are its subitems; plain folders live in
    subitems (and in a subitem detached to «Не требуется»)."""
    source = item or {}
    return bool(
        clean_cell_value(source.get("parentItemId"))
        or clean_cell_value(source.get("notRequiredReturnParentId"))
    )


TOP_LEVEL_FOLDER_ERROR = (
    "Папки пункта — это его подпункты: загрузите папку в пункт, "
    "и она станет подпунктом"
)


def find_folder(folders: Iterable[str], path: str) -> str:
    key = folder_key(path)
    for folder in folders or []:
        if folder_key(folder) == key:
            return folder
    return ""


def canonical_folder(folders: Iterable[str], path: str) -> str:
    """``path`` spelled like the existing folders of the item.

    Names differ only by letter case → the existing spelling wins, so one
    folder never splits into «Альбом» and «альбом».
    """
    known = {folder_key(folder): folder for folder in folders or []}
    result = ""
    for part in [part for part in clean_cell_value(path).split("/") if part]:
        candidate = f"{result}/{part}" if result else part
        result = known.get(folder_key(candidate)) or candidate
    return result
