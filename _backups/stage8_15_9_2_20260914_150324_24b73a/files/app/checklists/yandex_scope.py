"""Resolve a file inside the exact project and item that own the operation."""
from __future__ import annotations

from app.checklists.utils import clean_cell_value, normalize_checklist_key


class YandexScopeError(RuntimeError):
    pass


def canonical_path(value: str) -> str:
    value = clean_cell_value(value).replace("\\", "/")
    if not value:
        return ""
    if value.startswith("disk:/"):
        value = value[6:]
    elif ":" in value:
        raise YandexScopeError("Недопустимый путь Яндекс.Диска")
    parts = [part for part in value.strip("/").split("/") if part]
    if any(part in {".", ".."} for part in parts) or "\x00" in value:
        raise YandexScopeError("Недопустимые компоненты пути Яндекс.Диска")
    return "disk:/" + "/".join(parts)


def is_inside(path: str, root: str, *, allow_root: bool = False) -> bool:
    try:
        path, root = canonical_path(path), canonical_path(root)
    except YandexScopeError:
        return False
    # Compare complete path components; Project-2 is not inside Project.
    return bool(root and path and (
        path.startswith(root.rstrip("/") + "/")
        or (allow_root and path == root)
    ))


def project_root(dialog_id: str, context: dict | None = None) -> str:
    if context is None:
        from app.checklists.storage import get_project_storage_context
        context = get_project_storage_context(dialog_id) or {}
    if context.get("yandexRootExplicit") is False:
        raise YandexScopeError("Не задан projectRootPath объекта. Обновите привязку из n8n.")
    root = canonical_path((context.get("yandexDisk") or {}).get("projectRootPath"))
    if not root or root == "disk:/":
        raise YandexScopeError("Не задан корень объекта на Яндекс.Диске")
    return root


def require_project_path(dialog_id: str, path: str, *, context: dict | None = None,
                         allow_root: bool = False) -> str:
    root = project_root(dialog_id, context)
    normalized = canonical_path(path)
    if not is_inside(normalized, root, allow_root=allow_root):
        raise YandexScopeError("Путь находится вне папки текущего объекта. Операция остановлена.")
    return normalized


def item_folder(dialog_id: str, checklist_key: str, item: dict,
                context: dict | None = None) -> str:
    if context is None:
        from app.checklists.storage import get_project_storage_context
        context = get_project_storage_context(dialog_id) or {}
    root = project_root(dialog_id, context)
    explicit = clean_cell_value(item.get("yandexFolderPath"))
    if explicit and is_inside(explicit, root):
        return canonical_path(explicit)
    folders = (context.get("yandexDisk") or {}).get("folders") or {}
    alias = clean_cell_value(item.get("yandexFolderAlias"))
    key = normalize_checklist_key(checklist_key)
    candidates = []
    for mapping in context.get("itemMappings") or []:
        if normalize_checklist_key(mapping.get("checklistKey")) != key:
            continue
        same_alias = bool(alias and mapping.get("folderAlias") == alias)
        same_identity = (
            clean_cell_value(mapping.get("itemName")).casefold()
            == clean_cell_value(item.get("name")).casefold()
            and int(mapping.get("groupId") or 0) == int(item.get("group") or 0)
        )
        if not (same_alias or (not alias and same_identity)):
            continue
        path = (folders.get(mapping.get("folderAlias")) or {}).get("path") or ""
        if is_inside(path, root):
            candidates.append(canonical_path(path))
    if alias and not candidates:
        path = (folders.get(alias) or {}).get("path") or ""
        if is_inside(path, root):
            candidates.append(canonical_path(path))
    candidates = list(dict.fromkeys(candidates))
    if len(candidates) != 1:
        raise YandexScopeError("Не удалось однозначно определить папку пункта внутри текущего объекта")
    return candidates[0]


# The worker guard propagates ownership to low-level Yandex primitives too.
from contextvars import ContextVar
active_project = ContextVar("yandex_active_project", default="")


def assert_operation_path(path: str, *, allow_ancestor: bool = False) -> None:
    dialog_id = active_project.get()
    if not dialog_id:
        return
    root = project_root(dialog_id)
    if allow_ancestor and is_inside(root, path, allow_root=True):
        return  # ensure_folder_chain creates shared ancestors, never deletes them
    require_project_path(dialog_id, path, allow_root=True)
