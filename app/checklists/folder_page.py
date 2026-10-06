"""Server-rendered parts of the item folder page: breadcrumbs and folder tree.

Levels of one page: a top-level item (its folders are its subitems), a
subitem (plain folders of any depth) and a folder inside a subitem.
"""
from __future__ import annotations

import html
from urllib.parse import quote

from app.checklists.config import get_checklist_config
from app.checklists.document_folders import (
    document_relative_folder,
    documents_below,
    folder_ancestors,
    folder_key,
    folder_name,
    item_subfolders,
    join_yandex_folder,
    parent_folder,
)
from app.checklists.documents import normalize_documents_list
from app.checklists.subitems import children_of, parent_id_of
from app.checklists.utils import clean_cell_value


EYE_ICON = (
    '<svg class="folder-tree-eye" viewBox="0 0 24 24" aria-hidden="true">'
    '<path d="M1.5 12S5.5 4.5 12 4.5 22.5 12 22.5 12 18.5 19.5 12 19.5 1.5 12 1.5 12Z"></path>'
    '<circle cx="12" cy="12" r="3.2"></circle></svg>'
)
FOLDER_ICON = (
    '<svg class="folder-tree-folder" viewBox="0 0 24 24" aria-hidden="true">'
    '<path d="M2.5 6.5A1.5 1.5 0 0 1 4 5h5l2 2.2h9a1.5 1.5 0 0 1 1.5 1.5v9.8A1.5 1.5 0 0 1 20 20H4a1.5 1.5 0 0 1-1.5-1.5Z"></path></svg>'
)
SUBITEM_ICON = (
    '<svg class="folder-tree-folder folder-tree-subitem" viewBox="0 0 24 24" aria-hidden="true">'
    '<path d="M2.5 6.5A1.5 1.5 0 0 1 4 5h5l2 2.2h9a1.5 1.5 0 0 1 1.5 1.5v9.8A1.5 1.5 0 0 1 20 20H4a1.5 1.5 0 0 1-1.5-1.5Z"></path>'
    '<path d="M8 13.5l2.5 2.5L16 10.5"></path></svg>'
)


def plural(count: int, one: str, few: str, many: str) -> str:
    tail = count % 100
    if 11 <= tail <= 14:
        word = many
    elif count % 10 == 1:
        word = one
    elif count % 10 in (2, 3, 4):
        word = few
    else:
        word = many
    return f"{count} {word}"


class FolderPageLinks:
    def __init__(
        self,
        *,
        app_base_path: str,
        dialog_id: str,
        checklist_key: str,
        session_id: str,
        user_id: str,
        user_name: str,
    ):
        self.app_base_path = app_base_path
        self.dialog_id = dialog_id
        self.checklist_key = checklist_key
        self.session_id = session_id
        self.user_id = user_id
        self.user_name = user_name

    def folder(self, item_id: str, relative_folder: str = "") -> str:
        url = (
            f"{self.app_base_path}/api/checklist/folder"
            f"?dialogId={quote(self.dialog_id, safe='')}"
            f"&checklistKey={quote(self.checklist_key, safe='')}"
            f"&itemId={quote(clean_cell_value(item_id), safe='')}"
        )
        if relative_folder:
            url += f"&folder={quote(relative_folder, safe='')}"
        for key, value in (
            ("sessionId", self.session_id),
            ("userId", self.user_id),
            ("userName", self.user_name),
        ):
            if value:
                url += f"&{key}={quote(value, safe='')}"
        return url


def _find(items: list[dict], item_id: str) -> dict:
    target = clean_cell_value(item_id)
    return next(
        (item for item in items if clean_cell_value(item.get("id")) == target),
        {},
    )


def back_url(links: FolderPageLinks, items: list[dict], item: dict, folder: str) -> str:
    """One level up: folder → parent folder → subitem → parent item.

    Empty string means the item itself: back returns to the checklist.
    """
    item_id = clean_cell_value(item.get("id"))
    if folder:
        return links.folder(item_id, parent_folder(folder))
    parent_id = parent_id_of(item)
    if parent_id and _find(items, parent_id):
        return links.folder(parent_id)
    return ""


def resolve_item_yandex_folder(dialog_id: str, checklist_key: str, item: dict) -> dict:
    """{path, url} of the item's own Yandex folder.

    Same source as the popup Yandex button: the latest structure job (it
    follows renames and moves), then the stored fields. The legacy lookup by
    item name is only a fallback for standard items: a renamed item, or an
    item created as a subitem, has no mapping under its current name.
    """
    from app.checklists.yandex_structure_jobs import (
        get_latest_yandex_structure_job_for_item,
    )
    from app.checklists.yandex_structure_state import (
        build_yandex_structure_item_fields,
    )

    item_id = clean_cell_value(item.get("id"))
    try:
        latest = get_latest_yandex_structure_job_for_item(
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            item_id=item_id,
        ) or {}
    except Exception:
        latest = {}
    state = build_yandex_structure_item_fields(item=item, job=latest)
    path = clean_cell_value(state.get("yandexFolderPath"))
    url = clean_cell_value(state.get("yandexFolderUrl"))
    if not path and not parent_id_of(item):
        try:
            from app.checklists.storage import get_item_yandex_folder
            legacy = (get_item_yandex_folder(
                dialog_id,
                checklist_key,
                clean_cell_value(item.get("name")),
                group_id=int(item.get("group") or 0),
            ) or {}).get("folder") or {}
            path = clean_cell_value(legacy.get("path"))
            url = url or clean_cell_value(legacy.get("url"))
        except Exception:
            pass
    if not path:
        try:
            from app.checklists.yandex_scope import item_folder
            path = item_folder(dialog_id, checklist_key, item)
        except Exception:
            path = ""
    return {"path": path, "url": url}


def item_yandex_folder_path(dialog_id: str, checklist_key: str, item: dict) -> str:
    return resolve_item_yandex_folder(dialog_id, checklist_key, item)["path"]


def project_yandex_available(dialog_id: str) -> bool:
    from app.checklists.storage import get_project_storage_context
    from app.yandex_disk.client import is_yandex_disk_enabled

    context = get_project_storage_context(dialog_id) or {}
    targets = (context.get("storageMode") or {}).get("mirrorTargets") or []
    return bool(context) and "yandex_disk" in targets and bool(is_yandex_disk_enabled())


def breadcrumb_segments(
    *,
    dialog_id: str,
    checklist_key: str,
    data: dict,
    item: dict,
    folder: str,
    links: FolderPageLinks,
    project_context: dict,
) -> list[tuple[str, str]]:
    """[(label, url)] like the Yandex Disk path bar; url "" = plain text."""
    items = data.get("items") or []
    parent = _find(items, parent_id_of(item)) if parent_id_of(item) else {}
    top_item = parent or item
    top_path = item_yandex_folder_path(dialog_id, checklist_key, top_item)
    project_root = clean_cell_value(
        ((project_context or {}).get("yandexDisk") or {}).get("projectRootPath")
    ).rstrip("/")

    segments: list[tuple[str, str]] = []
    base_root = clean_cell_value(
        ((project_context or {}).get("yandexDisk") or {}).get("projectBaseRootPath")
    ).rstrip("/")
    if base_root and project_root.startswith(base_root + "/"):
        # A project stage: «Проект › Этап 2 › …».
        segments.append((base_root.rsplit("/", 1)[-1], ""))
    if top_path and project_root and top_path.startswith(project_root + "/"):
        root_name = project_root.rsplit("/", 1)[-1]
        inner = [part for part in top_path[len(project_root):].split("/") if part]
        segments.append((root_name, ""))
        for part in inner[:-1]:
            segments.append((part, ""))
        segments.append((inner[-1] if inner else clean_cell_value(top_item.get("name")),
                         links.folder(clean_cell_value(top_item.get("id")))))
    else:
        config = get_checklist_config(checklist_key)
        project_name = clean_cell_value((project_context or {}).get("projectName"))
        if project_name:
            segments.append((project_name, ""))
        segments.append((config.title, ""))
        group_title = config.get_group_title(int(top_item.get("group") or 0))
        if group_title and group_title != config.title:
            segments.append((group_title, ""))
        segments.append((clean_cell_value(top_item.get("name")) or "Пункт",
                         links.folder(clean_cell_value(top_item.get("id")))))

    if parent:
        subitem_path = clean_cell_value(item.get("yandexFolderPath"))
        subitem_label = (
            subitem_path.rstrip("/").rsplit("/", 1)[-1]
            if subitem_path
            else clean_cell_value(item.get("name"))
        )
        segments.append((subitem_label or "Подпункт", links.folder(clean_cell_value(item.get("id")))))

    for ancestor in folder_ancestors(folder):
        segments.append((folder_name(ancestor), links.folder(clean_cell_value(item.get("id")), ancestor)))
    return segments


def breadcrumb_html(segments: list[tuple[str, str]]) -> str:
    parts = []
    last = len(segments) - 1
    for index, (label, url) in enumerate(segments):
        escaped = html.escape(label)
        if index == last:
            parts.append(f'<span class="folder-breadcrumb-current" aria-current="page">{escaped}</span>')
        elif url:
            parts.append(f'<a class="folder-breadcrumb-link" href="{html.escape(url)}">{escaped}</a>')
        else:
            parts.append(f'<span class="folder-breadcrumb-text">{escaped}</span>')
    separator = '<span class="folder-breadcrumb-separator" aria-hidden="true">›</span>'
    return (
        '<nav class="folder-breadcrumbs" aria-label="Путь к папке">'
        + separator.join(parts)
        + "</nav>"
    )


def _file_rows(documents: list[dict], format_datetime) -> str:
    rows = []
    for document in documents:
        name = html.escape(clean_cell_value(document.get("name")) or "Файл")
        uploaded = html.escape(
            format_datetime(document.get("uploadedAt") or document.get("modifiedAt")) or "—"
        )
        rows.append(
            '<li class="folder-tree-file">'
            f'<span class="folder-tree-file-name">{name}</span>'
            f'<span class="folder-tree-file-date">{uploaded}</span>'
            "</li>"
        )
    return "".join(rows)


def _node_html(
    *,
    label: str,
    url: str,
    documents: list[dict],
    children_html: str,
    total_files: int,
    is_subitem: bool,
    format_datetime,
) -> str:
    files_html = _file_rows(documents, format_datetime)
    body = ""
    if files_html:
        body += f'<ul class="folder-tree-files">{files_html}</ul>'
    if children_html:
        body += f'<ul class="folder-tree-children">{children_html}</ul>'
    if not body:
        body = '<div class="folder-tree-empty">Пустая папка</div>'
    icon = SUBITEM_ICON if is_subitem else FOLDER_ICON
    meta = plural(total_files, "файл", "файла", "файлов") if total_files else "пусто"
    return (
        '<li class="folder-tree-node">'
        '<details class="folder-tree-details">'
        '<summary class="folder-tree-summary">'
        '<span class="folder-tree-chevron" aria-hidden="true"></span>'
        f"{icon}"
        f'<span class="folder-tree-name">{html.escape(label)}</span>'
        f'<span class="folder-tree-meta">{html.escape(meta)}</span>'
        f'<a class="folder-tree-view" href="{html.escape(url)}" title="Посмотреть папку">'
        f"{EYE_ICON}<span>Посмотреть папку</span></a>"
        "</summary>"
        f'<div class="folder-tree-body">{body}</div>'
        "</details>"
        "</li>"
    )


def _folder_nodes_html(item: dict, parent: str, links: FolderPageLinks, format_datetime) -> str:
    folders = item_subfolders(item)
    documents = normalize_documents_list(item.get("documents"))
    item_id = clean_cell_value(item.get("id"))
    parent_key = folder_key(parent)
    nodes = []
    for path in folders:
        if folder_key(parent_folder(path)) != parent_key:
            continue
        nodes.append(_node_html(
            label=folder_name(path),
            url=links.folder(item_id, path),
            documents=[
                document for document in documents
                if folder_key(document_relative_folder(document)) == folder_key(path)
            ],
            children_html=_folder_nodes_html(item, path, links, format_datetime),
            total_files=len(documents_below(documents, path)),
            is_subitem=False,
            format_datetime=format_datetime,
        ))
    return "".join(nodes)


def folder_tree_html(
    *,
    data: dict,
    item: dict,
    folder: str,
    links: FolderPageLinks,
    format_datetime,
) -> str:
    items = data.get("items") or []
    if not folder and not parent_id_of(item):
        # A top-level item: its folders are its subitems.
        nodes = []
        for child in children_of(items, clean_cell_value(item.get("id"))):
            child_documents = normalize_documents_list(child.get("documents"))
            nodes.append(_node_html(
                label=clean_cell_value(child.get("name")) or "Подпункт",
                url=links.folder(clean_cell_value(child.get("id"))),
                documents=[
                    document for document in child_documents
                    if not document_relative_folder(document)
                ],
                children_html=_folder_nodes_html(child, "", links, format_datetime),
                total_files=len(child_documents),
                is_subitem=True,
                format_datetime=format_datetime,
            ))
        content = "".join(nodes)
    else:
        content = _folder_nodes_html(item, folder, links, format_datetime)
    if not content:
        return ""
    return (
        '<section class="folder-tree" aria-labelledby="folderTreeTitle">'
        '<h2 id="folderTreeTitle" class="folder-tree-title">Папки</h2>'
        f'<ul class="folder-tree-root">{content}</ul>'
        "</section>"
    )


def public_folder_link(
    dialog_id: str,
    checklist_key: str,
    item: dict,
    relative_folder: str = "",
) -> dict:
    """Public Yandex link of the item folder or of a folder inside it.

    A folder that already has a public link keeps it; one without is
    published. A folder that is not on Yandex Disk yet (the edit session is
    not saved, or its files are still syncing) is never created from here.
    """
    from app.yandex_disk.client import (
        yandex_disk_get_resource_meta,
        yandex_disk_publish_path,
        yandex_disk_try_get_resource_meta,
    )

    if not project_yandex_available(dialog_id):
        return {"ok": False, "error": "Синхронизация с Яндекс.Диском для проекта отключена"}
    base = resolve_item_yandex_folder(dialog_id, checklist_key, item)
    if not base["path"]:
        return {"ok": False, "error": "Папка пункта на Яндекс.Диске ещё не создана"}
    if not relative_folder and base["url"]:
        return {"ok": True, "path": base["path"], "url": base["url"]}
    path = join_yandex_folder(base["path"], relative_folder)
    meta = yandex_disk_try_get_resource_meta(path)
    if not meta:
        return {
            "ok": False,
            "path": path,
            "error": (
                "Папки ещё нет на Яндекс.Диске — она появится после "
                "сохранения изменений и синхронизации файлов"
            ),
        }
    url = clean_cell_value(meta.get("public_url"))
    if not url:
        yandex_disk_publish_path(path)
        url = clean_cell_value(yandex_disk_get_resource_meta(path).get("public_url"))
    if not url:
        return {"ok": False, "path": path, "error": "Яндекс.Диск не выдал публичную ссылку на папку"}
    return {"ok": True, "path": path, "url": url, "published": True}


def move_targets(item: dict, folder: str) -> list[str]:
    """Folders of the same (sub)item where ``folder`` may be moved to."""
    from app.checklists.document_folders import is_within

    return [
        path for path in item_subfolders(item)
        if not is_within(path, folder)
        and folder_key(path) != folder_key(parent_folder(folder))
    ]


def item_move_targets(checklist_key: str, items: list[dict], item: dict) -> list[dict]:
    """Where an item or a subitem can be moved inside its checklist.

    A section makes it an item of that section; an item of a section makes
    it a subitem of that item. An item with its own subitems moves only
    between sections (items are two levels deep at most). «Не требуется»
    is reached through the status, not from here.
    """
    from app.checklists.utils import normalize_status

    config = get_checklist_config(checklist_key)
    item_id = clean_cell_value(item.get("id"))
    current_group = int(item.get("group") or 0)
    current_parent = parent_id_of(item)
    can_become_subitem = not children_of(items, item_id)
    targets: list[dict] = []
    for group in config.groups:
        if config.is_not_required_group(group.id):
            continue
        targets.append({
            "kind": "section",
            "groupId": group.id,
            "itemId": "",
            "label": clean_cell_value(config.get_group_title(group.id)) or f"Раздел {group.id}",
            "current": not current_parent and group.id == current_group,
        })
        if not can_become_subitem:
            continue
        hosts = sorted(
            (
                candidate for candidate in items
                if int(candidate.get("group") or 0) == group.id
                and not parent_id_of(candidate)
                and clean_cell_value(candidate.get("id")) != item_id
                and normalize_status(candidate.get("status")) != "Не требуется"
            ),
            key=lambda candidate: (
                int(candidate.get("order") or 100000),
                clean_cell_value(candidate.get("id")),
            ),
        )
        for host in hosts:
            host_id = clean_cell_value(host.get("id"))
            targets.append({
                "kind": "item",
                "groupId": group.id,
                "itemId": host_id,
                "label": clean_cell_value(host.get("name")) or "Пункт",
                "current": host_id == current_parent,
            })
    return targets
