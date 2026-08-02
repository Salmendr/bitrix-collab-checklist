from __future__ import annotations

import re
from typing import Iterable

from app.checklists.utils import clean_cell_value
from app.checklists.yandex_folders import sanitize_yandex_folder_name


MAX_ITEM_NAME_LENGTH = 160
FORBIDDEN_ITEM_NAME_RE = re.compile(r'[<>:"/\\|?*]')
CONTROL_CHARACTER_RE = re.compile(r'[\x00-\x1f\x7f]')
SUFFIX_RE = re.compile(r'^(.*?)(?:\s+\((\d+)\))?$')


def normalize_item_name_input(value: object) -> str:
    return re.sub(r'\s+', ' ', clean_cell_value(value)).strip()


def normalize_item_name_key(value: object) -> str:
    return normalize_item_name_input(value).casefold()


def normalize_item_folder_key(value: object) -> str:
    return sanitize_yandex_folder_name(
        normalize_item_name_input(value)
    ).casefold()


def validate_item_name(value: object) -> str:
    name = normalize_item_name_input(value)
    if not name:
        raise ValueError('name is required')
    if len(name) > MAX_ITEM_NAME_LENGTH:
        raise ValueError(
            f'Название пункта не должно быть длиннее {MAX_ITEM_NAME_LENGTH} символов'
        )
    if name in {'.', '..'}:
        raise ValueError('Недопустимое название пункта')
    if CONTROL_CHARACTER_RE.search(name):
        raise ValueError('Название пункта содержит управляющие символы')
    if FORBIDDEN_ITEM_NAME_RE.search(name):
        raise ValueError(
            'Название пункта содержит запрещённые символы: < > : " / \\ | ? *'
        )
    return name


def _base_name_for_suffix(value: str) -> str:
    match = SUFFIX_RE.match(normalize_item_name_input(value))
    if not match:
        return normalize_item_name_input(value)
    base = normalize_item_name_input(match.group(1))
    return base or normalize_item_name_input(value)


def choose_available_item_name(
    requested_name: object,
    items: Iterable[dict],
    *,
    group_id: int,
    exclude_item_id: str = '',
) -> dict:
    requested = validate_item_name(requested_name)
    normalized_exclude_id = clean_cell_value(exclude_item_id)
    used_name_keys: set[str] = set()
    used_folder_keys: set[str] = set()

    for raw_item in items or []:
        item = raw_item if isinstance(raw_item, dict) else {}
        if clean_cell_value(item.get('id')) == normalized_exclude_id:
            continue
        try:
            item_group_id = int(item.get('group') or 0)
        except (TypeError, ValueError):
            item_group_id = 0
        if item_group_id != int(group_id or 0):
            continue
        item_name = normalize_item_name_input(item.get('name'))
        if not item_name:
            continue
        used_name_keys.add(normalize_item_name_key(item_name))
        used_folder_keys.add(normalize_item_folder_key(item_name))

    base_name = _base_name_for_suffix(requested)
    for index in range(1, 10000):
        candidate = requested if index == 1 else f'{base_name} ({index})'
        candidate = validate_item_name(candidate)
        if normalize_item_name_key(candidate) in used_name_keys:
            continue
        if normalize_item_folder_key(candidate) in used_folder_keys:
            continue
        return {
            'requestedName': requested,
            'name': candidate,
            'adjusted': candidate != requested,
            'suffixNumber': 1 if candidate == requested else index,
            'normalizedName': normalize_item_name_key(candidate),
            'normalizedFolderName': normalize_item_folder_key(candidate),
        }

    raise RuntimeError('Не удалось подобрать свободное название пункта')
