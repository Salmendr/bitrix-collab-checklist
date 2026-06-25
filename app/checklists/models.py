from dataclasses import dataclass, field
from typing import Mapping


@dataclass(frozen=True)
class ChecklistGroupConfig:
    id: int
    title: str
    items: tuple[str, ...] = ()


@dataclass(frozen=True)
class ChecklistConfig:
    key: str
    title: str
    groups: tuple[ChecklistGroupConfig, ...]
    not_required_group_id: int
    default_group_id: int
    allow_custom_item_group_ids: tuple[int, ...]
    reset_status_on_last_document_removed: bool = True
    standard_yandex_folder_specs: Mapping[str, Mapping[str, str]] = field(default_factory=dict)

    def group_ids(self) -> tuple[int, ...]:
        return tuple(group.id for group in self.groups)

    def active_group_ids(self) -> tuple[int, ...]:
        return tuple(
            group.id
            for group in self.groups
            if group.id != self.not_required_group_id
        )

    def is_not_required_group(self, group_id: int) -> bool:
        return int(group_id or 0) == self.not_required_group_id

    def is_active_group(self, group_id: int) -> bool:
        return not self.is_not_required_group(group_id)

    def get_group(self, group_id: int) -> ChecklistGroupConfig | None:
        group_id = int(group_id or 0)
        for group in self.groups:
            if group.id == group_id:
                return group
        return None

    def get_group_title(self, group_id: int) -> str:
        group = self.get_group(group_id)
        return group.title if group else ""

    def get_items_for_group(self, group_id: int) -> tuple[str, ...]:
        group = self.get_group(group_id)
        return group.items if group else ()

    def has_custom_item_group(self, group_id: int) -> bool:
        return int(group_id or 0) in self.allow_custom_item_group_ids