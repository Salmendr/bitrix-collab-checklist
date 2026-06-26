from app.checklists.models import ChecklistGroupConfig


def groups_from_id_dict(raw_groups: dict) -> tuple[ChecklistGroupConfig, ...]:
    return tuple(
        ChecklistGroupConfig(
            id=int(group_id),
            title=str(group_data.get("title") or ""),
            items=tuple(group_data.get("items") or ()),
        )
        for group_id, group_data in raw_groups.items()
    )


def groups_from_list(raw_groups: list[dict]) -> tuple[ChecklistGroupConfig, ...]:
    return tuple(
        ChecklistGroupConfig(
            id=int(group.get("id") or 0),
            title=str(group.get("title") or ""),
            items=tuple(group.get("items") or ()),
        )
        for group in raw_groups
    )