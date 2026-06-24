from app.checklists.definitions.id import (
    ID_GROUPS,
    STANDARD_ID_YANDEX_FOLDER_SPECS,
)
from app.checklists.definitions.opr import (
    OPR_GROUPS,
    STANDARD_OPR_YANDEX_FOLDER_SPECS,
)
from app.checklists.definitions.concept import (
    CONCEPT_GROUPS,
    STANDARD_CONCEPT_YANDEX_FOLDER_SPECS,
)    

PROJECT_CHECKLISTS = [
    {"key": "id", "title": "Чек-лист ИД"},
    {"key": "opr", "title": "Чек-лист ОПР"},
    {"key": "concept", "title": "Чек-лист Концепция"},
]

CHECKLIST_TITLES = {
    "id": "Чек-лист ИД",
    "opr": "Чек-лист ОПР",
    "concept": "Чек-лист Концепция",
}

STATUS_OPTIONS = [
    "",
    "Есть",
    "Нет",
    "Не требуется",
]

PRIORITY_OPTIONS = ["white", "green", "gray"]


def get_project_checklists():
    return [dict(item) for item in PROJECT_CHECKLISTS]


def get_checklist_title(checklist_key: str) -> str:
    key = str(checklist_key or "").strip().lower()
    return CHECKLIST_TITLES.get(key, "Чек-лист ИД")