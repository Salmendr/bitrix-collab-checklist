from app.checklists.models import ChecklistConfig
from app.checklists.definitions.helpers import groups_from_list


P_GROUPS = [
    {
        "id": 1,
        "title": "Общие данные",
        "items": [
            "Состав проекта",
            "Заполнение штампа",
        ],
    },
    {
        "id": 2,
        "title": "Стадия П",
        "items": [
            "ПЗ",
            "ПЗУ",
            "АР",
            "КР",
            "ИОС 1",
            "ИОС 2,3",
            "ИОС 4",
            "ИОС 5",
            "ТР",
            "ПОС",
            "ООС",
            "ПБ",
            "ТБЭ",
            "ОДИ",
        ],
    },
    {
        "id": 3,
        "title": "Экспертиза ПД",
        "items": [
            "Замечания экспертизы",
            "Ответы на замечания",
            "Заключение экспертизы",
        ],
    },
    {
        "id": 4,
        "title": "Не требуется",
        "items": [],
    },
]


STANDARD_P_YANDEX_FOLDER_SPECS = {
    "Состав проекта": {
        "alias": "p_project_composition",
        "folderName": "01_Состав проекта",
        "relativePath": "01_Общие данные/01_Состав проекта",
    },
    "Заполнение штампа": {
        "alias": "p_stamp_filling",
        "folderName": "02_Заполнение штампа",
        "relativePath": "01_Общие данные/02_Заполнение штампа",
    },

    "ПЗ": {
        "alias": "p_pz",
        "folderName": "01_ПЗ",
        "relativePath": "02_Стадия П/01_ПЗ",
    },
    "ПЗУ": {
        "alias": "p_pzu",
        "folderName": "02_ПЗУ",
        "relativePath": "02_Стадия П/02_ПЗУ",
    },
    "АР": {
        "alias": "p_ar",
        "folderName": "03_АР",
        "relativePath": "02_Стадия П/03_АР",
    },
    "КР": {
        "alias": "p_kr",
        "folderName": "04_КР",
        "relativePath": "02_Стадия П/04_КР",
    },

    "ИОС 1": {
        "alias": "p_ios_1",
        "folderName": "ИОС_1",
        "relativePath": "02_Стадия П/05_ИОС/ИОС_1",
    },
    "ИОС 2,3": {
        "alias": "p_ios_23",
        "folderName": "ИОС_2,3",
        "relativePath": "02_Стадия П/05_ИОС/ИОС_2,3",
    },
    "ИОС 4": {
        "alias": "p_ios_4",
        "folderName": "ИОС_4",
        "relativePath": "02_Стадия П/05_ИОС/ИОС_4",
    },
    "ИОС 5": {
        "alias": "p_ios_5",
        "folderName": "ИОС_5",
        "relativePath": "02_Стадия П/05_ИОС/ИОС_5",
    },

    "ТР": {
        "alias": "p_tr",
        "folderName": "06_ТР",
        "relativePath": "02_Стадия П/06_ТР",
    },
    "ПОС": {
        "alias": "p_pos",
        "folderName": "07_ПОС",
        "relativePath": "02_Стадия П/07_ПОС",
    },
    "ООС": {
        "alias": "p_oos",
        "folderName": "08_ООС",
        "relativePath": "02_Стадия П/08_ООС",
    },
    "ПБ": {
        "alias": "p_pb",
        "folderName": "09_ПБ",
        "relativePath": "02_Стадия П/09_ПБ",
    },
    "ТБЭ": {
        "alias": "p_tbe",
        "folderName": "10_ТБЭ",
        "relativePath": "02_Стадия П/10_ТБЭ",
    },
    "ОДИ": {
        "alias": "p_odi",
        "folderName": "11_ОДИ",
        "relativePath": "02_Стадия П/11_ОДИ",
    },

    "Замечания экспертизы": {
        "alias": "p_expertise_comments",
        "folderName": "01_Замечания экспертизы",
        "relativePath": "03_Экспертиза ПД/01_Замечания экспертизы",
    },
    "Ответы на замечания": {
        "alias": "p_expertise_answers",
        "folderName": "02_Ответы на замечания",
        "relativePath": "03_Экспертиза ПД/02_Ответы на замечания",
    },
    "Заключение экспертизы": {
        "alias": "p_expertise_conclusion",
        "folderName": "03_Заключение экспертизы",
        "relativePath": "03_Экспертиза ПД/03_Заключение экспертизы",
    },
}


CHECKLIST_CONFIG = ChecklistConfig(
    key="p",
    title="Стадия П",
    groups=groups_from_list(P_GROUPS),
    not_required_group_id=4,
    default_group_id=2,
    allow_custom_item_group_ids=(1, 2, 3),
    order=30,
    yandex_root_relative_path="02_Выдача документации/03_Стадия П",
    yandex_root_alias="p_stage_root",
    yandex_root_folder_name="03_Стадия П",
    reset_status_on_last_document_removed=True,
    standard_yandex_folder_specs=STANDARD_P_YANDEX_FOLDER_SPECS,
)