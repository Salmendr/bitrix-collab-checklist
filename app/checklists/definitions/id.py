# Definitions for checklist "ИД".

from app.checklists.models import ChecklistConfig
from app.checklists.definitions.helpers import groups_from_id_dict

ID_GROUPS = {
    1: {
        "title": "ИД",
        "items": [
            "ППТ",
            "Выписка ЕГРН",
            "ГПЗУ",
            "Тех задание",
            "ИГДИ",
            "ИГИ",
            "ИЭИ",
            "ИГМИ",
            "ИГИ СМР",
        ],
    },
    2: {
        "title": "ТУ",
        "items": [
            "ТУ Тепловые сети",
            "ТУ Водоснабжение",
            "ТУ Бытовая канализация",
            "ТУ Электроснабжение",
            "ТУ Сети связи",
            "ТУ Наружное освещение",
            "ТУ Ливневая канализация",
            "ТУ Газоснабжение",
        ],
    },
    3: {
        "title": "Прочее",
        "items": [
            "Согласование с Аэропортом",
            "Примыкание к УДС",
            "Порубочный лист",
            "Справка вывоза мусора",
            "СТУ",
            "Сокращение ОКН",
            "Расположение пожарных гидрантов",
        ],
    },
    4: {
        "title": "Не требуется",
        "items": [],
    },
}

STANDARD_ID_YANDEX_FOLDER_SPECS = {
    "ППТ": {
        "groupId": 1,
        "alias": "ppt",
        "folderName": "06_ППТ",
        "relativePath": "06_ППТ",
    },
    "Выписка ЕГРН": {
        "groupId": 1,
        "alias": "egrn_extract",
        "folderName": "01_Выписка ЕГРН",
        "relativePath": "02_Градплан/01_Выписка ЕГРН",
    },
    "ГПЗУ": {
        "groupId": 1,
        "alias": "gpzu",
        "folderName": "02_ГПЗУ",
        "relativePath": "02_Градплан/02_ГПЗУ",
    },
    "Тех задание": {
        "groupId": 1,
        "alias": "tz_design",
        "folderName": "01_ТЗ на проектирование",
        "relativePath": "01_ТЗ на проектирование",
    },
    "ИГДИ": {
        "groupId": 1,
        "alias": "survey_igdi",
        "folderName": "01_ИГДИ",
        "relativePath": "04_Изыскания/01_ИГДИ",
    },
    "ИГИ": {
        "groupId": 1,
        "alias": "survey_igi",
        "folderName": "02_ИГИ",
        "relativePath": "04_Изыскания/02_ИГИ",
    },
    "ИЭИ": {
        "groupId": 1,
        "alias": "survey_iei",
        "folderName": "03_ИЭИ",
        "relativePath": "04_Изыскания/03_ИЭИ",
    },
    "ИГМИ": {
        "groupId": 1,
        "alias": "survey_igmi",
        "folderName": "04_ИГМИ",
        "relativePath": "04_Изыскания/04_ИГМИ",
    },
    "ИГИ СМР": {
        "groupId": 1,
        "alias": "survey_igi_smr",
        "folderName": "05_ИГИ СМР",
        "relativePath": "04_Изыскания/05_ИГИ СМР",
    },
    "ТУ Тепловые сети": {
        "groupId": 2,
        "alias": "tu_heat",
        "folderName": "01_ТУ Тепловые сети",
        "relativePath": "03_ТУ/01_ТУ Тепловые сети",
    },
    "ТУ Водоснабжение": {
        "groupId": 2,
        "alias": "tu_water",
        "folderName": "02_ТУ Водоснабжение",
        "relativePath": "03_ТУ/02_ТУ Водоснабжение",
    },
    "ТУ Бытовая канализация": {
        "groupId": 2,
        "alias": "tu_sewer",
        "folderName": "03_ТУ Бытовая канализация",
        "relativePath": "03_ТУ/03_ТУ Бытовая канализация",
    },
    "ТУ Электроснабжение": {
        "groupId": 2,
        "alias": "tu_power",
        "folderName": "04_ТУ Электроснабжение",
        "relativePath": "03_ТУ/04_ТУ Электроснабжение",
    },
    "ТУ Сети связи": {
        "groupId": 2,
        "alias": "tu_comm",
        "folderName": "05_ТУ Сети связи",
        "relativePath": "03_ТУ/05_ТУ Сети связи",
    },
    "ТУ Наружное освещение": {
        "groupId": 2,
        "alias": "tu_light",
        "folderName": "06_ТУ Наружное освещение",
        "relativePath": "03_ТУ/06_ТУ Наружное освещение",
    },
    "ТУ Ливневая канализация": {
        "groupId": 2,
        "alias": "tu_storm",
        "folderName": "07_ТУ Ливневая канализация",
        "relativePath": "03_ТУ/07_ТУ Ливневая канализация",
    },
    "ТУ Газоснабжение": {
        "groupId": 2,
        "alias": "tu_gas",
        "folderName": "08_ТУ Газоснабжение",
        "relativePath": "03_ТУ/08_ТУ Газоснабжение",
    },
    "Согласование с Аэропортом": {
        "groupId": 3,
        "alias": "ref_airport",
        "folderName": "01_Согласование с аэропортом",
        "relativePath": "05_Справки/01_Согласование с аэропортом",
    },
    "Примыкание к УДС": {
        "groupId": 3,
        "alias": "ref_uds",
        "folderName": "02_Примыкание к УДС",
        "relativePath": "05_Справки/02_Примыкание к УДС",
    },
    "Порубочный лист": {
        "groupId": 3,
        "alias": "ref_cutting",
        "folderName": "03_Порубочный лист",
        "relativePath": "05_Справки/03_Порубочный лист",
    },
    "Справка вывоза мусора": {
        "groupId": 3,
        "alias": "ref_waste",
        "folderName": "04_Справка вывоза мусора",
        "relativePath": "05_Справки/04_Справка вывоза мусора",
    },
    "Расположение пожарных гидрантов": {
        "groupId": 3,
        "alias": "ref_hydrants",
        "folderName": "05_Расположение пожарных гидрантов",
        "relativePath": "05_Справки/05_Расположение пожарных гидрантов",
    },
    "Сокращение ОКН": {
        "groupId": 3,
        "alias": "okn",
        "folderName": "07_ОКН",
        "relativePath": "07_ОКН",
    },
    "СТУ": {
        "groupId": 3,
        "alias": "stu",
        "folderName": "08_СТУ",
        "relativePath": "08_СТУ",
    },
}

CHECKLIST_CONFIG = ChecklistConfig(
    key="id",
    title="Чек-лист ИД",
    groups=groups_from_id_dict(ID_GROUPS),
    not_required_group_id=4,
    default_group_id=3,
    allow_custom_item_group_ids=(1, 2, 3),
    order=10,
    yandex_root_relative_path="00_Исходные данные/01_ИРД",
    yandex_root_alias="id_stage_root",
    yandex_root_folder_name="01_ИРД",
    reset_status_on_last_document_removed=True,
    standard_yandex_folder_specs=STANDARD_ID_YANDEX_FOLDER_SPECS,
)