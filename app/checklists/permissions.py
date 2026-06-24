FILE_DELETE_ALLOWED_USER_IDS = {
    "108",  # Анатолий Черняков
    "106",  # Юлий Продан
    "114",  # Алексей Кузьмин
    "116",  # Дмитрий Сорюс
    "72",   # Евгения Пулина
    "56",   # Евгений Фролов
    "26",   # Никита Радонежский
    "138",  # Сергей Жигарь
    "18",   # Олег Рашов
    "256",  # Сергей Карман
    "140",  # Василий Пастухов
    "280",  # Роман Фомин
    "124",  # Полина Тихонова
    "222",  # Вероника Варганова
}


def normalize_user_id(value) -> str:
    return str(value or "").strip()


def can_user_delete_files(user_id: str) -> bool:
    return normalize_user_id(user_id) in FILE_DELETE_ALLOWED_USER_IDS