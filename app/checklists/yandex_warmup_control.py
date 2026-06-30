import threading

from app.checklists.utils import normalize_dialog_id


YANDEX_WARMUP_STOP_REQUESTS: set[str] = set()
YANDEX_WARMUP_STOP_GUARD = threading.Lock()


def request_yandex_warmup_stop(dialog_id: str) -> dict:
    dialog_id = normalize_dialog_id(dialog_id)

    if not dialog_id:
        return {
            "ok": False,
            "stopRequested": False,
            "error": "dialogId is required",
        }

    with YANDEX_WARMUP_STOP_GUARD:
        YANDEX_WARMUP_STOP_REQUESTS.add(dialog_id)

    return {
        "ok": True,
        "stopRequested": True,
        "dialogId": dialog_id,
    }


def clear_yandex_warmup_stop(dialog_id: str):
    dialog_id = normalize_dialog_id(dialog_id)

    if not dialog_id:
        return

    with YANDEX_WARMUP_STOP_GUARD:
        YANDEX_WARMUP_STOP_REQUESTS.discard(dialog_id)


def is_yandex_warmup_stop_requested(dialog_id: str) -> bool:
    dialog_id = normalize_dialog_id(dialog_id)

    if not dialog_id:
        return False

    with YANDEX_WARMUP_STOP_GUARD:
        return dialog_id in YANDEX_WARMUP_STOP_REQUESTS


def get_yandex_warmup_stop_state() -> dict:
    with YANDEX_WARMUP_STOP_GUARD:
        return {
            "stopRequestedDialogIds": sorted(YANDEX_WARMUP_STOP_REQUESTS),
        }