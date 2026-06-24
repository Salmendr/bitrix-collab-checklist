import threading
import uuid
from datetime import datetime

from app.settings import EDIT_LOCK_TTL_SECONDS

from app.checklists.utils import (
    normalize_dialog_id,
    normalize_checklist_key,
)


ACTIVE_CHECKLIST_LOCKS = {}
ACTIVE_CHECKLIST_LOCKS_GUARD = threading.Lock()


def make_checklist_lock_key(dialog_id: str, checklist_key: str) -> str:
    return f"{normalize_dialog_id(dialog_id)}::{normalize_checklist_key(checklist_key)}"


def _cleanup_expired_checklist_locks(now_ts: float | None = None):
    now_ts = now_ts or datetime.now().timestamp()
    expired_keys = []

    for lock_key, lock_data in ACTIVE_CHECKLIST_LOCKS.items():
        updated_at = float(lock_data.get("updatedAtTs") or 0)
        if now_ts - updated_at > EDIT_LOCK_TTL_SECONDS:
            expired_keys.append(lock_key)

    for lock_key in expired_keys:
        ACTIVE_CHECKLIST_LOCKS.pop(lock_key, None)


def acquire_checklist_lock(
    dialog_id: str,
    checklist_key: str,
    user_id: str,
    user_name: str,
    lock_id: str,
) -> dict:
    dialog_id = normalize_dialog_id(dialog_id)
    checklist_key = normalize_checklist_key(checklist_key)
    user_id = str(user_id or "").strip()
    user_name = str(user_name or "").strip()
    lock_id = str(lock_id or "").strip()
    now = datetime.now()
    now_ts = now.timestamp()
    lock_key = make_checklist_lock_key(dialog_id, checklist_key)

    with ACTIVE_CHECKLIST_LOCKS_GUARD:
        _cleanup_expired_checklist_locks(now_ts)
        existing = ACTIVE_CHECKLIST_LOCKS.get(lock_key)

        if existing:
            existing_user_id = str(existing.get("userId") or "").strip()
            existing_lock_id = str(existing.get("lockId") or "").strip()

            # Тот же пользователь может безопасно продолжить свою сессию
            if existing_user_id and existing_user_id == user_id:
                effective_lock_id = existing_lock_id or lock_id or uuid.uuid4().hex

                ACTIVE_CHECKLIST_LOCKS[lock_key] = {
                    "lockId": effective_lock_id,
                    "dialogId": dialog_id,
                    "checklistKey": checklist_key,
                    "userId": user_id,
                    "userName": user_name or str(existing.get("userName") or "Неизвестный пользователь"),
                    "updatedAt": now.isoformat(),
                    "updatedAtTs": now_ts,
                }

                return {
                    "ok": True,
                    "owned": True,
                    "lockedByOther": False,
                    "lockId": effective_lock_id,
                    "dialogId": dialog_id,
                    "checklistKey": checklist_key,
                    "userId": user_id,
                    "userName": user_name or str(existing.get("userName") or "Неизвестный пользователь"),
                    "updatedAt": now.isoformat(),
                }

            # Чужой lockId наружу не отдаём
            return {
                "ok": True,
                "owned": False,
                "lockedByOther": True,
                "lockId": "",
                "dialogId": dialog_id,
                "checklistKey": checklist_key,
                "userId": existing_user_id,
                "userName": str(existing.get("userName") or "Другой сотрудник"),
                "updatedAt": str(existing.get("updatedAt") or ""),
            }

        if not lock_id:
            lock_id = uuid.uuid4().hex

        ACTIVE_CHECKLIST_LOCKS[lock_key] = {
            "lockId": lock_id,
            "dialogId": dialog_id,
            "checklistKey": checklist_key,
            "userId": user_id,
            "userName": user_name or "Неизвестный пользователь",
            "updatedAt": now.isoformat(),
            "updatedAtTs": now_ts,
        }

        return {
            "ok": True,
            "owned": True,
            "lockedByOther": False,
            "lockId": lock_id,
            "dialogId": dialog_id,
            "checklistKey": checklist_key,
            "userId": user_id,
            "userName": user_name or "Неизвестный пользователь",
            "updatedAt": now.isoformat(),
        }


def heartbeat_checklist_lock(
    dialog_id: str,
    checklist_key: str,
    user_id: str,
    user_name: str,
    lock_id: str,
) -> dict:
    dialog_id = normalize_dialog_id(dialog_id)
    checklist_key = normalize_checklist_key(checklist_key)
    user_id = str(user_id or "").strip()
    user_name = str(user_name or "").strip()
    lock_id = str(lock_id or "").strip()
    now = datetime.now()
    now_ts = now.timestamp()
    lock_key = make_checklist_lock_key(dialog_id, checklist_key)

    with ACTIVE_CHECKLIST_LOCKS_GUARD:
        _cleanup_expired_checklist_locks(now_ts)
        existing = ACTIVE_CHECKLIST_LOCKS.get(lock_key)

        if not existing:
            return {
                "ok": True,
                "owned": False,
                "lockedByOther": False,
                "lockExpired": True,
                "lockId": "",
                "dialogId": dialog_id,
                "checklistKey": checklist_key,
                "userId": "",
                "userName": "",
                "updatedAt": "",
            }

        existing_user_id = str(existing.get("userId") or "").strip()
        existing_lock_id = str(existing.get("lockId") or "").strip()

        if not lock_id or existing_lock_id != lock_id or existing_user_id != user_id:
            return {
                "ok": True,
                "owned": False,
                "lockedByOther": existing_user_id != user_id,
                "lockExpired": existing_user_id == user_id and existing_lock_id != lock_id,
                "lockId": "",
                "dialogId": dialog_id,
                "checklistKey": checklist_key,
                "userId": existing_user_id,
                "userName": str(existing.get("userName") or "Другой сотрудник"),
                "updatedAt": str(existing.get("updatedAt") or ""),
            }

        ACTIVE_CHECKLIST_LOCKS[lock_key] = {
            "lockId": existing_lock_id,
            "dialogId": dialog_id,
            "checklistKey": checklist_key,
            "userId": user_id,
            "userName": user_name or str(existing.get("userName") or "Неизвестный пользователь"),
            "updatedAt": now.isoformat(),
            "updatedAtTs": now_ts,
        }

        return {
            "ok": True,
            "owned": True,
            "lockedByOther": False,
            "lockExpired": False,
            "lockId": existing_lock_id,
            "dialogId": dialog_id,
            "checklistKey": checklist_key,
            "userId": user_id,
            "userName": user_name or str(existing.get("userName") or "Неизвестный пользователь"),
            "updatedAt": now.isoformat(),
        }


def release_checklist_lock(dialog_id: str, checklist_key: str, lock_id: str = "", user_id: str = "") -> dict:
    dialog_id = normalize_dialog_id(dialog_id)
    checklist_key = normalize_checklist_key(checklist_key)
    lock_id = str(lock_id or "").strip()
    user_id = str(user_id or "").strip()
    lock_key = make_checklist_lock_key(dialog_id, checklist_key)

    with ACTIVE_CHECKLIST_LOCKS_GUARD:
        _cleanup_expired_checklist_locks()
        existing = ACTIVE_CHECKLIST_LOCKS.get(lock_key)
        if not existing:
            return {
                "ok": True,
                "released": False,
                "dialogId": dialog_id,
                "checklistKey": checklist_key,
            }

        existing_user_id = str(existing.get("userId") or "").strip()
        existing_lock_id = str(existing.get("lockId") or "").strip()

        if not lock_id or not user_id:
            return {
                "ok": True,
                "released": False,
                "dialogId": dialog_id,
                "checklistKey": checklist_key,
                "lockedByOther": True,
                "userName": str(existing.get("userName") or ""),
            }

        if existing_lock_id != lock_id or existing_user_id != user_id:
            return {
                "ok": True,
                "released": False,
                "dialogId": dialog_id,
                "checklistKey": checklist_key,
                "lockedByOther": True,
                "userName": str(existing.get("userName") or ""),
            }

        ACTIVE_CHECKLIST_LOCKS.pop(lock_key, None)
        return {
            "ok": True,
            "released": True,
            "dialogId": dialog_id,
            "checklistKey": checklist_key,
        }