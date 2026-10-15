"""«Руководители»: who sees the leader buttons of a project («Перейти к
Договору», «Графики»).

Leaders of a project are:

* the administrators of the application;
* the permanent leaders (edited in the admin panel);
* everyone in the responsible fields of the project objects (Ответственный,
  Руководитель рабочей группы, Ответственный за Концепцию/Эскиз of every
  object, which also fill the ГИП fields of the checklists) and the
  Ответственный of their contracts.
"""
from __future__ import annotations

from datetime import datetime

from app.db import get_conn
from app.logging_utils import write_debug_log
from app.checklists.utils import clean_cell_value


DEFAULT_PERMANENT_LEADERS = (
    ("108", "Черняков Анатолий"),
    ("256", "Карман Сергей"),
    ("152", "Тен Виталий"),
    ("24", "Севастьян Александр"),
)


class LeaderError(RuntimeError):
    """A refused change; the message is for the user."""


def _ensure_table(conn) -> None:
    exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'app_leaders'"
    ).fetchone()
    if exists:
        return
    conn.execute("""
        CREATE TABLE IF NOT EXISTS app_leaders (
            user_id TEXT PRIMARY KEY,
            user_name TEXT,
            added_by_id TEXT,
            added_at TEXT
        )
    """)
    # The first start fills the list once; later the admins edit it.
    now = datetime.now().isoformat()
    conn.executemany(
        "INSERT OR IGNORE INTO app_leaders(user_id, user_name, added_by_id, added_at) VALUES (?, ?, ?, ?)",
        [(user_id, name, "default", now) for user_id, name in DEFAULT_PERMANENT_LEADERS],
    )
    conn.commit()


def list_permanent_leaders() -> list[dict]:
    conn = get_conn()
    try:
        _ensure_table(conn)
        rows = conn.execute(
            "SELECT user_id, user_name, added_by_id, added_at FROM app_leaders ORDER BY user_name, user_id"
        ).fetchall()
    finally:
        conn.close()
    return [
        {
            "userId": clean_cell_value(row["user_id"]),
            "name": clean_cell_value(row["user_name"]),
            "addedBy": clean_cell_value(row["added_by_id"]),
            "addedAt": clean_cell_value(row["added_at"]),
        }
        for row in rows
    ]


def permanent_leader_ids() -> set[str]:
    return {leader["userId"] for leader in list_permanent_leaders()}


def add_permanent_leader(user_id: str, user_name: str = "", *, acting_user_id: str = "") -> list[dict]:
    user = clean_cell_value(user_id)
    if not user.isdigit() or int(user) <= 0:
        raise LeaderError("Укажите ID сотрудника Битрикс24")
    name = clean_cell_value(user_name)
    if not name:
        from app.checklists.bitrix_users import get_cached_bitrix_user

        name = clean_cell_value((get_cached_bitrix_user(user) or {}).get("name")) or f"ID {user}"
    conn = get_conn()
    try:
        _ensure_table(conn)
        conn.execute(
            """
            INSERT INTO app_leaders(user_id, user_name, added_by_id, added_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET user_name = excluded.user_name
            """,
            (user, name, clean_cell_value(acting_user_id), datetime.now().isoformat()),
        )
        conn.commit()
    finally:
        conn.close()
    write_debug_log("app_leader_added", {"userId": user, "name": name, "actingUserId": acting_user_id})
    return list_permanent_leaders()


def remove_permanent_leader(user_id: str, *, acting_user_id: str = "") -> list[dict]:
    conn = get_conn()
    try:
        _ensure_table(conn)
        conn.execute("DELETE FROM app_leaders WHERE user_id = ?", (clean_cell_value(user_id),))
        conn.commit()
    finally:
        conn.close()
    write_debug_log("app_leader_removed", {"userId": user_id, "actingUserId": acting_user_id})
    return list_permanent_leaders()


def field_leader_ids(dialog_id: str) -> list[str]:
    """People of the responsible fields of the project objects and contracts."""
    from app.checklists.project_objects import responsible_user_ids

    try:
        return responsible_user_ids(dialog_id)
    except Exception as exc:
        write_debug_log("project_field_leaders_failed", {"dialogId": dialog_id, "error": str(exc)})
        return []


def project_leader_ids(dialog_id: str) -> list[str]:
    """Leaders of the project, the administrators aside."""
    result = sorted(permanent_leader_ids() | set(field_leader_ids(dialog_id)), key=lambda value: (len(value), value))
    return result


def is_project_leader(dialog_id: str, user_id: str) -> bool:
    from app.checklists.project_phases import is_project_admin

    user = clean_cell_value(user_id)
    if not user:
        return False
    return is_project_admin(user) or user in project_leader_ids(dialog_id)
