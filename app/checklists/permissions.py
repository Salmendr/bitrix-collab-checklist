from app.db import get_conn


DEFAULT_FILE_DELETE_ALLOWED_USER_IDS = {
    "18",
    "26",
    "56",
    "72",
    "100",
    "106",
    "108",
    "114",
    "116",
    "124",
    "138",
    "140",
    "222",
    "256",
    "280",
}

# Оставляем для обратной совместимости старых импортов.
# Новая рабочая логика ниже читает права из БД.
FILE_DELETE_ALLOWED_USER_IDS = set(DEFAULT_FILE_DELETE_ALLOWED_USER_IDS)

ARCHIVE_PERMANENT_DELETE_ADMIN_USER_IDS = {
    "18",
    "138",
}


def normalize_user_id(value) -> str:
    return str(value or "").strip()


def normalize_user_name(value) -> str:
    return str(value or "").strip()


def normalize_bool_flag(value) -> int:
    return 1 if bool(value) else 0


def ensure_user_permissions_table():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_permissions (
            user_id TEXT PRIMARY KEY,
            user_name TEXT,
            can_access_checklists INTEGER DEFAULT 1,
            can_delete_files INTEGER DEFAULT 0,
            source TEXT,
            updated_at TEXT
        )
    """)

    row = cur.execute(
        "SELECT COUNT(*) AS cnt FROM user_permissions"
    ).fetchone()

    if int(row["cnt"] if row else 0) == 0:
        for user_id in sorted(DEFAULT_FILE_DELETE_ALLOWED_USER_IDS, key=lambda x: int(x)):
            cur.execute("""
                INSERT OR IGNORE INTO user_permissions(
                    user_id,
                    user_name,
                    can_access_checklists,
                    can_delete_files,
                    source,
                    updated_at
                )
                VALUES (?, ?, 1, 1, 'seed', datetime('now'))
            """, (
                user_id,
                "",
            ))

    conn.commit()
    conn.close()


def list_user_permissions() -> list[dict]:
    ensure_user_permissions_table()

    conn = get_conn()
    rows = conn.execute("""
        SELECT
            user_id,
            user_name,
            can_access_checklists,
            can_delete_files,
            source,
            updated_at
        FROM user_permissions
        ORDER BY
            can_delete_files DESC,
            can_access_checklists DESC,
            CAST(user_id AS INTEGER),
            user_id
    """).fetchall()
    conn.close()

    return [
        {
            "userId": normalize_user_id(row["user_id"]),
            "userName": normalize_user_name(row["user_name"]),
            "canAccessChecklists": bool(row["can_access_checklists"]),
            "canDeleteFiles": bool(row["can_delete_files"]),
            "source": normalize_user_name(row["source"]),
            "updatedAt": normalize_user_name(row["updated_at"]),
        }
        for row in rows
    ]


def upsert_user_permission(
    user_id,
    user_name: str = "",
    can_access_checklists: bool = True,
    can_delete_files: bool = False,
    source: str = "admin",
) -> dict:
    ensure_user_permissions_table()

    user_id = normalize_user_id(user_id)
    user_name = normalize_user_name(user_name)

    if not user_id:
        raise ValueError("userId is required")

    conn = get_conn()
    conn.execute("""
        INSERT INTO user_permissions(
            user_id,
            user_name,
            can_access_checklists,
            can_delete_files,
            source,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(user_id) DO UPDATE SET
            user_name=excluded.user_name,
            can_access_checklists=excluded.can_access_checklists,
            can_delete_files=excluded.can_delete_files,
            source=excluded.source,
            updated_at=datetime('now')
    """, (
        user_id,
        user_name,
        normalize_bool_flag(can_access_checklists),
        normalize_bool_flag(can_delete_files),
        normalize_user_name(source) or "admin",
    ))
    conn.commit()
    conn.close()

    return get_user_permission(user_id) or {
        "userId": user_id,
        "userName": user_name,
        "canAccessChecklists": bool(can_access_checklists),
        "canDeleteFiles": bool(can_delete_files),
    }


def delete_user_permission(user_id) -> bool:
    ensure_user_permissions_table()

    user_id = normalize_user_id(user_id)
    if not user_id:
        return False

    conn = get_conn()
    cur = conn.execute(
        "DELETE FROM user_permissions WHERE user_id = ?",
        (user_id,),
    )
    conn.commit()
    deleted = int(cur.rowcount or 0) > 0
    conn.close()

    return deleted


def get_user_permission(user_id) -> dict | None:
    ensure_user_permissions_table()

    user_id = normalize_user_id(user_id)
    if not user_id:
        return None

    conn = get_conn()
    row = conn.execute("""
        SELECT
            user_id,
            user_name,
            can_access_checklists,
            can_delete_files,
            source,
            updated_at
        FROM user_permissions
        WHERE user_id = ?
    """, (
        user_id,
    )).fetchone()
    conn.close()

    if not row:
        return None

    return {
        "userId": normalize_user_id(row["user_id"]),
        "userName": normalize_user_name(row["user_name"]),
        "canAccessChecklists": bool(row["can_access_checklists"]),
        "canDeleteFiles": bool(row["can_delete_files"]),
        "source": normalize_user_name(row["source"]),
        "updatedAt": normalize_user_name(row["updated_at"]),
    }


def get_file_delete_allowed_user_ids() -> set[str]:
    ensure_user_permissions_table()

    conn = get_conn()
    rows = conn.execute("""
        SELECT user_id
        FROM user_permissions
        WHERE can_delete_files = 1
    """).fetchall()
    conn.close()

    return {
        normalize_user_id(row["user_id"])
        for row in rows
        if normalize_user_id(row["user_id"])
    }


def get_checklist_access_allowed_user_ids() -> set[str]:
    ensure_user_permissions_table()

    conn = get_conn()
    rows = conn.execute("""
        SELECT user_id
        FROM user_permissions
        WHERE can_access_checklists = 1
    """).fetchall()
    conn.close()

    return {
        normalize_user_id(row["user_id"])
        for row in rows
        if normalize_user_id(row["user_id"])
    }


def can_user_delete_files(user_id: str) -> bool:
    return normalize_user_id(user_id) in get_file_delete_allowed_user_ids()


def can_user_access_checklists(user_id: str) -> bool:
    return normalize_user_id(user_id) in get_checklist_access_allowed_user_ids()


def get_archive_permanent_delete_admin_user_ids() -> set[str]:
    return set(
        ARCHIVE_PERMANENT_DELETE_ADMIN_USER_IDS
    )


def can_user_permanently_delete_archive(
    user_id: str,
) -> bool:
    return (
        normalize_user_id(user_id)
        in ARCHIVE_PERMANENT_DELETE_ADMIN_USER_IDS
    )

