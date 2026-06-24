import sqlite3

from app.settings import DB_PATH


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS checklists (
            dialog_id TEXT PRIMARY KEY,
            title TEXT,
            data_json TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS project_storage_contexts (
            dialog_id TEXT PRIMARY KEY,
            project_id TEXT,
            project_name TEXT,
            provider TEXT,
            storage_mode_json TEXT,
            yandex_json TEXT,
            item_mappings_json TEXT,
            updated_at TEXT
        )
    """)

    conn.commit()
    conn.close()