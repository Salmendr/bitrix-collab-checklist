import os
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent

# Runtime-хранилище проекта.
# Не переносим, не переименовываем, не создаём вручную в рамках рефакторинга.
VOLUME_DIR = BASE_DIR / "Volume"

DB_DIR = VOLUME_DIR / "db"
DB_PATH = str(DB_DIR / "app.db")

APP_PORTAL_PATH = (os.getenv("APP_PORTAL_PATH", "/marketplace/app/80/") or "/marketplace/app/80/").strip()
APP_BASE_PATH = (os.getenv("APP_BASE_PATH", "") or "").strip()
PUBLIC_APP_BASE_URL = (os.getenv("PUBLIC_APP_BASE_URL", "") or "").strip()

TECH_USER_ID = int(os.getenv("TECH_USER_ID", "138"))
BITRIX_TECH_WEBHOOK_URL = os.getenv("BITRIX_TECH_WEBHOOK_URL", "").strip()
N8N_SHARED_TOKEN = os.getenv("N8N_SHARED_TOKEN", "").strip()

YANDEX_DISK_OAUTH_TOKEN = os.getenv("YANDEX_DISK_OAUTH_TOKEN", "").strip()
YANDEX_DISK_API_BASE = "https://cloud-api.yandex.net/v1/disk"

UPLOAD_ROOT = VOLUME_DIR / "uploads"
CHECKLIST_UPLOAD_ROOT = UPLOAD_ROOT / "checklists"

DEBUG_DIR = BASE_DIR / "debug"
DEBUG_LOG_PATH = DEBUG_DIR / "close_popup.log"

EDIT_LOCK_TTL_SECONDS = 45
EDIT_LOCK_HEARTBEAT_SECONDS = 15

def ensure_runtime_directories():
    """
    Создаёт только штатные runtime-директории приложения.

    Это не меняет архитектуру Volume:
    - база остаётся в Volume/db/app.db;
    - загрузки остаются в Volume/uploads;
    - app/ остаётся только кодом.
    """
    VOLUME_DIR.mkdir(parents=True, exist_ok=True)
    DB_DIR.mkdir(parents=True, exist_ok=True)
    UPLOAD_ROOT.mkdir(parents=True, exist_ok=True)
    CHECKLIST_UPLOAD_ROOT.mkdir(parents=True, exist_ok=True)
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)