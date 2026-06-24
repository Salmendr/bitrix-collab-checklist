import json
from datetime import datetime

from app.settings import DEBUG_DIR, DEBUG_LOG_PATH


def write_debug_log(event: str, payload: dict):
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)

    record = {
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "event": event,
        "payload": payload,
    }

    with open(DEBUG_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")