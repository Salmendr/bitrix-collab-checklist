import requests

from app.settings import BITRIX_TECH_WEBHOOK_URL


def bitrix_rest_call(domain: str, method: str, access_token: str, payload: dict):
    url = f"https://{domain}/rest/{method}.json"

    response = requests.post(
        url,
        data={**payload, "auth": access_token},
        timeout=30,
    )

    try:
        return response.json()
    except Exception:
        return {
            "http_status": response.status_code,
            "text": response.text,
        }


def bitrix_webhook_call(method: str, payload: dict):
    if not BITRIX_TECH_WEBHOOK_URL:
        return {
            "error": "TECH_WEBHOOK_NOT_CONFIGURED",
            "error_description": "BITRIX_TECH_WEBHOOK_URL is empty",
        }

    base = BITRIX_TECH_WEBHOOK_URL.rstrip("/")
    url = f"{base}/{method}.json"

    response = requests.post(url, data=payload, timeout=30)

    try:
        return response.json()
    except Exception:
        return {
            "http_status": response.status_code,
            "text": response.text,
        }