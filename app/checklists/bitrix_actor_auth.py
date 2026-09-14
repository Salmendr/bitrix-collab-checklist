"""Verify the caller for the new scheduled-message settings API."""
from urllib.parse import urlsplit
import requests
import app.settings as settings
from app.checklists.edit_sessions import EditSessionPermissionError


def verify_bitrix_actor(request, expected_user_id: str) -> str:
    token = request.headers.get('X-Bitrix-Access-Token', '').strip()
    if not token or len(token) > 4096:
        raise EditSessionPermissionError('Настройте оповещения из приложения внутри Битрикс24')
    # Never accept a client-supplied domain or webhook as the authentication authority.
    portal = urlsplit(settings.BITRIX_TECH_WEBHOOK_URL)
    if portal.scheme != 'https' or not portal.hostname or portal.username or portal.password:
        raise EditSessionPermissionError('Не настроен доверенный портал Битрикс24 для проверки пользователя')
    try:
        response = requests.post('https://' + portal.netloc + '/rest/user.current.json',
                                 data={'auth': token}, timeout=15, allow_redirects=False)
        data = response.json() if response.status_code == 200 else {}
        user = data.get('result') if isinstance(data, dict) else None
    except Exception:
        raise EditSessionPermissionError('Не удалось подтвердить пользователя в Битрикс24. Повторите попытку.') from None
    if not isinstance(user, dict) or str(user.get('ID') or '') != str(expected_user_id) or user.get('ACTIVE') in (False, 'N', 'false', 0):
        raise EditSessionPermissionError('Битрикс24 не подтвердил текущего пользователя')
    return str(user['ID'])
