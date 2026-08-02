import html
import uuid
from urllib.parse import quote

from app.settings import APP_BASE_PATH

from app.checklists.utils import (
    clean_cell_value,
    format_file_size,
)

from app.checklists.documents import (
    normalize_archive_versions,
    normalize_detached_archive_series,
)

from app.ui.shell import normalize_base_path


ARCHIVE_DELETE_BLOCKED_STATUSES = {
    "pending_after_replacement_sync",
    "queued",
    "running",
}


def build_archive_file_url(
    dialog_id: str,
    checklist_key: str,
    item_id: str,
    archive_version_id: str,
    series_id: str,
    download: bool = False,
) -> str:
    base_path = normalize_base_path(APP_BASE_PATH)

    url = (
        f"{base_path}/api/checklist/archive-file"
        f"?dialogId={quote(dialog_id, safe='')}"
        f"&checklistKey={quote(checklist_key, safe='')}"
        f"&itemId={quote(item_id, safe='')}"
        f"&archiveVersionId={quote(archive_version_id, safe='')}"
        f"&seriesId={quote(series_id, safe='')}"
    )

    if download:
        url += "&download=1"

    return url


def get_archive_yandex_status_presentation(
    value: str,
) -> tuple[str, str, str, str]:
    status = clean_cell_value(value).lower()

    presentations = {
        "pending_after_replacement_sync": (
            "Ожидает новую версию",
            "#92400e",
            "#fffbeb",
            "Старая копия на Яндекс.Диске будет обработана "
            "после синхронизации новой версии",
        ),
        "queued": (
            "Удаление с Яндекса в очереди",
            "#92400e",
            "#fffbeb",
            "Удаление старой копии с Яндекс.Диска поставлено в очередь",
        ),
        "running": (
            "Удаляется с Яндекса",
            "#175cd3",
            "#eff8ff",
            "Старая копия удаляется с Яндекс.Диска",
        ),
        "deleted": (
            "Удалено с Яндекса",
            "#027a48",
            "#ecfdf3",
            "Старая копия удалена с Яндекс.Диска",
        ),
        "not_required_same_path": (
            "Перезаписано на Яндексе",
            "#027a48",
            "#ecfdf3",
            "Новая версия записана по тому же пути. "
            "Отдельное удаление не требуется",
        ),
        "not_required": (
            "",
            "",
            "",
            "",
        ),
        "error": (
            "Ошибка удаления с Яндекса",
            "#b42318",
            "#fef3f2",
            "Старую копию не удалось удалить с Яндекс.Диска",
        ),
        "cancelled": (
            "Удаление отменено",
            "#475467",
            "#f2f4f7",
            "Удаление старой копии с Яндекс.Диска отменено",
        ),
        "skipped": (
            "Удаление пропущено",
            "#475467",
            "#f2f4f7",
            "Удаление старой копии с Яндекс.Диска пропущено",
        ),
    }

    return presentations.get(
        status,
        (
            "",
            "",
            "",
            "",
        ),
    )


def get_archive_yandex_status_class(value: str) -> str:
    status = clean_cell_value(value).lower()

    allowed = {
        "pending_after_replacement_sync",
        "queued",
        "running",
        "deleted",
        "not_required_same_path",
        "not_required",
        "error",
        "cancelled",
        "skipped",
    }

    if (
        status not in allowed
        or status == "not_required"
    ):
        return ""

    return status.replace("_", "-")


def build_archive_series_rows_html(
    dialog_id: str,
    checklist_key: str,
    item_id: str,
    series_id: str,
    archive_versions,
    panel_title: str,
    panel_id: str,
    format_datetime,
    detached: bool = False,
) -> str:
    versions = normalize_archive_versions(
        archive_versions,
        series_id=series_id,
    )

    if not versions:
        return ""

    versions = list(reversed(versions))
    version_rows = []

    for version in versions:
        archive_version_id = clean_cell_value(
            version.get("id")
        )

        version_number = int(
            version.get("version") or 0
        )

        version_label = (
            clean_cell_value(
                version.get("versionLabel")
            )
            or (
                f"v{version_number}"
                if version_number
                else "Версия"
            )
        )

        original_name = (
            clean_cell_value(
                version.get("originalName")
            )
            or clean_cell_value(
                version.get("name")
            )
            or "Архивный файл"
        )

        archive_name = clean_cell_value(
            version.get("name")
        )

        size_text = format_file_size(
            version.get("size") or 0
        )

        date_text = format_datetime(
            version.get("archivedAt")
            or version.get("uploadedAt")
        )

        user_text = (
            clean_cell_value(
                version.get("archivedByName")
            )
            or clean_cell_value(
                version.get("uploadedByName")
            )
            or "—"
        )

        open_url = build_archive_file_url(
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            item_id=item_id,
            archive_version_id=archive_version_id,
            series_id=series_id,
            download=False,
        )

        download_url = build_archive_file_url(
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            item_id=item_id,
            archive_version_id=archive_version_id,
            series_id=series_id,
            download=True,
        )

        yandex_delete_status = clean_cell_value(
            version.get("yandexDeleteStatus")
        ).lower()

        (
            yandex_status_text,
            _yandex_status_color,
            _yandex_status_background,
            yandex_status_title,
        ) = get_archive_yandex_status_presentation(
            yandex_delete_status
        )

        yandex_status_class = (
            get_archive_yandex_status_class(
                yandex_delete_status
            )
        )

        yandex_status_html = ""

        if yandex_status_text:
            yandex_status_html = f"""
                    <span
                        class="folder-archive-yandex-status folder-archive-yandex-status--{html.escape(yandex_status_class)}"
                        title="{html.escape(yandex_status_title)}"
                    >
                        {html.escape(yandex_status_text)}
                    </span>
            """

        delete_blocked = (
            yandex_delete_status
            in ARCHIVE_DELETE_BLOCKED_STATUSES
        )

        delete_disabled_attr = (
            "disabled"
            if delete_blocked
            else ""
        )

        delete_title = (
            "Дождитесь завершения операции "
            "с Яндекс.Диском"
            if delete_blocked
            else (
                "Окончательно удалить архивную "
                "версию"
            )
        )

        secondary_name_html = ""

        if (
            archive_name
            and archive_name != original_name
        ):
            secondary_name_html = f"""
                <div
                    class="folder-archive-version-secondary-name"
                    title="{html.escape(archive_name)}"
                >
                    Архивный файл:
                    {html.escape(archive_name)}
                </div>
            """

        version_rows.append(f"""
            <div
                class="folder-archive-version"
                data-role="folder-archive-version"
                data-archive-version-id="{html.escape(archive_version_id)}"
                data-series-id="{html.escape(series_id)}"
            >
                <div class="folder-archive-version-main">
                    <div class="folder-archive-version-heading">
                        <span class="folder-archive-version-badge">
                            {html.escape(version_label)}
                        </span>

                        <a
                            class="folder-archive-version-name"
                            href="{html.escape(open_url)}"
                            target="_blank"
                            rel="noopener noreferrer"
                            title="Открыть архивную версию: {html.escape(original_name)}"
                            aria-label="Открыть архивную версию {html.escape(original_name)}"
                        >
                            {html.escape(original_name)}
                        </a>
                    </div>

                    {secondary_name_html}
                </div>

                <div class="folder-archive-version-meta">
                    {html.escape(size_text)}
                </div>

                <div class="folder-archive-version-meta">
                    {html.escape(date_text)}
                </div>

                <div
                    class="folder-archive-version-user"
                    title="{html.escape(user_text)}"
                >
                    {html.escape(user_text)}
                </div>

                <div class="folder-archive-version-actions">
                    {yandex_status_html}

                    <button
                        type="button"
                        class="folder-archive-delete-button checklist-action-button checklist-action-button-remove"
                        data-role="folder-delete-archive-version"
                        data-archive-version-id="{html.escape(archive_version_id)}"
                        data-series-id="{html.escape(series_id)}"
                        data-archive-version-name="{html.escape(original_name)}"
                        data-yandex-delete-status="{html.escape(yandex_delete_status)}"
                        {delete_disabled_attr}
                        title="{html.escape(delete_title)}"
                        aria-label="Окончательно удалить архивную версию"
                    >
                        <span data-checklist-icon="remove"></span>
                    </button>
                </div>
            </div>
        """)

    detached_badge = (
        """
            <span class="folder-archive-detached-badge">
                Текущий файл удалён
            </span>
        """
        if detached
        else ""
    )

    return f"""
        <tr
            class="folder-archive-summary-row"
            data-role="folder-archive-summary-row"
            data-series-id="{html.escape(series_id)}"
        >
            <td
                class="folder-archive-summary-cell"
                colspan="5"
            >
                <button
                    type="button"
                    class="folder-archive-toggle"
                    data-role="folder-archive-toggle"
                    data-panel-id="{html.escape(panel_id)}"
                    aria-expanded="false"
                >
                    <span
                        class="folder-archive-toggle-icon"
                        data-role="folder-archive-toggle-icon"
                        aria-hidden="true"
                    >
                        ▸
                    </span>

                    <span>
                        {html.escape(panel_title)}:
                        {len(versions)}
                    </span>

                    {detached_badge}
                </button>
            </td>
        </tr>

        <tr
            id="{html.escape(panel_id)}"
            class="folder-archive-panel"
            data-role="folder-archive-panel"
            data-series-id="{html.escape(series_id)}"
        >
            <td
                class="folder-archive-panel-cell"
                colspan="5"
            >
                <div class="folder-archive-box">
                    {''.join(version_rows)}
                </div>
            </td>
        </tr>
    """


def build_detached_archive_rows_html(
    dialog_id: str,
    checklist_key: str,
    item_id: str,
    archived_document_series,
    format_datetime,
) -> str:
    detached_series = normalize_detached_archive_series(
        archived_document_series
    )

    if not detached_series:
        return ""

    rows = ["""
        <tr class="folder-archive-detached-heading-row">
            <td
                class="folder-archive-detached-heading-cell"
                colspan="5"
            >
                Архив удалённых файлов
            </td>
        </tr>
    """]

    for series in detached_series:
        series_id = clean_cell_value(
            series.get("seriesId")
        )

        current_name = (
            clean_cell_value(
                series.get("lastCurrentName")
            )
            or "Удалённый файл"
        )

        panel_id = (
            "archive-panel-detached-"
            + uuid.uuid5(
                uuid.NAMESPACE_URL,
                (
                    f"{dialog_id}|"
                    f"{checklist_key}|"
                    f"{item_id}|detached|"
                    f"{series_id}"
                ),
            ).hex
        )

        rows.append(
            build_archive_series_rows_html(
                dialog_id=dialog_id,
                checklist_key=checklist_key,
                item_id=item_id,
                series_id=series_id,
                archive_versions=series.get(
                    "archiveVersions"
                ),
                panel_title=(
                    f"Архив файла «{current_name}»"
                ),
                panel_id=panel_id,
                format_datetime=format_datetime,
                detached=True,
            )
        )

    return "".join(rows)
