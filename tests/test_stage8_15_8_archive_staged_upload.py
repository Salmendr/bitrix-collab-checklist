from __future__ import annotations

import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch


try:
    import requests as _requests  # noqa: F401
except ModuleNotFoundError:
    sys.modules.setdefault("requests", types.SimpleNamespace())

try:
    import fastapi as _fastapi  # noqa: F401
except ModuleNotFoundError:
    sys.modules.setdefault(
        "fastapi",
        types.SimpleNamespace(Request=object),
    )


class ArchiveMetadataHistoryTests(unittest.TestCase):
    def setUp(self):
        from app.checklists import documents

        self.documents = documents
        self.temp = tempfile.TemporaryDirectory()
        self.upload_root = Path(self.temp.name) / "uploads"
        self.upload_root.mkdir(parents=True)
        self.upload_patch = patch.object(
            documents,
            "UPLOAD_ROOT",
            self.upload_root,
        )
        self.upload_patch.start()

    def tearDown(self):
        self.upload_patch.stop()
        self.temp.cleanup()

    def current_document(
        self,
        *,
        number: int,
        uploader: str,
        uploaded_at: str,
        archive_versions: list[dict],
    ) -> dict:
        relative_path = Path(
            "checklists/chat101/item-1"
        ) / f"current-{number}.txt"
        absolute_path = self.upload_root / relative_path
        absolute_path.parent.mkdir(parents=True, exist_ok=True)
        contents = f"version-{number}".encode("utf-8")
        absolute_path.write_bytes(contents)
        url = "/uploads/" + relative_path.as_posix()
        return {
            "id": f"document-{number}",
            "seriesId": "series-1",
            "name": f"Файл {number}.txt",
            "path": url,
            "fileUrl": url,
            "previewUrl": url,
            "size": len(contents),
            "uploadedAt": uploaded_at,
            "modifiedAt": uploaded_at,
            "uploadedById": str(number),
            "uploadedByName": uploader,
            "archiveVersions": list(archive_versions),
        }

    def test_four_version_chain_keeps_each_original_uploader_and_time(self):
        uploaders = [
            "Сергей Жигарь",
            "Технический аккаунт",
            "Сергей Жигарь",
            "Технический аккаунт",
        ]
        uploaded_at = [
            "2026-08-24T11:42:00+00:00",
            "2026-08-24T11:43:00+00:00",
            "2026-08-24T11:44:00+00:00",
            "2026-08-24T11:45:00+00:00",
        ]

        archived_versions: list[dict] = []
        current = self.current_document(
            number=1,
            uploader=uploaders[0],
            uploaded_at=uploaded_at[0],
            archive_versions=archived_versions,
        )

        for next_number in range(2, 5):
            archived = (
                self.documents.archive_current_document_local_file(
                    dialog_id="chat101",
                    item_id="item-1",
                    document=current,
                    archived_by_id=str(next_number),
                    archived_by_name=uploaders[next_number - 1],
                    archived_at=uploaded_at[next_number - 1],
                )
            )
            archived_versions.append(archived)
            current = self.current_document(
                number=next_number,
                uploader=uploaders[next_number - 1],
                uploaded_at=uploaded_at[next_number - 1],
                archive_versions=archived_versions,
            )

        self.assertEqual(
            [row["uploadedByName"] for row in archived_versions],
            uploaders[:3],
        )
        self.assertEqual(
            [row["uploadedAt"] for row in archived_versions],
            uploaded_at[:3],
        )
        self.assertEqual(
            [row["archivedByName"] for row in archived_versions],
            uploaders[1:],
        )
        self.assertEqual(
            [row["archivedAt"] for row in archived_versions],
            uploaded_at[1:],
        )
        self.assertEqual(current["uploadedByName"], uploaders[3])
        self.assertEqual(current["uploadedAt"], uploaded_at[3])

    def test_archive_table_displays_upload_metadata_not_replacement_metadata(self):
        from app.checklists.archive_ui import build_archive_series_rows_html

        rendered = build_archive_series_rows_html(
            dialog_id="chat101",
            checklist_key="id",
            item_id="item-1",
            series_id="series-1",
            archive_versions=[{
                "id": "archive-1",
                "seriesId": "series-1",
                "version": 1,
                "name": "Файл_v1.txt",
                "originalName": "Файл.txt",
                "fileUrl": "/uploads/archive.txt",
                "size": 10,
                "uploadedAt": "ИСХОДНОЕ ВРЕМЯ",
                "uploadedByName": "Сергей Жигарь",
                "archivedAt": "ВРЕМЯ ЗАМЕНЫ",
                "archivedByName": "Технический аккаунт",
            }],
            panel_title="Архив версий",
            panel_id="archive-panel",
            format_datetime=lambda value: str(value),
        )

        self.assertIn("ИСХОДНОЕ ВРЕМЯ", rendered)
        self.assertIn("Сергей Жигарь", rendered)
        self.assertNotIn("ВРЕМЯ ЗАМЕНЫ", rendered)
        self.assertNotIn("Технический аккаунт", rendered)


class Stage8158UiContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.root = Path(__file__).resolve().parents[1]

    def read(self, relative: str) -> str:
        return (self.root / relative).read_text(encoding="utf-8")

    def test_share_buttons_are_icon_only_and_permanently_disabled(self):
        popup = self.read(
            "app/ui/static/js/popup-document-list.js"
        )
        popup_share = popup.split(
            'data-role="share-folder"', 1
        )[1].split("</button>", 1)[0]
        self.assertIn("Поделиться ссылкой — временно недоступно", popup_share)
        self.assertIn("aria-disabled=\"true\"", popup_share)
        self.assertIn("disabled", popup_share)
        self.assertNotIn("checklist-action-button-share-label", popup_share)

        routes = self.read("app/checklists/document_routes.py")
        folder_share = routes.split(
            'id="folderShareBtn"', 1
        )[1].split("</button>", 1)[0]
        self.assertIn('data-permanent-disabled="1"', folder_share)
        self.assertIn("Поделиться ссылкой — временно недоступно", folder_share)
        self.assertIn("disabled", folder_share)
        self.assertNotIn("checklist-action-button-share-label", folder_share)

    def test_current_and_archive_download_controls_are_preserved(self):
        routes = self.read("app/checklists/document_routes.py")
        archive = self.read("app/checklists/archive_ui.py")
        icons = self.read("app/ui/static/js/checklist-action-icons.js")

        self.assertIn('data-role="folder-download-file"', routes)
        self.assertIn('data-role="folder-download-archive-version"', archive)
        self.assertIn('href="{html.escape(download_url)}"', routes)
        self.assertIn('href="{html.escape(download_url)}"', archive)
        self.assertIn('title="Скачать файл"', routes)
        self.assertIn('title="Скачать файл"', archive)

        download_block = icons.split("download: `", 1)[1].split("`,", 1)[0]
        self.assertIn('<path d="M12 3v12"></path>', download_block)
        self.assertIn('<path d="m7 10 5 5 5-5"></path>', download_block)
        self.assertIn('<path d="M5 17v2a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2v-2"></path>', download_block)

    def test_staging_waits_for_explicit_confirmation(self):
        staging = self.read("app/ui/static/js/upload-staging.js")
        self.assertIn("Перетащите файлы для загрузки", staging)
        self.assertIn("input.addEventListener('change'", staging)
        self.assertIn("if (files.length) addFiles(files);", staging)
        self.assertIn("async function confirmUpload()", staging)
        self.assertIn(
            "confirmButton.addEventListener('click', confirmUpload)",
            staging,
        )
        self.assertIn("await options.onConfirm(selectedFiles, controller)", staging)
        self.assertIn("hasPendingFiles()", staging)

        session = self.read(
            "app/ui/static/js/popup-session-enhancements.js"
        )
        self.assertIn("В области загрузки остались неподтверждённые файлы", session)
        self.assertIn("staging.clearAll()", session)

        popup_actions = self.read(
            "app/ui/static/js/popup-document-actions.js"
        )
        folder_uploads = self.read(
            "app/ui/static/js/folder-uploads.js"
        )
        self.assertIn("ChecklistUploadStaging", popup_actions)
        self.assertIn("uploadPopupStagedFiles", popup_actions)
        self.assertIn("ChecklistUploadStaging.create", folder_uploads)
        self.assertIn("uploadFolderStagedFiles", folder_uploads)

    def test_folder_zone_is_below_file_table_and_popup_zone_precedes_actions(self):
        folder_template = self.read("app/ui/templates/folder.html")
        table_end = folder_template.index("</table>")
        mount_at = folder_template.index('id="folderUploadStagingMount"')
        self.assertLess(table_end, mount_at)

        popup_actions = self.read(
            "app/ui/static/js/popup-document-actions.js"
        )
        self.assertIn("prepend: true", popup_actions)
        self.assertIn("hideTriggerWhenExpanded: true", popup_actions)

    def test_file_cards_grow_by_rows_and_remove_control_appears_on_hover(self):
        css = self.read("app/ui/static/css/action-controls.css")
        self.assertIn(
            "grid-template-columns: repeat(auto-fill, minmax(92px, 1fr))",
            css,
        )
        self.assertIn(
            ".upload-staging-file-card:hover .upload-staging-file-remove",
            css,
        )
        self.assertIn("background: #d92d20", css)
        self.assertIn("min-height: 70px", css)

    def test_public_archive_also_prefers_original_upload_time(self):
        source = self.read("app/ui/static/js/public-folder.js")
        self.assertIn(
            "formatDate(version.uploadedAt || version.archivedAt)",
            source,
        )
        self.assertNotIn(
            "formatDate(version.archivedAt || version.uploadedAt)",
            source,
        )


if __name__ == "__main__":
    unittest.main()
