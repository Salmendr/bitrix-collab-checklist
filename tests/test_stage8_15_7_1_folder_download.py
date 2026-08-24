from __future__ import annotations

import sys
import types
import unittest
from pathlib import Path


try:
    import requests as _requests  # noqa: F401
except ModuleNotFoundError:
    sys.modules.setdefault("requests", types.SimpleNamespace())


class FolderDownloadUiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.root = Path(__file__).resolve().parents[1]

    def read(self, relative: str) -> str:
        return (self.root / relative).read_text(encoding="utf-8")

    def test_download_icon_is_up_arrow_touching_top_line(self):
        source = self.read(
            "app/ui/static/js/checklist-action-icons.js"
        )
        download_block = source.split("download: `", 1)[1].split("`,", 1)[0]

        self.assertIn('<path d="M5 4h14"></path>', download_block)
        self.assertIn('<path d="M12 20V4"></path>', download_block)
        self.assertIn('<path d="m7 9 5-5 5 5"></path>', download_block)
        self.assertNotIn('M5 21h14', download_block)
        self.assertNotIn('m7 10 5 5 5-5', download_block)

    def test_current_and_archive_rows_render_download_buttons(self):
        routes = self.read("app/checklists/document_routes.py")
        archive = self.read("app/checklists/archive_ui.py")

        self.assertIn('data-role="folder-download-file"', routes)
        self.assertIn(
            'data-role="folder-download-archive-version"',
            archive,
        )
        self.assertIn('href="{html.escape(download_url)}"', routes)
        self.assertIn('href="{html.escape(download_url)}"', archive)
        self.assertIn('data-checklist-icon="download"', routes)
        self.assertIn('data-checklist-icon="download"', archive)
        self.assertIn('title="Скачать файл"', routes)
        self.assertIn('title="Скачать файл"', archive)

        replace_at = routes.index('data-role="folder-replace-upload"')
        current_download_at = routes.index(
            'data-role="folder-download-file"'
        )
        self.assertLess(replace_at, current_download_at)

    def test_download_controls_use_existing_attachment_routes(self):
        routes = self.read("app/checklists/document_routes.py")
        archive = self.read("app/checklists/archive_ui.py")

        self.assertIn('download_url = open_url + "&download=1"', routes)
        self.assertIn('disposition = "attachment" if download', routes)
        self.assertIn("download=True", archive)
        self.assertIn(
            '"attachment"\n        if int(download or 0)',
            self.read("app/checklists/archive_routes.py"),
        )


if __name__ == "__main__":
    unittest.main()
