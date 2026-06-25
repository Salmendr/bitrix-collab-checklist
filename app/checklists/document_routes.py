import html
import mimetypes
import uuid
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

from fastapi import APIRouter, Request, UploadFile, File, Form
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse

from app.settings import (
    BASE_DIR,
    APP_BASE_PATH,
    UPLOAD_ROOT,
)

from app.logging_utils import write_debug_log

from app.checklists.utils import (
    can_preview_in_browser,
    clean_cell_value,
    format_file_size,
    normalize_dialog_id,
    normalize_checklist_key,
)

from app.checklists.permissions import can_user_delete_files

from app.checklists.storage import (
    get_checklist,
    get_item_yandex_folder,
    save_checklist,
)

from app.checklists.normalization import (
    normalize_checklist_data,
    derive_indicator_from_status,
)

from app.checklists.documents import (
    build_upload_rel_path,
    build_document_view_url,
    build_folder_view_url,
    get_upload_file_path_from_url,
    normalize_document_record,
    normalize_documents_list,
    migrate_legacy_document_fields,
    remove_item_document_file,
)

from app.checklists.yandex_folders import mirror_document_to_yandex
from app.ui.shell import normalize_base_path

from app.yandex_disk.client import (
    is_yandex_disk_enabled,
    yandex_disk_delete_path,
)


router = APIRouter()

@router.post("/api/checklist/upload-document")
async def api_checklist_upload_document(
    dialogId: str = Form(...),
    itemId: str = Form(...),
    file: UploadFile = File(...),
    checklistKey: str = Form("id"),
    itemGroup: str = Form("")
):
    dialog_id = normalize_dialog_id(dialogId)
    checklist_key = normalize_checklist_key(checklistKey)
    item_id = str(itemId or "").strip()
    item_group = int(str(itemGroup or "0").strip() or 0)

    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)

    if not item_id:
        return JSONResponse({"ok": False, "error": "itemId is required"}, status_code=400)

    data = get_checklist(dialog_id, checklist_key)
    items = data.get("items", []) or []

    target_item = None
    for index, item in enumerate(items):
        if str(item.get("id") or "") == item_id:
            target_item = migrate_legacy_document_fields(item)
            items[index] = target_item
            break

    if not target_item:
        return JSONResponse({"ok": False, "error": "item not found"}, status_code=404)

    rel_path = build_upload_rel_path(dialog_id, item_id, file.filename or "file.bin")
    abs_path = UPLOAD_ROOT / rel_path
    abs_path.parent.mkdir(parents=True, exist_ok=True)

    file_bytes = await file.read()
    with open(abs_path, "wb") as f:
        f.write(file_bytes)

    file_url = "/uploads/" + rel_path.replace("\\", "/")
    document_id = uuid.uuid4().hex
    document_view_url = build_document_view_url(dialog_id, checklist_key, item_id, document_id)
    folder_view_url = build_folder_view_url(dialog_id, checklist_key, item_id)

    try:
        folder_path = "/" + str(abs_path.parent.relative_to(BASE_DIR)).replace("\\", "/")
    except Exception:
        folder_path = file_url.rsplit("/", 1)[0]

    uploaded_name = Path(file.filename or "file.bin").name

    document_record = normalize_document_record({
        "id": document_id,
        "name": uploaded_name,
        "path": file_url,
        "fileUrl": file_url,
        "previewUrl": document_view_url,
        "size": len(file_bytes),
        "modifiedAt": datetime.now().isoformat(timespec="seconds"),
        "source": "local",

        "mirrorStatus": "",
        "mirrorError": "",
        "yandexPath": "",
        "yandexFileUrl": "",
        "yandexFolderAlias": "",
    })

    try:
        mirror_result = mirror_document_to_yandex(
            dialog_id=dialog_id,
            checklist_key=checklist_key,
            item_name=clean_cell_value(target_item.get("name")),
            filename=uploaded_name,
            file_bytes=file_bytes,
            item_id=str(target_item.get("id") or ""),
            item_group=int(target_item.get("group") or 0),
            is_custom=bool(target_item.get("isCustom", False)),
        )

        if mirror_result.get("ok"):
            document_record["mirrorStatus"] = "synced"
            document_record["mirrorError"] = ""
            document_record["yandexPath"] = clean_cell_value(mirror_result.get("filePath"))
            document_record["yandexFileUrl"] = clean_cell_value(mirror_result.get("folderUrl"))
            document_record["yandexFolderAlias"] = clean_cell_value(mirror_result.get("folderAlias"))
        else:
            document_record["mirrorStatus"] = "error"
            document_record["mirrorError"] = clean_cell_value(mirror_result.get("reason")) or "mirror failed"
    except Exception as e:
        document_record["mirrorStatus"] = "error"
        document_record["mirrorError"] = str(e)

    existing_documents = normalize_documents_list(target_item.get("documents"))
    existing_documents.append(document_record)
    normalized_documents = normalize_documents_list(existing_documents)

    target_item["documents"] = normalized_documents
    target_item["folderPath"] = folder_path
    target_item["folderUrl"] = folder_view_url if normalized_documents else ""

    first_doc = normalized_documents[0] if normalized_documents else {}
    target_item["documentUrl"] = clean_cell_value(first_doc.get("fileUrl"))
    target_item["documentName"] = clean_cell_value(first_doc.get("name"))

    actual_group = int(target_item.get("group") or item_group or 0)
    if checklist_key == "id" and actual_group != 4:
        target_item["status"] = "Есть"
        target_item["priority"] = derive_indicator_from_status("Есть")
    elif checklist_key == "opr" and actual_group != 2:
        target_item["status"] = "Есть"
        target_item["priority"] = derive_indicator_from_status("Есть")
    elif checklist_key == "concept" and actual_group != 10:
        target_item["status"] = "Есть"
        target_item["priority"] = derive_indicator_from_status("Есть")

    data["items"] = items
    data = normalize_checklist_data(data, checklist_key)
    save_checklist(dialog_id, data, checklist_key)

    updated_item = None
    for item in data.get("items", []):
        if str(item.get("id") or "") == item_id:
            updated_item = item
            break

    if not updated_item:
        return JSONResponse({"ok": False, "error": "updated item not found"}, status_code=500)

    return JSONResponse({
        "ok": True,
        "dialogId": dialog_id,
        "checklistKey": checklist_key,
        "item": updated_item,
        "progressPercent": data.get("progressPercent", 0),
    })

@router.post("/api/checklist/remove-document")
async def api_checklist_remove_document(request: Request):
    payload = await request.json()

    dialog_id = normalize_dialog_id(payload.get("dialogId"))
    checklist_key = normalize_checklist_key(payload.get("checklistKey"))
    item_id = str(payload.get("itemId") or "").strip()
    document_id = clean_cell_value(payload.get("documentId"))
    document_url = clean_cell_value(payload.get("documentUrl"))
    preserve_status = bool(payload.get("preserveStatus"))
    acting_user_id = clean_cell_value(payload.get("actingUserId"))
    acting_user_name = clean_cell_value(payload.get("actingUserName")) or "Пользователь"

    if not dialog_id:
        return JSONResponse({"ok": False, "error": "dialogId is required"}, status_code=400)

    if not item_id:
        return JSONResponse({"ok": False, "error": "itemId is required"}, status_code=400)

    if not can_user_delete_files(acting_user_id):
        return JSONResponse({
            "ok": False,
            "error": "У вас недостаточно прав на удаление файлов"
        }, status_code=403)

    data = get_checklist(dialog_id, checklist_key)
    items = data.get("items", []) or []

    target_item = None
    for index, item in enumerate(items):
        if str(item.get("id") or "") == item_id:
            target_item = migrate_legacy_document_fields(item)
            items[index] = target_item
            break

    if not target_item:
        return JSONResponse({"ok": False, "error": "item not found"}, status_code=404)

    documents = normalize_documents_list(target_item.get("documents"))

    doc_to_remove = None

    if document_id:
        for doc in documents:
            if str(doc.get("id") or "") == document_id:
                doc_to_remove = doc
                break

    if not doc_to_remove and document_url:
        for doc in documents:
            doc_file_url = clean_cell_value(doc.get("fileUrl"))
            doc_preview_url = clean_cell_value(doc.get("previewUrl"))
            doc_path = clean_cell_value(doc.get("path"))
            if document_url in {doc_file_url, doc_preview_url, doc_path}:
                doc_to_remove = doc
                break

    if not doc_to_remove and documents:
        doc_to_remove = documents[0]

    if doc_to_remove:
        local_document_url = (
            clean_cell_value(doc_to_remove.get("fileUrl"))
            or clean_cell_value(doc_to_remove.get("previewUrl"))
            or clean_cell_value(doc_to_remove.get("path"))
        )

        remove_item_document_file({
            "documentUrl": local_document_url
        })

        yandex_path = clean_cell_value(doc_to_remove.get("yandexPath"))
        if yandex_path and is_yandex_disk_enabled():
            try:
                yandex_disk_delete_path(yandex_path, permanently=True)
            except Exception as e:
                write_debug_log("yandex_mirror_delete_error", {
                    "dialogId": dialog_id,
                    "checklistKey": checklist_key,
                    "itemId": item_id,
                    "documentId": clean_cell_value(doc_to_remove.get("id")),
                    "yandexPath": yandex_path,
                    "error": str(e),
                })

    remaining_documents = []
    removed = False

    for doc in documents:
        same_id = document_id and str(doc.get("id") or "") == document_id
        same_url = document_url and document_url in {
            clean_cell_value(doc.get("fileUrl")),
            clean_cell_value(doc.get("previewUrl")),
            clean_cell_value(doc.get("path")),
        }

        if not removed and (same_id or same_url or (doc_to_remove and str(doc.get("id") or "") == str(doc_to_remove.get("id") or ""))):
            removed = True
            continue

        remaining_documents.append(doc)

    normalized_documents = normalize_documents_list(remaining_documents)
    target_item["documents"] = normalized_documents

    first_doc = normalized_documents[0] if normalized_documents else {}
    target_item["documentUrl"] = clean_cell_value(first_doc.get("fileUrl"))
    target_item["documentName"] = clean_cell_value(first_doc.get("name"))

    if normalized_documents:
        first_file_url = clean_cell_value(first_doc.get("fileUrl"))
        target_item["folderPath"] = first_file_url.rsplit("/", 1)[0] if first_file_url.startswith("/") else ""
        target_item["folderUrl"] = build_folder_view_url(dialog_id, checklist_key, item_id)
    else:
        target_item["folderPath"] = ""
        target_item["folderUrl"] = ""
        target_item["documentUrl"] = ""
        target_item["documentName"] = ""

        if checklist_key == "id" and int(target_item.get("group") or 0) != 4 and not preserve_status:
            target_item["status"] = ""
            target_item["priority"] = "white"
        elif checklist_key == "opr" and int(target_item.get("group") or 0) != 2 and not preserve_status:
            target_item["status"] = ""
            target_item["priority"] = "white"

    data["items"] = items
    data = normalize_checklist_data(data, checklist_key)
    save_checklist(dialog_id, data, checklist_key)

    updated_item = None
    for item in data.get("items", []):
        if str(item.get("id") or "") == item_id:
            updated_item = item
            break

    if not updated_item:
        return JSONResponse({"ok": False, "error": "updated item not found"}, status_code=500)

    return JSONResponse({
        "ok": True,
        "dialogId": dialog_id,
        "checklistKey": checklist_key,
        "item": updated_item,
        "progressPercent": data.get("progressPercent", 0),
    })

@router.get("/api/checklist/folder", response_class=HTMLResponse)
def api_checklist_folder(dialogId: str = "", itemId: str = "", checklistKey: str = "id"):
    dialog_id = normalize_dialog_id(dialogId)
    checklist_key = normalize_checklist_key(checklistKey)
    item_id = str(itemId or "").strip()

    if not dialog_id or not item_id:
        return HTMLResponse("<h3>Не переданы dialogId или itemId</h3>", status_code=400)

    data = get_checklist(dialog_id, checklist_key)
    items = data.get("items", []) or []

    target_item = None

    for index, item in enumerate(items):
        if str(item.get("id") or "") == item_id:
            target_item = migrate_legacy_document_fields(item)
            items[index] = target_item
            break

    if not target_item:
        return JSONResponse({"ok": False, "error": "item not found"}, status_code=404)
    documents = normalize_documents_list(target_item.get("documents"))
    yandex_folder_data = get_item_yandex_folder(
        dialog_id,
        checklist_key,
        clean_cell_value(target_item.get("name"))
    )
    yandex_folder = (yandex_folder_data or {}).get("folder") or {}
    yandex_folder_url = clean_cell_value(yandex_folder.get("url"))
    yandex_folder_path = clean_cell_value(yandex_folder.get("path"))
    rows = []
    for doc in documents:
        doc_id = str(doc.get("id") or "")
        doc_name = html.escape(str(doc.get("name") or "Файл"))
        doc_size = html.escape(format_file_size(doc.get("size") or 0))
        open_url = build_document_view_url(dialog_id, checklist_key, item_id, doc_id)
        download_url = open_url + "&download=1"

        rows.append(f"""
            <tr>
                <td style="padding:10px 12px;border-bottom:1px solid #edf0f2;">{doc_name}</td>
                <td style="padding:10px 12px;border-bottom:1px solid #edf0f2;white-space:nowrap;">{doc_size}</td>
                <td style="padding:10px 12px;border-bottom:1px solid #edf0f2;white-space:nowrap;">
                    <a href="{html.escape(open_url)}" target="_blank">Открыть</a>
                    &nbsp;|&nbsp;
                    <a href="{html.escape(download_url)}" target="_blank">Скачать</a>
                    &nbsp;|&nbsp;
                    <button
                        type="button"
                        data-role="folder-remove-file"
                        data-dialog-id="{html.escape(dialog_id)}"
                        data-checklist-key="{html.escape(checklist_key)}"
                        data-item-id="{html.escape(item_id)}"
                        data-document-id="{html.escape(doc_id)}"
                        data-document-name="{doc_name}"
                        style="border:none;background:transparent;color:#b42318;cursor:pointer;font-size:16px;line-height:1;padding:0 2px;"
                        title="Удалить файл"
                    >
                        ×
                    </button>
                </td>
            </tr>
        """)

    table_html = "".join(rows) if rows else """
        <tr>
            <td colspan="3" style="padding:14px 12px;color:#667085;">В папке пока нет файлов</td>
        </tr>
    """

    title = html.escape(str(target_item.get("name") or "Папка"))
    checklist_title = html.escape(str(data.get("title") or "Чек-лист"))
    remove_api_url = html.escape(f"{normalize_base_path(APP_BASE_PATH)}/api/checklist/remove-document")
    upload_api_url = html.escape(f"{normalize_base_path(APP_BASE_PATH)}/api/checklist/upload-document")
    folder_item_group = html.escape(str(target_item.get("group") or ""))

    yandex_folder_path_html = ""
    if yandex_folder_path and not yandex_folder_url:
        yandex_folder_path_html = f'''
            <div style="margin-top:12px;font-size:12px;color:#667085;">
                Папка Яндекс Диска: {html.escape(yandex_folder_path)}
            </div>
        '''

    folder_actions_html = f'''
        <div style="display:flex;gap:10px;align-items:center;justify-content:flex-end;flex-wrap:wrap;">
            <button
                type="button"
                id="folderUploadBtn"
                style="display:inline-block;padding:8px 12px;border:1px solid #d0d7de;border-radius:8px;background:#f8fafc;color:#1f2328;text-decoration:none;cursor:pointer;"
            >
                Загрузить файлы в папку пункта
            </button>
            <input type="file" id="folderUploadInput" style="display:none;" multiple>
            {f'''
                <a
                    href="{html.escape(yandex_folder_url)}"
                    target="_blank"
                    style="display:inline-block;padding:8px 12px;border:1px solid #d0d7de;border-radius:8px;background:#f8fafc;color:#1f2328;text-decoration:none;"
                >
                    Открыть папку на Яндекс Диске
                </a>
            ''' if yandex_folder_url else ''}
        </div>
    '''

    return f"""
    <html>
    <head>
        <meta charset="utf-8">
        <title>{title}</title>
    </head>
    <body style="font-family:Arial,sans-serif;background:#f8fafc;margin:0;padding:24px;color:#1f2328;">
        <div style="max-width:960px;margin:0 auto;background:#fff;border:1px solid #e5e7eb;border-radius:14px;overflow:hidden;">
            <div style="padding:16px 18px;border-bottom:1px solid #edf0f2;background:#fafbfc;">
                <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:16px;flex-wrap:wrap;">
                    <div>
                        <div style="font-size:13px;color:#667085;margin-bottom:4px;">{checklist_title}</div>
                        <div style="font-size:22px;font-weight:700;">{title}</div>
                        {yandex_folder_path_html}
                    </div>
                    {folder_actions_html}
                </div>
            </div>
            <div style="padding:18px;">
                <table style="width:100%;border-collapse:collapse;">
                    <thead>
                        <tr>
                            <th style="text-align:left;padding:10px 12px;background:#f8fafc;border-bottom:1px solid #e5e7eb;">Файл</th>
                            <th style="text-align:left;padding:10px 12px;background:#f8fafc;border-bottom:1px solid #e5e7eb;">Размер</th>
                            <th style="text-align:left;padding:10px 12px;background:#f8fafc;border-bottom:1px solid #e5e7eb;">Действия</th>
                        </tr>
                    </thead>
                    <tbody>
                        {table_html}
                    </tbody>
                </table>
            </div>
        </div>
        <script>
            const folderRemoveApiUrl = "{remove_api_url}";
            const folderUploadApiUrl = "{upload_api_url}";
            const folderDialogId = "{html.escape(dialog_id)}";
            const folderChecklistKey = "{html.escape(checklist_key)}";
            const folderItemId = "{html.escape(item_id)}";
            const folderItemGroup = "{folder_item_group}";

            const folderDeleteAllowedUserIds = new Set([
                '108',
                '106',
                '114',
                '116',
                '72',
                '56',
                '26',
                '138',
                '18',
                '256',
                '140',
                '280',
                '124',
                '222'
            ]);

            function getFolderDeleteActor() {{
                try {{
                    const openerEditor = window.opener && window.opener.currentEditor
                        ? window.opener.currentEditor
                        : null;

                    return {{
                        id: String(openerEditor && openerEditor.id || '').trim(),
                        name: String(openerEditor && openerEditor.name || '').trim() || 'Пользователь'
                    }};
                }} catch (e) {{
                    return {{
                        id: '',
                        name: 'Пользователь'
                    }};
                }}
            }}

            function notifyParentChecklistDocumentChanged(messageType = 'checklist-document-changed', extraPayload = {{}}) {{
                try {{
                    if (window.opener && typeof window.opener.postMessage === 'function') {{
                        window.opener.postMessage({{
                            type: messageType,
                            dialogId: folderDialogId,
                            checklistKey: folderChecklistKey,
                            itemId: folderItemId,
                            ...extraPayload
                        }}, '*');
                    }}
                }} catch (e) {{
                    console.log('opener sync error:', e);
                }}
            }}

            document.querySelectorAll('[data-role="folder-remove-file"]').forEach(btn => {{
                btn.addEventListener('click', async function () {{
                    const documentName = this.dataset.documentName || 'файл';
                    const actor = getFolderDeleteActor();

                    if (!folderDeleteAllowedUserIds.has(String(actor.id || '').trim())) {{
                        alert('У вас недостаточно прав на удаление файлов');
                        return;
                    }}

                    if (!window.confirm('Удалить файл "' + documentName + '"?')) {{
                        return;
                    }}

                    this.disabled = true;

                    try {{
                        const response = await fetch(folderRemoveApiUrl, {{
                            method: 'POST',
                            headers: {{ 'Content-Type': 'application/json' }},
                            body: JSON.stringify({{
                                dialogId: this.dataset.dialogId,
                                checklistKey: this.dataset.checklistKey,
                                itemId: this.dataset.itemId,
                                documentId: this.dataset.documentId,
                                actingUserId: actor.id,
                                actingUserName: actor.name
                            }})
                        }});

                        const result = await response.json();
                        if (!response.ok || !result.ok) {{
                            throw new Error(result.error || 'remove document failed');
                        }}

                        notifyParentChecklistDocumentChanged('checklist-document-removed', {{
                            documentName: documentName
                        }});
                        window.location.reload();
                    }} catch (e) {{
                        console.log('folder remove error:', e);
                        alert(e && e.message ? e.message : 'Ошибка удаления файла');
                    }} finally {{
                        this.disabled = false;
                    }}
                }});
            }});

            const folderUploadBtn = document.getElementById('folderUploadBtn');
            const folderUploadInput = document.getElementById('folderUploadInput');

            if (folderUploadBtn && folderUploadInput) {{
                folderUploadBtn.addEventListener('click', function () {{
                    folderUploadInput.click();
                }});

                folderUploadInput.addEventListener('change', async function () {{
                    const files = Array.from(this.files || []);
                    if (!files.length) {{
                        return;
                    }}

                    folderUploadBtn.disabled = true;

                    try {{
                        for (const file of files) {{
                            const formData = new FormData();
                            formData.append('dialogId', folderDialogId);
                            formData.append('itemId', folderItemId);
                            formData.append('file', file);
                            formData.append('checklistKey', folderChecklistKey);
                            formData.append('itemGroup', folderItemGroup);

                            const response = await fetch(folderUploadApiUrl, {{
                                method: 'POST',
                                body: formData
                            }});

                            const result = await response.json();
                            if (!response.ok || !result.ok) {{
                                throw new Error(result.error || 'upload document failed');
                            }}
                        }}

                        notifyParentChecklistDocumentChanged('checklist-document-uploaded');
                        window.location.reload();
                    }} catch (e) {{
                        console.log('folder upload error:', e);
                        alert('Ошибка загрузки файлов');
                    }} finally {{
                        this.value = '';
                        folderUploadBtn.disabled = false;
                    }}
                }});
            }}
        </script>
    </body>
    </html>
    """
@router.get("/api/checklist/file")
def api_checklist_file(
    dialogId: str = "",
    itemId: str = "",
    documentId: str = "",
    checklistKey: str = "id",
    download: int = 0
):
    dialog_id = normalize_dialog_id(dialogId)
    checklist_key = normalize_checklist_key(checklistKey)
    item_id = str(itemId or "").strip()
    document_id = str(documentId or "").strip()

    if not dialog_id or not item_id or not document_id:
        return JSONResponse({"ok": False, "error": "dialogId, itemId and documentId are required"}, status_code=400)

    data = get_checklist(dialog_id, checklist_key)
    items = data.get("items", []) or []

    target_item = None
    for item in items:
        if str(item.get("id") or "") == item_id:
            target_item = item
            break

    if not target_item:
        return JSONResponse({"ok": False, "error": "item not found"}, status_code=404)

    target_item = migrate_legacy_document_fields(target_item)
    documents = normalize_documents_list(target_item.get("documents"))

    target_doc = None
    for doc in documents:
        if str(doc.get("id") or "") == document_id:
            target_doc = doc
            break

    if not target_doc:
        return JSONResponse({"ok": False, "error": "document not found"}, status_code=404)

    file_url = clean_cell_value(target_doc.get("fileUrl")) or clean_cell_value(target_doc.get("path"))
    file_path = get_upload_file_path_from_url(file_url)

    if not file_path or not file_path.exists():
        return JSONResponse({"ok": False, "error": "file not found on disk"}, status_code=404)

    filename = clean_cell_value(target_doc.get("name")) or file_path.name
    media_type, _ = mimetypes.guess_type(str(file_path))
    media_type = media_type or "application/octet-stream"

    inline_allowed = can_preview_in_browser(filename, media_type)
    disposition = "attachment" if download else ("inline" if inline_allowed else "attachment")

    response = FileResponse(
        path=str(file_path),
        media_type=media_type
    )
    response.headers["Content-Disposition"] = f"{disposition}; filename*=UTF-8''{quote(filename)}"
    return response