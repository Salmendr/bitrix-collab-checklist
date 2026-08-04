from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles

from app.settings import (
    UPLOAD_ROOT,
    ensure_runtime_directories,
)

from app.db import init_db
from app.ui.template_engine import UI_STATIC_ROOT

from app.ui.routes import router as ui_router
from app.ui.popup_routes import router as popup_router
from app.service_routes import router as service_router
from app.checklists.lock_routes import router as lock_router
from app.checklists.edit_session_routes import (
    router as edit_session_router,
)
from app.checklists.session_routes import router as session_router
from app.checklists.project_routes import router as project_router
from app.integrations.n8n_routes import router as n8n_router
from app.checklists.checklist_routes import router as checklist_router
from app.checklists.document_routes import router as document_router
from app.checklists.archive_routes import router as archive_router
from app.checklists.version_link_routes import (
    router as version_link_router,
)
from app.checklists.notification_routes import (
    router as notification_router,
)
from app.checklists.assignment_part_routes import (
    router as assignment_part_router,
)
from app.checklists.bitrix_user_routes import (
    router as bitrix_user_router,
)
from app.checklists.bitrix_company_routes import (
    router as bitrix_company_router,
)
from app.checklists.project_curator_routes import (
    router as project_curator_router,
)
from app.checklists.notification_delivery_routes import (
    router as notification_delivery_router,
)
from app.checklists.document_assignment_history_routes import (
    router as document_assignment_history_router,
)
from app.checklists.notification_delivery import (
    ensure_notification_delivery_schema,
)
from app.checklists.session_finalization import (
    recover_pending_finalization_deliveries,
)
from app.checklists.assignment_parts import (
    backfill_assignment_parts_from_drafts,
)
from app.checklists.yandex_structure_routes import (
    router as yandex_structure_router,
)
from app.admin_routes import router as admin_router
from app.checklists.yandex_warmup_queue import (
    start_yandex_warmup_workers,
    enqueue_all_saved_project_contexts,
)

from app.checklists.yandex_mirror_queue import (
    start_yandex_mirror_workers,
    recover_yandex_mirror_state_on_startup,
)
from app.checklists.yandex_mirror_reconciliation import (
    start_yandex_mirror_reconciliation,
)

from app.checklists.edit_session_worker import (
    recover_edit_sessions_on_startup,
    start_edit_session_sweeper,
)

from app.checklists.yandex_structure_queue import (
    recover_yandex_structure_state_on_startup,
    start_yandex_structure_workers,
)

from app.logging_utils import write_debug_log

app = FastAPI()

@app.middleware("http")
async def upload_request_debug_middleware(request: Request, call_next):
    path = str(request.url.path or "")

    if path.endswith("/api/checklist/upload-document"):
        write_debug_log("upload_http_request_received", {
            "method": request.method,
            "path": path,
            "contentLength": request.headers.get("content-length", ""),
            "contentType": request.headers.get("content-type", ""),
            "xForwardedFor": request.headers.get("x-forwarded-for", ""),
            "xForwardedProto": request.headers.get("x-forwarded-proto", ""),
            "xForwardedHost": request.headers.get("x-forwarded-host", ""),
        })

        try:
            response = await call_next(request)

            write_debug_log("upload_http_request_completed", {
                "method": request.method,
                "path": path,
                "statusCode": response.status_code,
                "contentLength": request.headers.get("content-length", ""),
            })

            return response

        except Exception as exc:
            write_debug_log("upload_http_request_failed_before_response", {
                "method": request.method,
                "path": path,
                "contentLength": request.headers.get("content-length", ""),
                "contentType": request.headers.get("content-type", ""),
                "error": str(exc),
                "errorType": type(exc).__name__,
            })
            raise

    return await call_next(request)

ensure_runtime_directories()

app.mount("/uploads", StaticFiles(directory=str(UPLOAD_ROOT)), name="uploads")
app.mount("/ui-static", StaticFiles(directory=str(UI_STATIC_ROOT)), name="ui-static")

# Новая админ-панель должна подключаться до ui_router,
# потому что старая /admin могла быть внутри ui_router.
app.include_router(admin_router)
app.include_router(ui_router)
app.include_router(popup_router)
app.include_router(service_router)
app.include_router(lock_router)
app.include_router(edit_session_router)
app.include_router(session_router)
app.include_router(project_router)
app.include_router(n8n_router)
app.include_router(checklist_router)
app.include_router(document_router)
app.include_router(archive_router)
app.include_router(version_link_router)
app.include_router(notification_router)
app.include_router(assignment_part_router)
app.include_router(bitrix_user_router)
app.include_router(bitrix_company_router)
app.include_router(project_curator_router)
app.include_router(notification_delivery_router)
app.include_router(document_assignment_history_router)
app.include_router(yandex_structure_router)
# Страховочный вызов при импорте модуля
init_db()
ensure_notification_delivery_schema()
backfill_assignment_parts_from_drafts()


@app.on_event("startup")
def startup_event():
    init_db()
    ensure_notification_delivery_schema()
    backfill_assignment_parts_from_drafts()

    recover_edit_sessions_on_startup(
        source="startup"
    )
    start_edit_session_sweeper()
    recover_pending_finalization_deliveries(
        source="startup"
    )

    start_yandex_warmup_workers()
    enqueue_all_saved_project_contexts(source="startup")

    recover_yandex_mirror_state_on_startup(
        source="startup"
    )
    start_yandex_mirror_workers()

    recover_yandex_structure_state_on_startup(
        source="startup"
    )
    start_yandex_structure_workers()

    # Stage 8.15.4: one asynchronous pass per process startup restores
    # legacy/skipped/failed current-document uploads without duplicating synced jobs.
    start_yandex_mirror_reconciliation(
        source="startup"
    )
