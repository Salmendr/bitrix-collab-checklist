from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles

from app.settings import (
    UPLOAD_ROOT,
    ensure_runtime_directories,
)

from app.db import init_db

from app.ui.routes import router as ui_router
from app.ui.popup_routes import router as popup_router
from app.service_routes import router as service_router
from app.checklists.lock_routes import router as lock_router
from app.checklists.session_routes import router as session_router
from app.checklists.project_routes import router as project_router
from app.integrations.n8n_routes import router as n8n_router
from app.checklists.checklist_routes import router as checklist_router
from app.checklists.document_routes import router as document_router

from app.checklists.yandex_warmup_queue import (
    start_yandex_warmup_workers,
    enqueue_all_saved_project_contexts,
)

from app.checklists.yandex_mirror_queue import (
    start_yandex_mirror_workers,
    enqueue_pending_yandex_mirror_jobs,
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
app.include_router(ui_router)
app.include_router(popup_router)
app.include_router(service_router)
app.include_router(lock_router)
app.include_router(session_router)
app.include_router(project_router)
app.include_router(n8n_router)
app.include_router(checklist_router)
app.include_router(document_router)

# Страховочный вызов при импорте модуля
init_db()


@app.on_event("startup")
def startup_event():
    init_db()

    start_yandex_warmup_workers()
    enqueue_all_saved_project_contexts(source="startup")

    start_yandex_mirror_workers()
    enqueue_pending_yandex_mirror_jobs(source="startup")