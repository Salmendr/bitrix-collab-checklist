from fastapi import FastAPI
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

app = FastAPI()

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