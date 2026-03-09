from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.api.routes.advice import router as advice_router
from app.api.routes.data import router as data_router
from app.api.routes.performance import router as performance_router
from app.api.routes.portfolio import router as portfolio_router
from app.api.routes.user import router as user_router
from app.core.config import get_settings
from app.core.database import init_db
from app.web.pages import router as pages_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(title=settings.app_name, lifespan=lifespan)

    static_dir = Path(settings.base_dir) / "static"
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    app.include_router(user_router)
    app.include_router(data_router)
    app.include_router(advice_router)
    app.include_router(portfolio_router)
    app.include_router(performance_router)
    app.include_router(pages_router)
    return app


app = create_app()
