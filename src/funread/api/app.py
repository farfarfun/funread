"""FastAPI app assembly (sync — funread's storage layer is sync SQLAlchemy)."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from funread.api.v1 import api_router
from funread.legado.manage.source.storage import init_source_db


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    # Create tables (if missing) on startup so config/connectivity errors
    # surface immediately instead of on the first request.
    init_source_db()
    yield


def create_app() -> FastAPI:
    app = FastAPI(
        title="funread",
        version="0.1.0",
        description="Manage funread's source-list collection",
        lifespan=lifespan,
    )
    app.include_router(api_router, prefix="/api/v1")

    @app.get("/healthz", tags=["ops"])
    def healthz() -> dict:
        return {"status": "ok"}

    return app


app = create_app()


def run() -> None:
    import uvicorn

    uvicorn.run("funread.api.app:app", host="127.0.0.1", port=18811)


if __name__ == "__main__":
    run()
