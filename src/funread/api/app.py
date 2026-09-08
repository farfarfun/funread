"""FastAPI app assembly (sync — funread's storage layer is sync SQLAlchemy)."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

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
        description="Read-only API over funread's collected source data",
        lifespan=lifespan,
    )
    # Dev/test scope only: the web frontend runs on a different origin
    # (Vite dev server) and this API has no auth to protect anyway.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["GET"],
        allow_headers=["*"],
    )
    app.include_router(api_router, prefix="/api/v1")

    @app.get("/healthz", tags=["ops"])
    def healthz() -> dict:
        return {"status": "ok"}

    return app


app = create_app()


def run() -> None:
    import uvicorn

    uvicorn.run("funread.api.app:app", host="0.0.0.0", port=8000)


if __name__ == "__main__":
    run()
