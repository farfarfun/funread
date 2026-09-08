from fastapi import APIRouter

from .sources import router as sources_router

api_router = APIRouter()
api_router.include_router(sources_router)

__all__ = ["api_router"]
