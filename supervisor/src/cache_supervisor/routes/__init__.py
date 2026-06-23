from fastapi import APIRouter

from .cache import router as cache_router

api_router = APIRouter()
api_router.include_router(cache_router)
