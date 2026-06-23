import asyncio
import contextlib
import logging
import ssl
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.routing import APIRoute

from .config import settings
from .db import init_db
from .reconcile import monitor_caches, reconcile_caches
from .routes import api_router

logger = logging.getLogger(__name__)


def custom_generate_unique_id(route: APIRoute) -> str:
    return f"{route.tags[0]}-{route.name}"


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncGenerator[None]:
    logger.info("Starting %s...", settings.PROJECT_NAME)
    await init_db()
    await reconcile_caches()
    monitor = asyncio.create_task(monitor_caches())
    try:
        yield
    finally:
        monitor.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await monitor


app = FastAPI(
    title=settings.PROJECT_NAME,
    openapi_url=f"{settings.API_V1_STR}/openapi.json",
    generate_unique_id_function=custom_generate_unique_id,
    lifespan=lifespan,
)

app.include_router(api_router, prefix=settings.API_V1_STR)


def main() -> None:
    if not settings.tls_enabled:
        if settings.ENVIRONMENT in ("staging", "production"):
            raise RuntimeError(
                f"Refusing to start over plain HTTP in '{settings.ENVIRONMENT}'. "
                "Set SSL_CERTFILE and SSL_KEYFILE to enable HTTPS."
            )
        logger.warning(
            "!!! SERVING OVER PLAIN HTTP — traffic is UNENCRYPTED. "
            "Set SSL_CERTFILE and SSL_KEYFILE to enable HTTPS. "
            "Do NOT use this in staging/production. !!!"
        )

    uvicorn.run(
        app,
        host=settings.HOST,
        port=settings.PORT,
        ssl_certfile=settings.SSL_CERTFILE,
        ssl_keyfile=settings.SSL_KEYFILE,
        ssl_ca_certs=str(settings.SSL_CA_CERTS) if settings.SSL_CA_CERTS else None,
        ssl_cert_reqs=(
            ssl.CERT_REQUIRED if settings.REQUIRE_CLIENT_CERT else ssl.CERT_NONE
        ),
    )
