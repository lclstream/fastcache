import asyncio
import logging

from sqlalchemy import select

from .config import settings
from .db import SessionLocal
from .models import CacheStatus
from .process import is_alive
from .tables import Cache

logger = logging.getLogger(__name__)

# Statuses whose process should still be running, so must be checked vs reality.
_NON_FINAL = [s.value for s in CacheStatus if not s.is_final()]


async def sweep_dead_caches() -> int:
    """Mark non-final caches whose process is gone as failed; return how many.

    Caches outlive the supervisor, so a row's status is a claim we verify against
    the live process. A detached process's exit code is unrecoverable, so a clean
    exit and a crash both land in failed (see exit-marker, future work).
    """
    async with SessionLocal() as session:
        result = await session.execute(
            select(Cache).where(Cache.status.in_(_NON_FINAL))
        )
        caches = result.scalars().all()

        stale = 0
        for cache in caches:
            if is_alive(cache.pid, cache.create_time):
                continue
            logger.warning(
                "Cache %s (pid=%d) is no longer running; marking failed",
                cache.id,
                cache.pid,
            )
            cache.status = CacheStatus.failed
            stale += 1

        if stale:
            await session.commit()
        return stale


async def reconcile_caches() -> None:
    """One-shot sweep at startup to reconcile DB state against live processes."""
    stale = await sweep_dead_caches()
    logger.info("Startup reconcile complete; %d cache(s) marked failed", stale)


async def monitor_caches() -> None:
    """Poll liveness until cancelled; one failed sweep must not kill the loop."""
    while True:
        await asyncio.sleep(settings.CACHE_POLL_SECONDS)
        try:
            await sweep_dead_caches()
        except Exception:
            logger.exception("Cache liveness sweep failed; continuing")
