from enum import StrEnum
from pathlib import Path
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class CacheStatus(StrEnum):
    new = "new"
    queued = "queued"
    active = "active"
    completed = "completed"
    failed = "failed"
    canceled = "canceled"

    def is_final(self) -> bool:
        return self.value in ("completed", "failed", "canceled")


class CacheConfig(BaseModel):
    """Configuration document persisted as JSON on a cache row.

    Mirrors the cacheserver config (see config/default.json) plus the
    resolved ZMQ bind URIs.
    """

    hostname: str
    pull_uri: str
    push_uri: str
    type: int = 4
    helper_threads: int = 0
    num_workers: int = 1
    io_threads: int = 16
    hwm: int = 10
    verbose: bool = False


class FastcacheConfig(BaseModel):
    """On-disk config consumed by the fastcache binary (see src/config.cpp).

    We should adjust these fields later to better match the request schema.
    """

    inurl: str
    outurl: str
    type: int = 4
    helper_threads: int = 0
    num_workers: int = 1
    io_threads: int = 16
    hwm: int = 10
    verbose: bool = False

    @classmethod
    def from_cache_config(cls, config: CacheConfig) -> FastcacheConfig:
        cfg_dict = config.model_dump(mode="json", exclude={"pull_uri", "push_uri"})
        return cls(
            inurl=config.pull_uri,
            outurl=config.push_uri,
            **cfg_dict,
        )


class CacheRequest(BaseModel):
    transfer_id: str
    hostname: str
    # later relax to none
    pull_port: int
    push_port: int


class CachePublic(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    transfer_id: str
    user: str
    status: CacheStatus
    log_path: Path
    config: CacheConfig


class CacheProcess(BaseModel):
    pid: int
    create_time: float | None
    log_path: Path


class CachesPublic(BaseModel):
    caches: list[CachePublic]
