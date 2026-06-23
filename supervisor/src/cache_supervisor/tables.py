from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    JSON,
    DateTime,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from .models import CacheStatus


class Base(DeclarativeBase):
    pass


class UpdatedAtMixin:
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class CreatedAtMixin:
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )


class DTMixin(CreatedAtMixin, UpdatedAtMixin):
    pass


class Cache(DTMixin, Base):
    __tablename__ = "caches"

    id: Mapped[UUID] = mapped_column(default=uuid4, primary_key=True)
    transfer_id: Mapped[str] = mapped_column(unique=True, doc="Unique transfer ID")
    pid: Mapped[int] = mapped_column(doc="PID of the cache process")
    create_time: Mapped[float | None] = mapped_column(
        default=None,
        doc=(
            "psutil process create_time at launch. Combined with pid this is a "
            "reuse-safe process identity: the OS recycles pids, so a bare pid "
            "may later point at an unrelated process. NULL means identity is "
            "unverifiable and the supervisor must never kill the process."
        ),
    )
    user: Mapped[str] = mapped_column(doc="User that created this cache")
    status: Mapped[str] = mapped_column(
        default=CacheStatus.new,
        doc="Lifecycle status; stored as the CacheStatus string value",
    )
    log_path: Mapped[str] = mapped_column(
        doc="Absolute path to the cache process log file",
    )
    config: Mapped[dict[str, Any]] = mapped_column(
        JSON, doc="Cache configuration json for lclstream-fastcache"
    )
