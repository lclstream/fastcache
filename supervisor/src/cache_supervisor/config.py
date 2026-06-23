from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import BeforeValidator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def parse_comma_list(v: Any) -> list[str] | str:
    if isinstance(v, str) and not v.startswith("["):
        return [i.strip() for i in v.split(",") if i.strip()]
    elif isinstance(v, list | str):
        return v
    raise ValueError(v)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_ignore_empty=True,
        extra="ignore",
    )
    API_V1_STR: str = "/api/v1"
    ENVIRONMENT: Literal["local", "staging", "production"] = "local"

    PROJECT_NAME: str = "lclstream_cache_supervisor"

    # Server bind address and TLS. Set both SSL_CERTFILE and SSL_KEYFILE to
    # serve over HTTPS; leave unset to serve plain HTTP.
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    SSL_CERTFILE: Path | None = None
    SSL_KEYFILE: Path | None = None
    SSL_CA_CERTS: Path | None = None
    REQUIRE_CLIENT_CERT: bool = False

    # SQLite database file path; async access via aiosqlite.
    SQLITE_PATH: Path = Path("cache_supervisor.sqlite")

    # Verified email addresses allowed to access the service.
    EXPECTED_USERS: Annotated[list[str], BeforeValidator(parse_comma_list)] = []

    # OIDC token validation. Identity provider is configured via environment;
    # no provider URL is baked into source.
    OIDC_ISSUER_URL: str
    OIDC_JWKS_URI: str | None = None
    OIDC_AUDIENCES: Annotated[list[str], BeforeValidator(parse_comma_list)] = []

    # Cache process management
    # Path to the fastcache executable (overridable; defaults to PATH lookup).
    FASTCACHE_BINARY: Path = Path("lclstream-fastcache")
    # Directory where per-cache config/log files are written.
    RUN_DIR: Path = Path("run")
    # Grace period before escalating SIGTERM to SIGKILL on shutdown.
    SHUTDOWN_GRACE_SECONDS: float = 5.0
    # How often the liveness poller checks running caches against their pids.
    CACHE_POLL_SECONDS: float = 5.0

    @property
    def DATABASE_URL(self) -> str:
        return f"sqlite+aiosqlite:///{self.SQLITE_PATH}"

    @model_validator(mode="after")
    def _check_tls_pair(self) -> Settings:
        if bool(self.SSL_CERTFILE) != bool(self.SSL_KEYFILE):
            raise ValueError(
                "Set both SSL_CERTFILE and SSL_KEYFILE to enable HTTPS, or neither"
            )
        if self.REQUIRE_CLIENT_CERT and not self.SSL_CA_CERTS:
            raise ValueError(
                "REQUIRE_CLIENT_CERT needs SSL_CA_CERTS (the CA signing client certs)"
            )
        if self.REQUIRE_CLIENT_CERT and not self.SSL_CERTFILE:
            raise ValueError(
                "REQUIRE_CLIENT_CERT needs server TLS (SSL_CERTFILE and SSL_KEYFILE)"
            )
        return self

    @property
    def tls_enabled(self) -> bool:
        return bool(self.SSL_CERTFILE and self.SSL_KEYFILE)


settings = Settings()  # type: ignore

print(settings)
