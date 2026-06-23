from typing import Annotated

from fastapi import Depends, HTTPException, status
from fastapi_jwks.dependencies.jwk_auth import JWKSAuth
from fastapi_jwks.models.types import (
    JWKSAuthCredentials,
    JWKSConfig,
    JWTDecodeConfig,
)
from fastapi_jwks.validators import JWKSValidator
from pydantic import BaseModel, ConfigDict

from .config import settings

REQUIRED_JWT_FIELDS = ["exp", "iss", "aud"]


class TokenPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    iss: str
    aud: str | list[str]
    exp: int
    email: str
    email_verified: bool = False
    sub: str | None = None
    name: str | None = None


jwks_validator = JWKSValidator[TokenPayload](
    decode_config=JWTDecodeConfig(
        audience=settings.OIDC_AUDIENCES,
        issuer=settings.OIDC_ISSUER_URL,
        options={"require": REQUIRED_JWT_FIELDS},
    ),
    jwks_config=JWKSConfig(
        url=settings.OIDC_JWKS_URI or f"{settings.OIDC_ISSUER_URL}/keys"
    ),
)

jwks_auth = JWKSAuth[TokenPayload](jwks_validator)


def require_user(
    credentials: Annotated[JWKSAuthCredentials[TokenPayload], Depends(jwks_auth)],
) -> TokenPayload:
    payload = credentials.payload
    if not payload.email_verified or payload.email not in settings.EXPECTED_USERS:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User is not authorized to access this resource",
        )
    return payload
