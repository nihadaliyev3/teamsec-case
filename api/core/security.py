"""
FastAPI API Key authentication - validates X-API-Key against Tenant.api_token_hash.
"""
import hashlib

from fastapi import Depends, HTTPException, Security, status
from fastapi.security import APIKeyHeader
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from .infrastructure import get_pg_session

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def _hash_token(raw_token: str) -> str:
    return hashlib.sha256(raw_token.encode("utf-8")).hexdigest()


async def get_current_tenant(
    api_key: str = Security(api_key_header),
    db: AsyncSession = Depends(get_pg_session),
) -> str:
    """
    Validates X-API-Key against Tenant.api_token_hash (constant-time comparison via DB lookup).
    Returns tenant_id for isolation.
    """
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing API Key header (X-API-Key)",
        )

    token_hash = _hash_token(api_key)
    query = text(
        "SELECT tenant_id FROM orchestrator_tenant "
        "WHERE api_token_hash = :hash AND is_active = true"
    )
    result = await db.execute(query, {"hash": token_hash})
    row = result.fetchone()

    if not row:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API Key",
        )
    return row[0]
