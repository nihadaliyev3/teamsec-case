from typing import List, Literal

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from .config import settings
from .infrastructure import ClickHouse, get_pg_session
from .security import get_current_tenant
from schemas import LoanData, ProfilingData, TaskResponse

router = APIRouter()

LoanCategoryParam = Literal["COMMERCIAL", "RETAIL"]

class SyncPayload(BaseModel):
    loan_type: LoanCategoryParam # "COMMERCIAL" or "RETAIL"
    force: bool = False


@router.post("/sync", response_model=TaskResponse)
async def trigger_sync(
    payload: SyncPayload,
    request: Request,
    tenant_id: str = Depends(get_current_tenant),
):
    """
    Proxies to adapter SyncTriggerView. Forwards X-API-Key for auth.
    """
    adapter_url = f"{settings.ADAPTER_URL.rstrip('/')}/api/sync/"
    api_key = request.headers.get("X-API-Key")
    if not api_key:
        raise HTTPException(status_code=401, detail="Missing X-API-Key")

    body = {
        "loan_category": payload.loan_type.upper(),
        "force": payload.force,
    }
    headers = {"X-API-Key": api_key, "Content-Type": "application/json"}

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(adapter_url, json=body, headers=headers)

    if resp.status_code == 202:
        data = resp.json()
        return TaskResponse(
            task_id=str(data.get("job_id", "")),
            status="queued",
            message=f"Sync triggered for {tenant_id}",
        )
    if resp.status_code == 401:
        raise HTTPException(status_code=401, detail="Invalid API Key")
    if resp.status_code == 409:
        raise HTTPException(
            status_code=409,
            detail=resp.json().get("error", "Could not start sync job"),
        )
    raise HTTPException(
        status_code=resp.status_code,
        detail=resp.text or "Adapter request failed",
    )





@router.get("/data", response_model=List[LoanData])
def get_loan_data(
    loan_type: LoanCategoryParam,
    limit: int = 100,
    tenant_id: str = Depends(get_current_tenant),
):
    """
    Fetches loan data filtered by tenant and loan_type (matches credits_all.loan_type).
    """
    client = ClickHouse.get()
    query = """
    SELECT
        loan_account_number,
        original_loan_amount,
        outstanding_principal_balance,
        loan_status_code,
        days_past_due
    FROM credits_all
    WHERE tenant_id = %(tenant)s AND loan_type = %(loan_type)s
    LIMIT %(limit)s
    """
    result = client.query(
        query,
        parameters={"tenant": tenant_id, "loan_type": loan_type, "limit": limit},
    )
    loans = []
    for row in result.result_rows:
        loans.append({
            "loan_account_number": row[0],
            "original_loan_amount": float(row[1]) if row[1] else None,
            "outstanding_principal_balance": float(row[2]) if row[2] else None,
            "loan_status_code": row[3],
            "days_past_due": row[4],
        })
    return loans


@router.get("/profiling", response_model=List[ProfilingData])
async def get_profiling_stats(
    loan_type: LoanCategoryParam,
    db: AsyncSession = Depends(get_pg_session),
    tenant_id: str = Depends(get_current_tenant),
):
    """
    Fetches profiling reports filtered by tenant and loan_type (matches SyncJob.loan_category).
    """
    from sqlalchemy import text

    query = text("""
        SELECT
            j.tenant_id, j.completed_at, j.status,
            r.total_rows_processed, r.validation_errors, r.profiling_stats
        FROM orchestrator_syncreport r
        JOIN orchestrator_syncjob j ON r.job_id = j.id
        WHERE j.tenant_id = :tenant AND j.loan_category = :loan_category
        ORDER BY j.completed_at DESC
        LIMIT 5
    """)
    result = await db.execute(
        query, {"tenant": tenant_id, "loan_category": loan_type}
    )
    rows = result.fetchall()
    reports = []
    for row in rows:
        reports.append({
            "tenant_id": row[0],
            "sync_date": row[1],
            "status": row[2],
            "total_rows": row[3],
            "validation_errors": row[4] or {},
            "profiling_stats": row[5] or {},
        })
    return reports
