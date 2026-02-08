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





@router.get("/data/count")
def get_loan_count(
    loan_type: LoanCategoryParam,
    tenant_id: str = Depends(get_current_tenant),
):
    """Returns total row count for pagination."""
    client = ClickHouse.get()
    result = client.query(
        "SELECT count() FROM credits_all WHERE tenant_id = %(tenant)s AND loan_type = %(loan_type)s",
        parameters={"tenant": tenant_id, "loan_type": loan_type},
    )
    return {"count": result.result_rows[0][0]}


@router.get("/data", response_model=List[LoanData])
def get_loan_data(
    loan_type: LoanCategoryParam,
    limit: int = 100,
    offset: int = 0,
    tenant_id: str = Depends(get_current_tenant),
):
    """
    Fetches loan data filtered by tenant and loan_type. Supports pagination.
    """
    limit = min(limit, 5000)  # Cap for performance
    client = ClickHouse.get()
    query = """
    SELECT
        loan_account_number,
        customer_id,
        customer_type,
        loan_product_type,
        loan_status_code,
        loan_status_flag,
        days_past_due,
        original_loan_amount,
        outstanding_principal_balance,
        nominal_interest_rate,
        total_installment_count,
        outstanding_installment_count,
        loan_start_date,
        final_maturity_date,
        internal_rating,
        sector_code,
        customer_segment
    FROM credits_all
    WHERE tenant_id = %(tenant)s AND loan_type = %(loan_type)s
    ORDER BY loan_account_number
    LIMIT %(limit)s OFFSET %(offset)s
    """
    result = client.query(
        query,
        parameters={
            "tenant": tenant_id,
            "loan_type": loan_type,
            "limit": limit,
            "offset": offset,
        },
    )
    loans = []
    for row in result.result_rows:
        loans.append({
            "loan_account_number": row[0],
            "customer_id": str(row[1]) if row[1] else None,
            "customer_type": str(row[2]) if row[2] else None,
            "loan_product_type": str(row[3]) if row[3] else None,
            "loan_status_code": str(row[4]) if row[4] else None,
            "loan_status_flag": str(row[5]) if row[5] else None,
            "days_past_due": int(row[6]) if row[6] is not None else None,
            "original_loan_amount": float(row[7]) if row[7] else None,
            "outstanding_principal_balance": float(row[8]) if row[8] else None,
            "nominal_interest_rate": float(row[9]) if row[9] else None,
            "total_installment_count": int(row[10]) if row[10] is not None else None,
            "outstanding_installment_count": int(row[11]) if row[11] is not None else None,
            "loan_start_date": str(row[12]) if row[12] else None,
            "final_maturity_date": str(row[13]) if row[13] else None,
            "internal_rating": str(row[14]) if row[14] else None,
            "sector_code": str(row[15]) if row[15] else None,
            "customer_segment": str(row[16]) if row[16] else None,
        })
    return loans


@router.get("/profiling", response_model=List[ProfilingData])
async def get_profiling_stats(
    loan_type: LoanCategoryParam,
    db: AsyncSession = Depends(get_pg_session),
    tenant_id: str = Depends(get_current_tenant),
):
    from sqlalchemy import text

    # ... query stays the same ...
    query = text("""
        SELECT
            t.tenant_id, j.completed_at, j.status,
            r.total_rows_processed, r.validation_errors, r.profiling_stats
        FROM orchestrator_syncreport r
        JOIN orchestrator_syncjob j ON r.job_id = j.id
        JOIN orchestrator_tenant t ON j.tenant_id = t.id
        WHERE t.tenant_id = :tenant AND j.loan_category = :loan_category AND j.status = 'SUCCESS'
        ORDER BY j.completed_at DESC
        LIMIT 5
    """)
    result = await db.execute(
        query, {"tenant": tenant_id, "loan_category": loan_type}
    )
    rows = result.fetchall()
    
    reports = []
    for row in rows:
        raw_errors = row[4]
        final_errors = {}
        
        if isinstance(raw_errors, list):
            # Wrap the list in a dict to satisfy the contract
            final_errors = {"general_errors": raw_errors}
        elif isinstance(raw_errors, dict):
            final_errors = raw_errors
        
        reports.append({
            "tenant_id": row[0],
            "sync_date": row[1],
            "status": row[2],
            "total_rows": row[3],
            "validation_errors": final_errors, 
            "profiling_stats": row[5] or {},
        })
    return reports