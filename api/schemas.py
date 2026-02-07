from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime

# --- Request Models ---
class SyncRequest(BaseModel):
    loan_type: str = Field(..., example="commercial_credit")
    force: bool = False

# --- Response Models ---
class TaskResponse(BaseModel):
    task_id: str
    status: str
    message: str

class ProfilingData(BaseModel):
    tenant_id: str
    sync_date: datetime
    status: str
    total_rows: int
    validation_errors: Dict[str, Any]
    profiling_stats: Dict[str, Any]

class LoanData(BaseModel):
    loan_account_number: str
    original_loan_amount: Optional[float]
    outstanding_principal_balance: Optional[float]
    loan_status_code: Optional[str]
    days_past_due: Optional[int]