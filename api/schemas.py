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
    customer_id: Optional[str] = None
    customer_type: Optional[str] = None
    loan_product_type: Optional[str] = None
    loan_status_code: Optional[str] = None
    loan_status_flag: Optional[str] = None
    days_past_due: Optional[int] = None
    original_loan_amount: Optional[float] = None
    outstanding_principal_balance: Optional[float] = None
    nominal_interest_rate: Optional[float] = None
    total_installment_count: Optional[int] = None
    outstanding_installment_count: Optional[int] = None
    loan_start_date: Optional[str] = None
    final_maturity_date: Optional[str] = None
    internal_rating: Optional[str] = None
    sector_code: Optional[str] = None
    customer_segment: Optional[str] = None