import json
import logging
import requests
import ijson
from celery import shared_task
from django.utils import timezone
from django.conf import settings

from orchestrator.constants import (
    LoanCategory,
    SyncJobStatus,
    CREDIT_COLUMNS,
    PAYMENT_COLUMNS,
    FieldType,
    CREDITS_FIELD_SCHEMA,
    PAYMENTS_FIELD_SCHEMA,
)
from orchestrator.models import SyncJob, Tenant, SyncReport
from orchestrator.sql import (
    ghost_loans_sql,
    orphan_payments_sql,
    negative_balances_sql,
    count_rows_sql,
    build_profiling_sql,
    copy_partition_sql,
    select_count_sql,
    drop_table_sql,
)
from orchestrator.utils.normalizer import DataNormalizer
from utils.ch_client import ch_client

logger = logging.getLogger(__name__)

# Configurable Batch Size
BATCH_SIZE = getattr(settings, 'CLICKHOUSE_INSERT_BATCH_SIZE', 10000)

class ValidationException(Exception):
    """Halts the pipeline when data quality rules are violated."""
    pass

# ============================================================================
# 1. HELPER FUNCTIONS (Network & ClickHouse)
# ============================================================================

def get_remote_version(tenant, file_type):
    """
    Performs a lightweight HEAD request to check file version.
    """
    try:
        url = f"{tenant.api_url}"
        headers = {'Authorization': f'Bearer {tenant.api_token}'}
        params = {'file_type': file_type, 'tenant': tenant.tenant_id}
        
        response = requests.head(url, params=params, headers=headers, timeout=5)
        
        if response.status_code == 200:
            version_header = response.headers.get('X-Data-Version')
            if version_header is not None:
                return int(version_header)
        return None
    except Exception as e:
        logger.warning(f"Version check failed for {tenant.tenant_id} {file_type}: {e}")
        return None

def stream_to_staging(tenant, file_type, staging_table, table_type, loan_category):
    """
    Streams JSON from Simulator -> Normalizes -> Batch Inserts to ClickHouse Staging.
    """
    url = f"{tenant.api_url}"
    params = {'file_type': file_type, 'tenant': tenant.tenant_id}
    headers = {'Authorization': f'Bearer {tenant.api_token}'}
    
    # stream=True prevents loading 200MB into RAM
    response = requests.get(url, params=params, headers=headers, stream=True)
    if response.status_code != 200:
        raise Exception(f"API Error {response.status_code}: {response.text}")

    # Configuration based on table type
    if table_type == 'credits':
        columns = CREDIT_COLUMNS
        normalize_func = DataNormalizer.normalize_credit_row
    else:
        columns = PAYMENT_COLUMNS
        normalize_func = DataNormalizer.normalize_payment_row

    batch = []
    total_rows = 0
    
    # ijson parses the stream byte-by-byte
    for record in ijson.items(response.raw, 'item'):
        # Inject Context
        record['tenant_id'] = tenant.tenant_id
        record['loan_type'] = loan_category # e.g. COMMERCIAL
        
        # Normalize (strict=False to allow loading dirty data for validation)
        cleaned_row = normalize_func(record, strict=False)
        
        # Prepare row for ClickHouse
        row_values = [cleaned_row.get(col) for col in columns]
        batch.append(row_values)
        
        if len(batch) >= BATCH_SIZE:
            ch_client.insert_batch(staging_table, batch, columns)
            total_rows += len(batch)
            batch = []

    if batch:
        ch_client.insert_batch(staging_table, batch, columns)
        total_rows += len(batch)
        
    logger.info(f"Loaded {total_rows} rows into {staging_table}")
    return total_rows


def run_validation_suite(stg_credits, stg_payments):
    """
    ELT Validation: Runs SQL checks on Staging Tables.
    Returns list of error strings. Must be critical to abort the sync.
    """
    client = ch_client.connect()
    critical_errors = []
    quality_warnings = []

    # 1. CRITICAL: Missing ID (Must Kill)
    ghost_count = client.command(ghost_loans_sql(stg_credits))
    if ghost_count > 0:
        critical_errors.append(f"CRITICAL: {ghost_count} rows missing Loan Account Number. Sync Aborted.")

    # 2. QUALITY: Orphan Payments (Accept but warn)
    if stg_payments:
        orphan_count = client.command(orphan_payments_sql(stg_payments, stg_credits))
        if orphan_count > 0:
            quality_warnings.append(f"WARNING: {orphan_count} payments are orphans (no matching loan).")

    # 3. QUALITY: Negative Balances (Accept but warn)
    neg_count = client.command(negative_balances_sql(stg_credits))
    if neg_count > 0:
        quality_warnings.append(f"WARNING: {neg_count} loans have negative balances.")

    return critical_errors, quality_warnings


def calculate_profiling_stats(stg_table: str, table_type: str) -> dict:
    """
    Requirement #10: Calculate comprehensive profiling stats for all fields.
    
    Args:
        stg_table: Staging table name (e.g., stg_bank001_commercial_credits)
        table_type: 'credits' or 'payments' to select the appropriate schema
        
    Returns:
        Dict with stats per field based on field type:
        - Numeric: min, max, avg, stddev, null_ratio
        - Categorical: unique_count, most_frequent, most_frequent_pct, null_ratio
        - Date: min, max, null_ratio
        - String: unique_count, null_ratio
    """
    client = ch_client.connect()
    schema = CREDITS_FIELD_SCHEMA if table_type == 'credits' else PAYMENTS_FIELD_SCHEMA
    stats = {}
    
    # Get total row count once
    try:
        total_rows = client.query(count_rows_sql(stg_table)).result_rows[0][0]
        if total_rows == 0:
            logger.warning(f"Profiling: {stg_table} is empty")
            return {'_meta': {'total_rows': 0, 'table': stg_table}}
    except Exception as e:
        logger.error(f"Profiling failed to get row count: {e}")
        return {'_meta': {'error': str(e)}}
    
    stats['_meta'] = {'total_rows': total_rows, 'table': stg_table}
    
    for field_name, field_type in schema.items():
        if field_type == FieldType.SKIP:
            continue
            
        try:
            field_stats = _profile_field(client, stg_table, field_name, field_type, total_rows)
            if field_stats:
                stats[field_name] = field_stats
        except Exception as e:
            logger.warning(f"Profiling failed for {field_name}: {e}")
            stats[field_name] = {'error': str(e)}
    
    return stats


def _profile_field(client, table: str, field: str, field_type, total_rows: int) -> dict | None:
    """
    Profile a single field based on its type.
    Returns dict with appropriate statistics.
    """
    sql = build_profiling_sql(table, field, field_type)
    if not sql:
        return None

    row = client.query(sql).result_rows[0]

    if field_type == FieldType.NUMERIC:
        return {
            'min': _safe_float(row[0]),
            'max': _safe_float(row[1]),
            'avg': _safe_float(row[2]),
            'stddev': _safe_float(row[3]),
            'null_ratio': round(row[4] / total_rows, 4) if total_rows > 0 else 0,
            'null_count': row[4],
        }

    if field_type == FieldType.CATEGORICAL:
        unique_count = row[0]
        null_count = row[1]
        most_frequent = row[2] if row[2] else None
        most_frequent_count = row[3] if row[3] else 0
        return {
            'unique_count': unique_count,
            'most_frequent': most_frequent,
            'most_frequent_pct': round(most_frequent_count / total_rows, 4) if total_rows > 0 else 0,
            'null_ratio': round(null_count / total_rows, 4) if total_rows > 0 else 0,
            'null_count': null_count,
        }

    if field_type == FieldType.DATE:
        return {
            'min': str(row[0]) if row[0] else None,
            'max': str(row[1]) if row[1] else None,
            'null_ratio': round(row[2] / total_rows, 4) if total_rows > 0 else 0,
            'null_count': row[2],
        }

    if field_type == FieldType.STRING:
        return {
            'unique_count': row[0],
            'null_or_empty_ratio': round(row[1] / total_rows, 4) if total_rows > 0 else 0,
            'null_or_empty_count': row[1],
        }

    return None


def _safe_float(val) -> float:
    """Safely convert to float, handling None and Decimal."""
    if val is None:
        return None
    try:
        return round(float(val), 4)
    except (TypeError, ValueError):
        return None

# ============================================================================
# 2. PERIODIC TASK: CHECKER (CDC)
# ============================================================================

def trigger_sync_logic(tenant, category, force=False):
    """
    Core logic to check versions and launch a SyncJob.
    Used by both the Periodic Scheduler and the Manual API.
    
    Args:
        tenant: Tenant model instance
        category: LoanCategory enum
        force: If True, skips the 'Is New Version?' check and runs anyway.
    
    Returns:
        job_id (int) if started, None if skipped.
    """
    prefix = category.value.lower()
    credit_type = f"{prefix}_credit"
    payment_type = f"{prefix}_payment"
    
    # 1. Fetch Remote Versions
    v_credit = get_remote_version(tenant, credit_type)
    v_payment = get_remote_version(tenant, payment_type)
    
    if v_credit is None or v_payment is None:
        logger.warning(f"Skipping {tenant.tenant_id} {category}: API Unreachable")
        return None

    # 2. Check History (CDC)
    last_job = SyncJob.objects.filter(
        tenant=tenant,
        loan_category=category.value,
        status=SyncJobStatus.SUCCESS.value
    ).order_by('-completed_at').first()

    # 3. The Decision
    has_update = (
        not last_job or 
        last_job.remote_version_credit != v_credit or 
        last_job.remote_version_payment != v_payment
    )

    if has_update or force:
        # Guard: Don't double-queue if already running
        is_running = SyncJob.objects.filter(
            tenant=tenant,
            loan_category=category.value,
            status__in=[SyncJobStatus.PENDING.value, SyncJobStatus.IN_PROGRESS.value]
        ).exists()

        if is_running:
            logger.info(f"Skipping {tenant.tenant_id} {category}: Job already running")
            return None

        # 4. Launch
        logger.info(f"Triggering Sync for {tenant.tenant_id} {category} (Force={force})")
        job = SyncJob.objects.create(
            tenant=tenant,
            loan_category=category.value,
            remote_version_credit=v_credit,
            remote_version_payment=v_payment,
            status=SyncJobStatus.PENDING.value
        )
        process_sync.delay(job.id)
        return job.id
    
    return None

@shared_task
def check_for_updates():
    """
    Polls Simulator for version changes.
    Triggers process_sync only if new data exists.
    """
    tenants = Tenant.objects.filter(is_active=True)
    
    for tenant in tenants:
        for category in [LoanCategory.COMMERCIAL, LoanCategory.RETAIL]:
            trigger_sync_logic(tenant, category, force=False)

# ============================================================================
# 3. WORKER TASK: PROCESS SYNC (ETL)
# ============================================================================

@shared_task
def process_sync(job_id):
    """
    ETL Process: Downloads, Validates, Profilings, and Swaps. Must be atomic.
    """
    job = SyncJob.objects.get(id=job_id)
    tenant = job.tenant
    category = job.loan_category # e.g. 'COMMERCIAL'
    prefix = category.lower()    # 'commercial'
    
    job.status = 'IN_PROGRESS'
    job.started_at = timezone.now()
    job.save()
    
    stg_credits = None
    stg_payments = None
    
    try:
        # 1. Check which files actually need downloading
        last_job = SyncJob.objects.filter(tenant=tenant, loan_category=category, status=SyncJobStatus.SUCCESS.value).first()
        
        needs_credit_dl = not last_job or last_job.remote_version_credit != job.remote_version_credit
        needs_payment_dl = not last_job or last_job.remote_version_payment != job.remote_version_payment

        # 2. Prepare Staging
        stg_credits = ch_client.prepare_staging(tenant.tenant_id, category, 'credits_all')
        stg_payments = ch_client.prepare_staging(tenant.tenant_id, category, 'payments_all')

        # 3. SELECTIVE LOAD (The Efficiency Win)
        if needs_credit_dl:
            rows_credits = stream_to_staging(tenant, f"{prefix}_credit", stg_credits, 'credits', category)
        else:
            # OPTIMIZATION: Copy partition locally in CH instead of re-downloading
            client = ch_client.connect()
            client.command(copy_partition_sql(stg_credits, 'credits_all', tenant.tenant_id, category))
            rows_credits = client.command(select_count_sql(stg_credits))
            logger.info(f"Re-used local partition for {category} Credits (Version {job.remote_version_credit})")

        if needs_payment_dl:
            rows_payments = stream_to_staging(tenant, f"{prefix}_payment", stg_payments, 'payments', category)
        else:
            # OPTIMIZATION: Copy partition locally in CH instead of re-downloading
            client = ch_client.connect()
            client.command(copy_partition_sql(stg_payments, 'payments_all', tenant.tenant_id, category))
            rows_payments = client.command(select_count_sql(stg_payments))
            logger.info(f"Re-used local partition for {category} Payments (Version {job.remote_version_payment})")
        
        # D. VALIDATION
        critical_errors, quality_warnings = run_validation_suite(stg_credits, stg_payments)

        
        if critical_errors:
            # Rejection Path
            raise ValidationException(json.dumps(critical_errors))
            
        # E. PROFILING (On Clean Staging Data for both tables)
        logger.info("Calculating profiling stats for credits and payments...")
        profiling_stats = {
            'credits': calculate_profiling_stats(stg_credits, 'credits'),
            'payments': calculate_profiling_stats(stg_payments, 'payments'),
        }
        
        # F. ATOMIC SWAP
        ch_client.swap_partition(tenant.tenant_id, category, stg_credits, 'credits_all')
        ch_client.swap_partition(tenant.tenant_id, category, stg_payments, 'payments_all')
        
        # G. SUCCESS
        job.status = SyncJobStatus.SUCCESS.value
        job.completed_at = timezone.now()
        job.save()
        
        SyncReport.objects.create(
            job=job,
            total_rows_processed=rows_credits + rows_payments,
            profiling_stats=profiling_stats,
            validation_errors=quality_warnings
        )

    except ValidationException as ve:
        # Expected Data Error
        job.status = SyncJobStatus.FAILED.value
        job.error_message = "Data Validation Failed"
        job.completed_at = timezone.now()
        job.save()
        
        SyncReport.objects.create(
            job=job,
            validation_errors=json.loads(str(ve)) # Store explicit errors for Frontend
        )
        
    except Exception as e:
        # Unexpected System Error
        logger.exception(f"Sync failed: {e}")
        job.status = SyncJobStatus.FAILED.value
        job.error_message = f"System Error: {str(e)}"
        job.completed_at = timezone.now()
        job.save()
        
    finally:
        # Cleanup: Drop Staging Tables (Success or Fail)
        try:
            client = ch_client.connect()
            if stg_credits:
                client.command(drop_table_sql(stg_credits))
            if stg_payments:
                client.command(drop_table_sql(stg_payments))
        except Exception as e:
            logger.error(f"Cleanup failed: {e}")