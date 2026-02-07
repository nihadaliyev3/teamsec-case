"""
ClickHouse SQL queries for the ETL pipeline.

Centralizes all SQL used by orchestrator tasks for:
- Validation (ghost loans, orphan payments, negative balances)
- Profiling (field stats by type)
- ETL (partition copy, count, cleanup)
"""

from .validation import ghost_loans_sql, orphan_payments_sql, negative_balances_sql
from .profiling import (
    count_rows_sql,
    profile_numeric_sql,
    profile_categorical_sql,
    profile_date_sql,
    profile_string_sql,
    build_profiling_sql,
)
from .etl import copy_partition_sql, select_count_sql, drop_table_sql

__all__ = [
    'ghost_loans_sql',
    'orphan_payments_sql',
    'negative_balances_sql',
    'count_rows_sql',
    'profile_numeric_sql',
    'profile_categorical_sql',
    'profile_date_sql',
    'profile_string_sql',
    'build_profiling_sql',
    'copy_partition_sql',
    'select_count_sql',
    'drop_table_sql',
]
