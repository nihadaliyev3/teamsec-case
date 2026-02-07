"""
ClickHouse SQL for ETL operations (partition copy, count, cleanup).
"""


def copy_partition_sql(stg_table: str, base_table: str, tenant_id: str, loan_type: str) -> str:
    """Copy partition from base table into staging (reuse when version unchanged)."""
    return f"""
    INSERT INTO {stg_table}
    SELECT * FROM {base_table}
    WHERE tenant_id = '{tenant_id}' AND loan_type = '{loan_type}'
    """


def select_count_sql(table: str) -> str:
    """Row count for a table."""
    return f"SELECT count() FROM {table}"


def drop_table_sql(table: str) -> str:
    """Drop table if exists."""
    return f"DROP TABLE IF EXISTS {table}"
