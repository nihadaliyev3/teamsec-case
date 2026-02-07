"""
ClickHouse SQL for data profiling (field-level statistics).
"""

from orchestrator.constants import FieldType


def count_rows_sql(table: str) -> str:
    """Total row count for a table."""
    return f"SELECT count() FROM {table}"


def profile_numeric_sql(table: str, field: str) -> str:
    """Min, max, avg, stddev, null count for numeric fields."""
    return f"""
    SELECT
        min({field}),
        max({field}),
        avg({field}),
        stddevPop({field}),
        countIf({field} IS NULL)
    FROM {table}
    """


def profile_categorical_sql(table: str, field: str) -> str:
    """Unique count, null count, most frequent value and its count."""
    return f"""
    SELECT
        uniqExact({field}),
        countIf({field} IS NULL),
        topK(1)({field})[1] AS most_freq,
        countIf({field} = topK(1)({field})[1])
    FROM {table}
    """


def profile_date_sql(table: str, field: str) -> str:
    """Min, max, null count for date fields."""
    return f"""
    SELECT
        min({field}),
        max({field}),
        countIf({field} IS NULL)
    FROM {table}
    """


def profile_string_sql(table: str, field: str) -> str:
    """Unique count, null-or-empty count for string fields."""
    return f"""
    SELECT
        uniqExact({field}),
        countIf({field} IS NULL OR {field} = '')
    FROM {table}
    """


def build_profiling_sql(table: str, field: str, field_type: FieldType) -> str | None:
    """Return the appropriate profiling SQL for the given field type."""
    if field_type == FieldType.NUMERIC:
        return profile_numeric_sql(table, field)
    if field_type == FieldType.CATEGORICAL:
        return profile_categorical_sql(table, field)
    if field_type == FieldType.DATE:
        return profile_date_sql(table, field)
    if field_type == FieldType.STRING:
        return profile_string_sql(table, field)
    return None
