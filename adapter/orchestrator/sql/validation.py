"""
ClickHouse SQL for ELT validation checks on staging tables.
"""


def ghost_loans_sql(stg_credits: str) -> str:
    """Rows with empty loan_account_number (critical - sync abort)."""
    return f"SELECT count() FROM {stg_credits} WHERE trim(loan_account_number) = '' OR loan_account_number = 'None'"
    


def orphan_payments_sql(stg_payments: str, stg_credits: str) -> str:
    """Payments referencing non-existent loans in this batch."""
    return f"""
    SELECT count() FROM {stg_payments}
    WHERE loan_account_number NOT IN (SELECT loan_account_number FROM {stg_credits})
    """


def negative_balances_sql(stg_credits: str) -> str:
    """Loans with negative outstanding_principal_balance."""
    return f"SELECT count() FROM {stg_credits} WHERE outstanding_principal_balance < 0"
