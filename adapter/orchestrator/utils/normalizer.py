"""
ETL normalization layer for external bank data.

Converts raw values from the simulator API / CSV into types and formats
expected by ClickHouse (ISO dates, Decimals, canonical enum codes).
Raises NormalizationError on invalid data so the ETL can skip or log bad rows.
"""

import logging
import re
from decimal import Decimal, InvalidOperation
from datetime import datetime
from typing import Any, Optional, Type, Dict
from enum import Enum

from orchestrator.constants import (
    CustomerType, LoanStatusCode, LoanStatusFlag, 
    InsuranceIncluded, InstallmentStatus,
    CUSTOMER_TYPE_LABELS, LOAN_STATUS_CODE_LABELS,
    LOAN_STATUS_FLAG_LABELS, INSURANCE_INCLUDED_LABELS,
    INSTALLMENT_STATUS_LABELS
)

logger = logging.getLogger(__name__)


class NormalizationError(Exception):
    """Raised when a field cannot be normalized; used by ETL to reject or log bad rows."""


class DataNormalizer:
    """
    Centralized cleaning and type coercion for external bank data before insert to ClickHouse.
    """

    # Supported date formats, in order of expected frequency (ISO first).
    DATE_FORMATS = [
        "%Y-%m-%d",   # ISO: 2025-09-01
        "%Y%m%d",     # Compact: 20250901
        "%d.%m.%Y",   # TR: 01.09.2025
        "%d/%m/%Y",   # Slash: 01/09/2025
        "%b.%y"       # Legacy: May.24
    ]

    # Excel corruption: month names get substituted for digits (e.g. 5.14 -> "May.14").
    # Maps month abbreviations (EN + TR) to digit so we can reconstruct the number.
    MONTH_MAP = {
        'jan': '1', 'feb': '2', 'mar': '3', 'apr': '4', 'may': '5', 'jun': '6',
        'jul': '7', 'aug': '8', 'sep': '9', 'oct': '10', 'nov': '11', 'dec': '12',
        'oca': '1', 'şub': '2', 'mar': '3', 'nis': '4', 'may': '5', 'haz': '6', # Turkish
        'tem': '7', 'ağu': '8', 'eyl': '9', 'eki': '10', 'kas': '11', 'ara': '12'
    }

    @staticmethod
    def _repair_excel_rate(value_str: str) -> str:
        """
        Repairs Excel auto-format where decimals like 5.14 are saved as 'May.14' or '5.Mar'.
        Handles both patterns: month.digits (May.14) and digits.month (5.Mar).
        """
        lower_val = value_str.lower()
        
        # Pattern 1: month.digits (e.g., "may.14" -> "5.14")
        match = re.match(r"([a-zşçöğüı]{3})[\.]?(\d+)", lower_val)
        if match:
            month_str, remainder = match.groups()
            if month_str in DataNormalizer.MONTH_MAP:
                month_num = DataNormalizer.MONTH_MAP[month_str]
                return f"{month_num}.{remainder}"
        
        # Pattern 2: digits.month (e.g., "5.mar" -> "5.3")
        match = re.match(r"(\d+)[\.]([a-zşçöğüı]{3})", lower_val)
        if match:
            digits, month_str = match.groups()
            if month_str in DataNormalizer.MONTH_MAP:
                month_num = DataNormalizer.MONTH_MAP[month_str]
                return f"{digits}.{month_num}"
        
        return value_str

    @staticmethod
    def to_date(value: Any) -> Optional[str]:
        """
        Normalize date to ISO YYYY-MM-DD for ClickHouse Date columns.
        Tries DATE_FORMATS in order; raises NormalizationError if none match.
        """
        if not value:
            return None
        value_str = str(value).strip()
        if not value_str:  # Whitespace-only after strip
            return None
        for fmt in DataNormalizer.DATE_FORMATS:
            try:
                dt = datetime.strptime(value_str, fmt)
                return dt.date().isoformat()
            except ValueError:
                continue
        raise NormalizationError(f"Invalid date format: {value}")

    @staticmethod
    def to_decimal(value: Any, precision: int = 4) -> Optional[Decimal]:
        """
        Normalize monetary amounts to Decimal with fixed precision.
        Strips commas; does not interpret % or bps (use to_rate for rates).
        """
        if value is None or value == "":
            return None
        try:
            val_str = str(value).replace(",", "")
            d = Decimal(val_str)
            return d.quantize(Decimal(f"1.{'0' * precision}"))
        except InvalidOperation:
            raise NormalizationError(f"Invalid money amount: {value}")

    @staticmethod
    def to_rate(value: Any, precision: int = 6) -> Optional[Decimal]:
        """
        Normalize interest/tax rates to decimal (e.g. 0.0514).
        - Strips %, commas; bps values are divided by 10000.
        - Repairs Excel corruption (e.g. "May.14" -> 5.14 or "5.Mar" -> 5.3 then -> 0.0514/0.053).
        - If value >= 1, treats as percentage and divides by 100.
        """
        if value is None or value == "":
            return None
        try:
            val_str = str(value).strip()
            val_str = val_str.replace("%", "").replace(",", "")
            if 'bps' in val_str.lower():
                val_str = val_str.lower().replace('bps', '').strip()
                return (Decimal(val_str) / 10000).quantize(Decimal(f"1.{'0' * precision}"))

            val_str = DataNormalizer._repair_excel_rate(val_str)
            d = Decimal(val_str)
            # Percentage heuristic: 18.5 or 5.14 or 1.0 -> store as 0.185 / 0.0514 / 0.01
            if d >= 1:
                d = d / 100
            return d.quantize(Decimal(f"1.{'0' * precision}"))
        except InvalidOperation:
            raise NormalizationError(f"Invalid rate: {value}")

    @staticmethod
    def to_enum(value: Any, enum_class: Type[Enum], label_map: Dict[str, str]) -> Any:
        """
        Map raw value to canonical enum code for ClickHouse ENUM columns.
        Accepts either the code (e.g. "K") or the label (e.g. "Kapalı"); returns the code.
        """
        if not value:
            return None
        val_str = str(value).strip()
        if val_str in [e.value for e in enum_class]:
            return val_str
        # Label -> code lookup (case-insensitive)
        reverse_map = {v.upper(): k for k, v in label_map.items()}
        if val_str.upper() in reverse_map:
            return reverse_map[val_str.upper()]
        raise NormalizationError(f"Unknown category for {enum_class.__name__}: {val_str}")

    @staticmethod
    def _safe_normalize(normalizer_func, value, *args, **kwargs):
        """
        Safely apply a normalizer function; return None instead of raising on failure.
        Used for nullable fields during data profiling to track missing/invalid ratios.
        """
        try:
            return normalizer_func(value, *args, **kwargs)
        except (NormalizationError, ValueError, TypeError, InvalidOperation):
            return None

    @classmethod
    def normalize_credit_row(cls, row: dict, strict: bool = False) -> dict:
        """
        Normalize a single credit row from simulator CSV/API to ClickHouse schema.
        Handles both commercial and retail credit fields.
        
        Args:
            row: Raw data row (dict)
            strict: If True, raises NormalizationError on any failure.
                   If False (default), allows nulls for profiling purposes.
        
        Returns:
            Cleaned dict with normalized fields; nullable fields may be None.
        """
        cleaned = {}
        try:
            # ===== Identity (required for partitioning and ordering) =====
            cleaned['loan_account_number'] = str(row.get('loan_account_number', ''))
            cleaned['customer_id'] = str(row.get('customer_id', ''))
            cleaned['tenant_id'] = row.get('tenant_id')  # Injected by ETL
            cleaned['loan_type'] = row.get('loan_type')  # Injected by ETL

            # ===== Categorical: map to canonical codes (ClickHouse ENUMs) =====
            if strict:
                cleaned['customer_type'] = cls.to_enum(row.get('customer_type'), CustomerType, CUSTOMER_TYPE_LABELS)
                cleaned['loan_status_code'] = cls.to_enum(row.get('loan_status_code'), LoanStatusCode, LOAN_STATUS_CODE_LABELS)
            else:
                cleaned['customer_type'] = cls._safe_normalize(cls.to_enum, row.get('customer_type'), CustomerType, CUSTOMER_TYPE_LABELS)
                cleaned['loan_status_code'] = cls._safe_normalize(cls.to_enum, row.get('loan_status_code'), LoanStatusCode, LOAN_STATUS_CODE_LABELS)
            
            # loan_status_flag (present in commercial; defaults to loan_status_code in retail)
            if row.get('loan_status_flag'):
                cleaned['loan_status_flag'] = cls._safe_normalize(cls.to_enum, row.get('loan_status_flag'), LoanStatusFlag, LOAN_STATUS_FLAG_LABELS)
            else:
                # Default to same as loan_status_code if available
                cleaned['loan_status_flag'] = cleaned.get('loan_status_code')

            # ===== Loan Details =====
            cleaned['loan_product_type'] = str(row.get('loan_product_type', '')) if row.get('loan_product_type') not in (None, '') else None

            # ===== Dates: ISO YYYY-MM-DD only (ClickHouse Date columns) - allow None =====
            cleaned['final_maturity_date'] = cls._safe_normalize(cls.to_date, row.get('final_maturity_date'))
            cleaned['first_payment_date'] = cls._safe_normalize(cls.to_date, row.get('first_payment_date'))
            cleaned['loan_start_date'] = cls._safe_normalize(cls.to_date, row.get('loan_start_date'))
            cleaned['loan_closing_date'] = cls._safe_normalize(cls.to_date, row.get('loan_closing_date'))

            # ===== Installment Counts (Nullable Int32) - allow None for profiling =====
            # Convert to int only if value exists and is not empty; 0 is a valid value
            try:
                val = row.get('total_installment_count')
                cleaned['total_installment_count'] = int(val) if val not in (None, '') else None
            except (ValueError, TypeError):
                cleaned['total_installment_count'] = None
            
            try:
                val = row.get('outstanding_installment_count')
                cleaned['outstanding_installment_count'] = int(val) if val not in (None, '') else None
            except (ValueError, TypeError):
                cleaned['outstanding_installment_count'] = None
            
            try:
                val = row.get('paid_installment_count')
                cleaned['paid_installment_count'] = int(val) if val not in (None, '') else None
            except (ValueError, TypeError):
                cleaned['paid_installment_count'] = None
            
            try:
                val = row.get('installment_frequency')
                cleaned['installment_frequency'] = int(val) if val not in (None, '') else None
            except (ValueError, TypeError):
                cleaned['installment_frequency'] = None
            
            try:
                val = row.get('grace_period_months')
                cleaned['grace_period_months'] = int(val) if val not in (None, '') else None
            except (ValueError, TypeError):
                cleaned['grace_period_months'] = None
            
            try:
                val = row.get('days_past_due')
                cleaned['days_past_due'] = int(val) if val not in (None, '') else None
            except (ValueError, TypeError):
                cleaned['days_past_due'] = None

            # ===== Monetary amounts (Decimal 18,4) - allow None for profiling =====
            cleaned['original_loan_amount'] = cls._safe_normalize(cls.to_decimal, row.get('original_loan_amount'))
            cleaned['outstanding_principal_balance'] = cls._safe_normalize(cls.to_decimal, row.get('outstanding_principal_balance'))
            cleaned['total_interest_amount'] = cls._safe_normalize(cls.to_decimal, row.get('total_interest_amount'))
            cleaned['kkdf_amount'] = cls._safe_normalize(cls.to_decimal, row.get('kkdf_amount'))
            cleaned['bsmv_amount'] = cls._safe_normalize(cls.to_decimal, row.get('bsmv_amount'))

            # ===== Rates (Decimal 10,6): Excel repair + percentage heuristic - allow None =====
            cleaned['nominal_interest_rate'] = cls._safe_normalize(cls.to_rate, row.get('nominal_interest_rate'))
            cleaned['kkdf_rate'] = cls._safe_normalize(cls.to_rate, row.get('kkdf_rate'))
            cleaned['bsmv_rate'] = cls._safe_normalize(cls.to_rate, row.get('bsmv_rate'))

            # ===== Ratings & Risk (Nullable) =====
            cleaned['internal_rating'] = str(row['internal_rating']) if row.get('internal_rating') not in (None, '') else None
            cleaned['internal_credit_rating'] = str(row['internal_credit_rating']) if row.get('internal_credit_rating') not in (None, '') else None
            cleaned['external_rating'] = str(row['external_rating']) if row.get('external_rating') not in (None, '') else None
            cleaned['default_probability'] = cls._safe_normalize(cls.to_decimal, row.get('default_probability'), precision=6)
            cleaned['risk_class'] = str(row['risk_class']) if row.get('risk_class') not in (None, '') else None

            # ===== Demographics / Segments (Nullable) =====
            cleaned['sector_code'] = str(row['sector_code']) if row.get('sector_code') not in (None, '') else None
            cleaned['customer_segment'] = str(row['customer_segment']) if row.get('customer_segment') not in (None, '') else None
            cleaned['customer_province_code'] = str(row['customer_province_code']) if row.get('customer_province_code') not in (None, '') else None
            cleaned['customer_district_code'] = str(row['customer_district_code']) if row.get('customer_district_code') not in (None, '') else None
            cleaned['customer_region_code'] = str(row['customer_region_code']) if row.get('customer_region_code') not in (None, '') else None

            # insurance_included (retail only; Nullable ENUM)
            cleaned['insurance_included'] = cls._safe_normalize(cls.to_enum, row.get('insurance_included'), InsuranceIncluded, INSURANCE_INCLUDED_LABELS)

            return cleaned

        except Exception as e:
            if strict:
                raise NormalizationError(f"Row {row.get('loan_account_number', '?')}: {str(e)}")
            else:
                # In non-strict mode, log warning and return partial data
                logger.warning(f"Partial normalization for row {row.get('loan_account_number', '?')}: {str(e)}")
                return cleaned

    @classmethod
    def normalize_payment_row(cls, row: dict, strict: bool = False) -> dict:
        """
        Normalize a single payment row from simulator CSV/API to ClickHouse schema.
        Handles installment payment data for both commercial and retail loans.
        
        Args:
            row: Raw payment data row (dict)
            strict: If True, raises NormalizationError on any failure.
                   If False (default), allows nulls for profiling purposes.
        
        Returns:
            Cleaned dict with normalized payment fields; nullable fields may be None.
        """
        cleaned = {}
        try:
            # ===== Identity (required for partitioning and ordering) =====
            cleaned['loan_account_number'] = str(row.get('loan_account_number', ''))
            cleaned['tenant_id'] = row.get('tenant_id')  # Injected by ETL
            cleaned['loan_type'] = row.get('loan_type')  # Injected by ETL
            
            # ===== Installment Number (Nullable Int32) =====
            try:
                val = row.get('installment_number')
                cleaned['installment_number'] = int(val) if val not in (None, '') else None
            except (ValueError, TypeError):
                cleaned['installment_number'] = None

            # ===== Dates (Nullable) =====
            cleaned['scheduled_payment_date'] = cls._safe_normalize(cls.to_date, row.get('scheduled_payment_date'))
            cleaned['actual_payment_date'] = cls._safe_normalize(cls.to_date, row.get('actual_payment_date'))

            # ===== Payment Components (Nullable Decimals) =====
            cleaned['installment_amount'] = cls._safe_normalize(cls.to_decimal, row.get('installment_amount'))
            cleaned['principal_component'] = cls._safe_normalize(cls.to_decimal, row.get('principal_component'))
            cleaned['interest_component'] = cls._safe_normalize(cls.to_decimal, row.get('interest_component'))
            cleaned['kkdf_component'] = cls._safe_normalize(cls.to_decimal, row.get('kkdf_component'))
            cleaned['bsmv_component'] = cls._safe_normalize(cls.to_decimal, row.get('bsmv_component'))

            # ===== Remaining Balances (Nullable Decimals) =====
            cleaned['remaining_principal'] = cls._safe_normalize(cls.to_decimal, row.get('remaining_principal'))
            cleaned['remaining_interest'] = cls._safe_normalize(cls.to_decimal, row.get('remaining_interest'))
            cleaned['remaining_kkdf'] = cls._safe_normalize(cls.to_decimal, row.get('remaining_kkdf'))
            cleaned['remaining_bsmv'] = cls._safe_normalize(cls.to_decimal, row.get('remaining_bsmv'))

            # ===== Status (Nullable ENUM) =====
            if strict:
                cleaned['installment_status'] = cls.to_enum(row.get('installment_status'), InstallmentStatus, INSTALLMENT_STATUS_LABELS)
            else:
                cleaned['installment_status'] = cls._safe_normalize(cls.to_enum, row.get('installment_status'), InstallmentStatus, INSTALLMENT_STATUS_LABELS)

            return cleaned

        except Exception as e:
            if strict:
                raise NormalizationError(f"Payment row {row.get('loan_account_number', '?')}/{row.get('installment_number', '?')}: {str(e)}")
            else:
                # In non-strict mode, log warning and return partial data
                logger.warning(f"Partial normalization for payment {row.get('loan_account_number', '?')}/{row.get('installment_number', '?')}: {str(e)}")
                return cleaned