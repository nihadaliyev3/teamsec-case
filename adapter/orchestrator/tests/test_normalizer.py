"""
Comprehensive test suite for DataNormalizer.

Tests cover:
- Date normalization (ISO, compact, Turkish, various formats)
- Decimal and rate normalization (amounts, percentages, Excel corruption)
- Enum normalization (codes, labels, case-insensitive)
- Integer field handling (None, empty, zero)
- Credit row normalization (commercial and retail)
- Payment row normalization
- Edge cases (invalid data, missing fields, strict/lenient modes)
"""

import unittest
from decimal import Decimal
from datetime import date

from orchestrator.utils.normalizer import DataNormalizer, NormalizationError
from orchestrator.constants import (
    CustomerType, LoanStatusCode, LoanStatusFlag,
    InsuranceIncluded, InstallmentStatus
)


class TestDateNormalization(unittest.TestCase):
    """Test date format conversions and edge cases."""

    def test_iso_date_format(self):
        """ISO 8601 format (YYYY-MM-DD)."""
        result = DataNormalizer.to_date("2025-09-01")
        self.assertEqual(result, "2025-09-01")

    def test_compact_date_format(self):
        """Compact format (YYYYMMDD)."""
        result = DataNormalizer.to_date("20250901")
        self.assertEqual(result, "2025-09-01")

    def test_turkish_date_format(self):
        """Turkish format (DD.MM.YYYY)."""
        result = DataNormalizer.to_date("01.09.2025")
        self.assertEqual(result, "2025-09-01")

    def test_slash_date_format(self):
        """Slash format (DD/MM/YYYY)."""
        result = DataNormalizer.to_date("01/09/2025")
        self.assertEqual(result, "2025-09-01")

    def test_legacy_month_format(self):
        """Legacy format (May.24)."""
        result = DataNormalizer.to_date("May.24")
        self.assertEqual(result, "2024-05-01")

    def test_empty_date(self):
        """Empty string returns None."""
        result = DataNormalizer.to_date("")
        self.assertIsNone(result)

    def test_none_date(self):
        """None returns None."""
        result = DataNormalizer.to_date(None)
        self.assertIsNone(result)

    def test_invalid_date_format(self):
        """Invalid format raises NormalizationError."""
        with self.assertRaises(NormalizationError):
            DataNormalizer.to_date("invalid-date")

    def test_whitespace_trimming(self):
        """Whitespace is trimmed before parsing."""
        result = DataNormalizer.to_date("  2025-09-01  ")
        self.assertEqual(result, "2025-09-01")


class TestDecimalNormalization(unittest.TestCase):
    """Test decimal/amount conversions."""

    def test_basic_decimal(self):
        """Basic decimal string."""
        result = DataNormalizer.to_decimal("1000.50")
        self.assertEqual(result, Decimal("1000.5000"))

    def test_integer_to_decimal(self):
        """Integer converted to decimal."""
        result = DataNormalizer.to_decimal(1000)
        self.assertEqual(result, Decimal("1000.0000"))

    def test_decimal_with_commas(self):
        """Commas are stripped."""
        result = DataNormalizer.to_decimal("1,000,000.50")
        self.assertEqual(result, Decimal("1000000.5000"))

    def test_empty_decimal(self):
        """Empty string returns None."""
        result = DataNormalizer.to_decimal("")
        self.assertIsNone(result)

    def test_none_decimal(self):
        """None returns None."""
        result = DataNormalizer.to_decimal(None)
        self.assertIsNone(result)

    def test_invalid_decimal(self):
        """Invalid decimal raises NormalizationError."""
        with self.assertRaises(NormalizationError):
            DataNormalizer.to_decimal("not-a-number")

    def test_custom_precision(self):
        """Custom precision for rates."""
        result = DataNormalizer.to_decimal("0.123456", precision=6)
        self.assertEqual(result, Decimal("0.123456"))


class TestRateNormalization(unittest.TestCase):
    """Test rate conversions including Excel corruption repair."""

    def test_basic_rate_as_decimal(self):
        """Rate already in decimal form (0.0514)."""
        result = DataNormalizer.to_rate("0.0514")
        self.assertEqual(result, Decimal("0.051400"))

    def test_rate_as_percentage(self):
        """Rate as percentage (5.14 -> 0.0514)."""
        result = DataNormalizer.to_rate("5.14")
        self.assertEqual(result, Decimal("0.051400"))

    def test_rate_with_percent_sign(self):
        """Rate with % sign (18.5% -> 0.185)."""
        result = DataNormalizer.to_rate("18.5%")
        self.assertEqual(result, Decimal("0.185000"))

    def test_excel_corruption_may(self):
        """Excel corruption: May.14 -> 0.0514."""
        result = DataNormalizer.to_rate("May.14")
        self.assertEqual(result, Decimal("0.051400"))

    def test_excel_corruption_apr(self):
        """Excel corruption: Apr.75 -> 0.0475."""
        result = DataNormalizer.to_rate("Apr.75")
        self.assertEqual(result, Decimal("0.047500"))

    def test_excel_corruption_mar(self):
        """Excel corruption: 5.Mar -> 0.053."""
        result = DataNormalizer.to_rate("5.Mar")
        self.assertEqual(result, Decimal("0.053000"))

    def test_turkish_month_corruption(self):
        """Turkish month: Oca.15 -> 0.0115."""
        result = DataNormalizer.to_rate("Oca.15")
        self.assertEqual(result, Decimal("0.011500"))

    def test_rate_bps(self):
        """Basis points (150bps -> 0.0150)."""
        result = DataNormalizer.to_rate("150bps")
        self.assertEqual(result, Decimal("0.015000"))

    def test_rate_with_commas(self):
        """Rate with commas (5,14 -> 514 -> 5.14 after /100)."""
        # Note: Comma is stripped, so "5,14" becomes "514"
        result = DataNormalizer.to_rate("5,14")
        self.assertEqual(result, Decimal("5.140000"))

    def test_empty_rate(self):
        """Empty rate returns None."""
        result = DataNormalizer.to_rate("")
        self.assertIsNone(result)

    def test_none_rate(self):
        """None rate returns None."""
        result = DataNormalizer.to_rate(None)
        self.assertIsNone(result)


class TestEnumNormalization(unittest.TestCase):
    """Test enum code and label mapping."""

    def test_enum_with_code(self):
        """Enum value passed as code."""
        result = DataNormalizer.to_enum("T", CustomerType, {"T": "Tüzel", "V": "Vatandaş"})
        self.assertEqual(result, "T")

    def test_enum_with_label(self):
        """Enum value passed as label."""
        result = DataNormalizer.to_enum("Tüzel", CustomerType, {"T": "Tüzel", "V": "Vatandaş"})
        self.assertEqual(result, "T")

    def test_enum_case_insensitive_label(self):
        """Label matching is case-insensitive."""
        result = DataNormalizer.to_enum("TÜZEL", CustomerType, {"T": "Tüzel", "V": "Vatandaş"})
        self.assertEqual(result, "T")

    def test_enum_with_whitespace(self):
        """Whitespace is trimmed."""
        result = DataNormalizer.to_enum("  T  ", CustomerType, {"T": "Tüzel", "V": "Vatandaş"})
        self.assertEqual(result, "T")

    def test_enum_empty_returns_none(self):
        """Empty string returns None."""
        result = DataNormalizer.to_enum("", CustomerType, {"T": "Tüzel", "V": "Vatandaş"})
        self.assertIsNone(result)

    def test_enum_none_returns_none(self):
        """None returns None."""
        result = DataNormalizer.to_enum(None, CustomerType, {"T": "Tüzel", "V": "Vatandaş"})
        self.assertIsNone(result)

    def test_enum_invalid_value(self):
        """Invalid enum value raises NormalizationError."""
        with self.assertRaises(NormalizationError):
            DataNormalizer.to_enum("INVALID", CustomerType, {"T": "Tüzel", "V": "Vatandaş"})


class TestSafeNormalize(unittest.TestCase):
    """Test _safe_normalize helper for lenient mode."""

    def test_safe_normalize_success(self):
        """Successful normalization returns value."""
        result = DataNormalizer._safe_normalize(DataNormalizer.to_decimal, "100.50")
        self.assertEqual(result, Decimal("100.5000"))

    def test_safe_normalize_failure_returns_none(self):
        """Failed normalization returns None instead of raising."""
        result = DataNormalizer._safe_normalize(DataNormalizer.to_decimal, "invalid")
        self.assertIsNone(result)

    def test_safe_normalize_with_none(self):
        """None input returns None."""
        result = DataNormalizer._safe_normalize(DataNormalizer.to_date, None)
        self.assertIsNone(result)


class TestCreditRowNormalization(unittest.TestCase):
    """Test complete credit row normalization."""

    def setUp(self):
        """Set up test data."""
        self.commercial_row = {
            'loan_account_number': 'LOAN_000001',
            'customer_id': 'CUST_00001',
            'tenant_id': 'BANK001',
            'loan_type': 'commercial_credit',
            'customer_type': 'T',
            'loan_status_code': 'A',
            'loan_status_flag': 'A',
            'loan_product_type': '4',
            'days_past_due': '1',
            'final_maturity_date': '20250901',
            'first_payment_date': '20250901',
            'loan_start_date': '20250618',
            'total_installment_count': '1',
            'outstanding_installment_count': '1',
            'paid_installment_count': '0',
            'installment_frequency': '1',
            'grace_period_months': '0',
            'original_loan_amount': '28370',
            'outstanding_principal_balance': '26410',
            'nominal_interest_rate': '0',
            'total_interest_amount': '0',
            'kkdf_rate': '0',
            'kkdf_amount': '0',
            'bsmv_rate': 'May.14',
            'bsmv_amount': '0',
            'internal_rating': '8',
            'internal_credit_rating': '3',
            'external_rating': '934',
            'default_probability': '0.0217',
            'risk_class': '1',
            'customer_segment': '1',
            'customer_region_code': 'REGION_1',
            'sector_code': '4',
        }

        self.retail_row = {
            'customer_id': 'CUST_02237',
            'customer_type': 'I',
            'loan_account_number': 'LOAN_004441',
            'tenant_id': 'BANK001',
            'loan_type': 'retail_credit',
            'loan_status_code': 'A',
            'days_past_due': '0',
            'final_maturity_date': '20260302',
            'total_installment_count': '10',
            'outstanding_installment_count': '7',
            'paid_installment_count': '3',
            'first_payment_date': '20250402',
            'original_loan_amount': '98940',
            'outstanding_principal_balance': '88600',
            'nominal_interest_rate': '55.47',
            'total_interest_amount': '785.08',
            'kkdf_rate': '15.14',
            'kkdf_amount': '113.73',
            'bsmv_rate': '15.27',
            'bsmv_amount': '112.31',
            'grace_period_months': '0',
            'installment_frequency': '1',
            'loan_start_date': '20250302',
            'insurance_included': 'H',
            'customer_district_code': 'DISTRICT_B',
            'customer_province_code': 'PROVINCE_1',
            'internal_rating': '2',
            'external_rating': '1366',
        }

    def test_commercial_credit_normalization(self):
        """Complete commercial credit row normalization."""
        result = DataNormalizer.normalize_credit_row(self.commercial_row)
        
        # Identity
        self.assertEqual(result['loan_account_number'], 'LOAN_000001')
        self.assertEqual(result['customer_id'], 'CUST_00001')
        
        # Enums
        self.assertEqual(result['customer_type'], 'T')
        self.assertEqual(result['loan_status_code'], 'A')
        self.assertEqual(result['loan_status_flag'], 'A')
        
        # Dates
        self.assertEqual(result['final_maturity_date'], '2025-09-01')
        self.assertEqual(result['first_payment_date'], '2025-09-01')
        self.assertEqual(result['loan_start_date'], '2025-06-18')
        
        # Integers
        self.assertEqual(result['days_past_due'], 1)
        self.assertEqual(result['total_installment_count'], 1)
        
        # Decimals
        self.assertEqual(result['original_loan_amount'], Decimal('28370.0000'))
        self.assertEqual(result['outstanding_principal_balance'], Decimal('26410.0000'))
        
        # Rates (with Excel corruption repair)
        self.assertEqual(result['bsmv_rate'], Decimal('0.051400'))
        
        # Nullable fields
        self.assertEqual(result['sector_code'], '4')
        self.assertEqual(result['customer_region_code'], 'REGION_1')

    def test_retail_credit_normalization(self):
        """Complete retail credit row normalization."""
        result = DataNormalizer.normalize_credit_row(self.retail_row)
        
        # Retail-specific fields
        self.assertEqual(result['insurance_included'], 'H')
        self.assertEqual(result['customer_district_code'], 'DISTRICT_B')
        self.assertEqual(result['customer_province_code'], 'PROVINCE_1')
        
        # loan_status_flag defaults to loan_status_code in retail
        self.assertEqual(result['loan_status_flag'], 'A')
        
        # Percentage rates converted to decimals
        self.assertEqual(result['nominal_interest_rate'], Decimal('0.554700'))
        self.assertEqual(result['kkdf_rate'], Decimal('0.151400'))

    def test_credit_row_with_missing_optional_fields(self):
        """Missing optional fields should be None."""
        minimal_row = {
            'loan_account_number': 'LOAN_TEST',
            'customer_id': 'CUST_TEST',
            'tenant_id': 'BANK001',
            'loan_type': 'commercial_credit',
        }
        result = DataNormalizer.normalize_credit_row(minimal_row)
        
        self.assertIsNone(result['sector_code'])
        self.assertIsNone(result['customer_segment'])
        self.assertIsNone(result['default_probability'])
        self.assertIsNone(result['insurance_included'])

    def test_credit_row_strict_mode_with_invalid_data(self):
        """Strict mode raises error on invalid data."""
        invalid_row = {
            'loan_account_number': 'LOAN_TEST',
            'customer_type': 'INVALID',
            'tenant_id': 'BANK001',
            'loan_type': 'commercial_credit',
        }
        with self.assertRaises(NormalizationError):
            DataNormalizer.normalize_credit_row(invalid_row, strict=True)

    def test_credit_row_lenient_mode_with_invalid_data(self):
        """Lenient mode returns None for invalid data."""
        invalid_row = {
            'loan_account_number': 'LOAN_TEST',
            'customer_type': 'INVALID',
            'tenant_id': 'BANK001',
            'loan_type': 'commercial_credit',
        }
        result = DataNormalizer.normalize_credit_row(invalid_row, strict=False)
        self.assertIsNone(result['customer_type'])


class TestPaymentRowNormalization(unittest.TestCase):
    """Test complete payment row normalization."""

    def setUp(self):
        """Set up test payment data."""
        self.payment_row = {
            'loan_account_number': 'LOAN_000001',
            'tenant_id': 'BANK001',
            'loan_type': 'commercial_payment',
            'installment_number': '1',
            'scheduled_payment_date': '20250901',
            'actual_payment_date': '20250901',
            'installment_amount': '1500.50',
            'principal_component': '1000.00',
            'interest_component': '400.00',
            'kkdf_component': '50.25',
            'bsmv_component': '50.25',
            'installment_status': 'K',
            'remaining_principal': '25000.00',
            'remaining_interest': '500.00',
            'remaining_kkdf': '75.00',
            'remaining_bsmv': '75.00',
        }

    def test_complete_payment_normalization(self):
        """Complete payment row normalization."""
        result = DataNormalizer.normalize_payment_row(self.payment_row)
        
        # Identity
        self.assertEqual(result['loan_account_number'], 'LOAN_000001')
        self.assertEqual(result['tenant_id'], 'BANK001')
        self.assertEqual(result['loan_type'], 'commercial_payment')
        
        # Installment number
        self.assertEqual(result['installment_number'], 1)
        
        # Dates
        self.assertEqual(result['scheduled_payment_date'], '2025-09-01')
        self.assertEqual(result['actual_payment_date'], '2025-09-01')
        
        # Payment components
        self.assertEqual(result['installment_amount'], Decimal('1500.5000'))
        self.assertEqual(result['principal_component'], Decimal('1000.0000'))
        self.assertEqual(result['interest_component'], Decimal('400.0000'))
        
        # Remaining balances
        self.assertEqual(result['remaining_principal'], Decimal('25000.0000'))
        self.assertEqual(result['remaining_interest'], Decimal('500.0000'))
        
        # Status
        self.assertEqual(result['installment_status'], 'K')

    def test_payment_with_missing_optional_fields(self):
        """Missing optional fields should be None."""
        minimal_row = {
            'loan_account_number': 'LOAN_TEST',
            'tenant_id': 'BANK001',
            'loan_type': 'retail_payment',
        }
        result = DataNormalizer.normalize_payment_row(minimal_row)
        
        self.assertIsNone(result['installment_number'])
        self.assertIsNone(result['actual_payment_date'])
        self.assertIsNone(result['installment_amount'])

    def test_payment_strict_mode_with_invalid_status(self):
        """Strict mode raises error on invalid status."""
        invalid_row = {
            'loan_account_number': 'LOAN_TEST',
            'tenant_id': 'BANK001',
            'loan_type': 'retail_payment',
            'installment_status': 'INVALID',
        }
        with self.assertRaises(NormalizationError):
            DataNormalizer.normalize_payment_row(invalid_row, strict=True)

    def test_payment_lenient_mode_with_invalid_status(self):
        """Lenient mode returns None for invalid status."""
        invalid_row = {
            'loan_account_number': 'LOAN_TEST',
            'tenant_id': 'BANK001',
            'loan_type': 'retail_payment',
            'installment_status': 'INVALID',
        }
        result = DataNormalizer.normalize_payment_row(invalid_row, strict=False)
        self.assertIsNone(result['installment_status'])


class TestEdgeCases(unittest.TestCase):
    """Test edge cases and boundary conditions."""

    def test_zero_values_are_preserved(self):
        """Zero is a valid value, not treated as None."""
        result = DataNormalizer.to_decimal("0")
        self.assertEqual(result, Decimal("0.0000"))
        
        result = DataNormalizer.to_rate("0")
        self.assertEqual(result, Decimal("0.000000"))

    def test_empty_string_vs_none(self):
        """Empty string and None both return None."""
        self.assertIsNone(DataNormalizer.to_decimal(""))
        self.assertIsNone(DataNormalizer.to_decimal(None))

    def test_whitespace_only_treated_as_empty(self):
        """Whitespace-only strings are treated as empty."""
        result = DataNormalizer.to_date("   ")
        self.assertIsNone(result)

    def test_very_large_decimal(self):
        """Very large decimals are handled."""
        result = DataNormalizer.to_decimal("999999999999.9999")
        self.assertEqual(result, Decimal("999999999999.9999"))

    def test_very_small_rate(self):
        """Very small rates are handled."""
        result = DataNormalizer.to_rate("0.000001")
        self.assertEqual(result, Decimal("0.000001"))

    def test_negative_values(self):
        """Negative values are preserved."""
        result = DataNormalizer.to_decimal("-1000.50")
        self.assertEqual(result, Decimal("-1000.5000"))

    def test_rate_exactly_one(self):
        """Rate of exactly 1.0 is treated as 100% -> 0.01."""
        result = DataNormalizer.to_rate("1.0")
        self.assertEqual(result, Decimal("0.010000"))

    def test_rate_less_than_one(self):
        """Rate less than 1 is kept as-is."""
        result = DataNormalizer.to_rate("0.95")
        self.assertEqual(result, Decimal("0.950000"))


if __name__ == '__main__':
    unittest.main()
