"""
Categorical field definitions for ClickHouse tables.

Used for:
- Data validation before inserting to ClickHouse
- Admin display (human-readable labels)
- API responses
- Data quality checks
- Schema documentation

Uses Python Enums for type safety and better IDE support.
"""

from enum import Enum

# ============================================================================
# Loan/File Type Categories
# ============================================================================

class LoanType(str, Enum):
    """Loan type classification for credits and payments."""
    COMMERCIAL_CREDIT = 'commercial_credit'
    COMMERCIAL_PAYMENT = 'commercial_payment'
    RETAIL_CREDIT = 'retail_credit'
    RETAIL_PAYMENT = 'retail_payment'
    
    @property
    def label(self):
        return {
            'commercial_credit': 'Commercial Credit',
            'commercial_payment': 'Commercial Payment',
            'retail_credit': 'Retail Credit',
            'retail_payment': 'Retail Payment',
        }[self.value]
    
    @property
    def is_credit(self):
        """Check if this is a credit (loan) type."""
        return self.value in ('commercial_credit', 'retail_credit')
    
    @property
    def is_payment(self):
        """Check if this is a payment type."""
        return self.value in ('commercial_payment', 'retail_payment')
    
    @property
    def is_commercial(self):
        """Check if this is a commercial loan type."""
        return self.value.startswith('commercial_')
    
    @property
    def is_retail(self):
        """Check if this is a retail loan type."""
        return self.value.startswith('retail_')

# ============================================================================
# Credits Table Categories (as Enums)
# ============================================================================

class CustomerType(str, Enum):
    """Customer type classification."""
    TUZEL = 'T'      # Legal entity (Tüzel)
    VATANDAS = 'V'   # Individual citizen (Vatandaş)
    
    @property
    def label(self):
        return {
            'T': 'Tüzel',
            'V': 'Vatandaş',
        }[self.value]


class LoanStatusCode(str, Enum):
    """Loan status code."""
    AKTIF = 'A'   # Active
    KAPALI = 'K'  # Closed
    
    @property
    def label(self):
        return {
            'A': 'Aktif',
            'K': 'Kapalı',
        }[self.value]


class LoanStatusFlag(str, Enum):
    """Loan status flag."""
    AKTIF = 'A'   # Active
    KAPALI = 'K'  # Closed
    
    @property
    def label(self):
        return {
            'A': 'Aktif',
            'K': 'Kapalı',
        }[self.value]


class InsuranceIncluded(str, Enum):
    """Insurance inclusion status."""
    EVET = 'E'   # Yes
    HAYIR = 'H'  # No
    
    @property
    def label(self):
        return {
            'E': 'Evet',
            'H': 'Hayır',
        }[self.value]


class LoanProductType(int, Enum):
    """Loan product type classification."""
    COMMERCIAL_TYPE_1 = 1  # Ticari Kredi Tip 1
    COMMERCIAL_TYPE_2 = 2  # Ticari Kredi Tip 2
    RETAIL_TYPE_1 = 3      # Bireysel Kredi Tip 1
    RETAIL_TYPE_2 = 4      # Bireysel Kredi Tip 2
    # Add more as needed
    
    @property
    def label(self):
        return {
            1: 'Ticari Kredi Tip 1',
            2: 'Ticari Kredi Tip 2',
            3: 'Bireysel Kredi Tip 1',
            4: 'Bireysel Kredi Tip 2',
        }[self.value]


# ============================================================================
# Payments Table Categories (as Enums)
# ============================================================================

class InstallmentStatus(str, Enum):
    """Installment payment status."""
    AKTIF = 'A'   # Active/Unpaid
    KAPALI = 'K'  # Closed/Paid
    
    @property
    def label(self):
        return {
            'A': 'Aktif',
            'K': 'Kapalı',
        }[self.value]


# ============================================================================
# Backward Compatibility: Django-style Choices (for forms, admin, etc.)
# ============================================================================

LOAN_TYPE_CHOICES = [(e.value, e.label) for e in LoanType]
CUSTOMER_TYPE_CHOICES = [(e.value, e.label) for e in CustomerType]
LOAN_STATUS_CODE_CHOICES = [(e.value, e.label) for e in LoanStatusCode]
LOAN_STATUS_FLAG_CHOICES = [(e.value, e.label) for e in LoanStatusFlag]
INSURANCE_INCLUDED_CHOICES = [(e.value, e.label) for e in InsuranceIncluded]
INSTALLMENT_STATUS_CHOICES = [(e.value, e.label) for e in InstallmentStatus]
LOAN_PRODUCT_TYPE_CHOICES = [(e.value, e.label) for e in LoanProductType]

# ============================================================================
# Helper Sets for Fast Validation
# ============================================================================

VALID_LOAN_TYPES = {e.value for e in LoanType}
VALID_CUSTOMER_TYPES = {e.value for e in CustomerType}
VALID_LOAN_STATUS_CODES = {e.value for e in LoanStatusCode}
VALID_LOAN_STATUS_FLAGS = {e.value for e in LoanStatusFlag}
VALID_INSURANCE_INCLUDED = {e.value for e in InsuranceIncluded}
VALID_INSTALLMENT_STATUS = {e.value for e in InstallmentStatus}
VALID_LOAN_PRODUCT_TYPES = {e.value for e in LoanProductType}

# ============================================================================
# Display Mappings (code -> label) - for backward compatibility
# ============================================================================

LOAN_TYPE_LABELS = {e.value: e.label for e in LoanType}
CUSTOMER_TYPE_LABELS = {e.value: e.label for e in CustomerType}
LOAN_STATUS_CODE_LABELS = {e.value: e.label for e in LoanStatusCode}
LOAN_STATUS_FLAG_LABELS = {e.value: e.label for e in LoanStatusFlag}
INSURANCE_INCLUDED_LABELS = {e.value: e.label for e in InsuranceIncluded}
INSTALLMENT_STATUS_LABELS = {e.value: e.label for e in InstallmentStatus}
LOAN_PRODUCT_TYPE_LABELS = {e.value: e.label for e in LoanProductType}

# ============================================================================
# ClickHouse ENUM Definitions (for schema creation)
# ============================================================================

CH_CUSTOMER_TYPE_ENUM = "Enum8('T' = 1, 'V' = 2)"
CH_LOAN_STATUS_CODE_ENUM = "Enum8('A' = 1, 'K' = 2)"
CH_LOAN_STATUS_FLAG_ENUM = "Enum8('A' = 1, 'K' = 2)"
CH_INSURANCE_INCLUDED_ENUM = "Enum8('E' = 1, 'H' = 2)"
CH_INSTALLMENT_STATUS_ENUM = "Enum8('A' = 1, 'K' = 2)"

# ============================================================================
# Helper Functions
# ============================================================================

def validate_categorical_field(field_name, value, valid_set):
    """
    Validate that a categorical field value is in the allowed set.
    
    Args:
        field_name: Name of the field (for error messages)
        value: The value to validate
        valid_set: Set of valid values
        
    Raises:
        ValueError: If value is not in valid_set
        
    Returns:
        True if valid
    """
    if value not in valid_set:
        raise ValueError(
            f"Invalid {field_name}: '{value}'. "
            f"Allowed values: {', '.join(sorted(valid_set))}"
        )
    return True


def get_field_label(field_name, value):
    """
    Get human-readable label for a categorical field value.
    
    Args:
        field_name: Name of the field
        value: The code/value
        
    Returns:
        Human-readable label or the original value if not found
    """
    label_maps = {
        'customer_type': CUSTOMER_TYPE_LABELS,
        'loan_status_code': LOAN_STATUS_CODE_LABELS,
        'loan_status_flag': LOAN_STATUS_FLAG_LABELS,
        'insurance_included': INSURANCE_INCLUDED_LABELS,
        'installment_status': INSTALLMENT_STATUS_LABELS,
        'loan_product_type': LOAN_PRODUCT_TYPE_LABELS,
    }
    
    label_map = label_maps.get(field_name, {})
    return label_map.get(value, value)
