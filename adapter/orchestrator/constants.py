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
# SyncJob Status (orchestrator)
# ============================================================================

class SyncJobStatus(str, Enum):
    """Sync job lifecycle status. Used by SyncJob model and tasks."""
    PENDING = 'PENDING'
    IN_PROGRESS = 'IN_PROGRESS'
    SUCCESS = 'SUCCESS'
    FAILED = 'FAILED'
    WARNING = 'WARNING'

    @property
    def label(self):
        return {
            'PENDING': 'Pending',
            'IN_PROGRESS': 'In Progress',
            'SUCCESS': 'Success',
            'FAILED': 'Failed',
            'WARNING': 'Warning',
        }[self.value]


SYNC_JOB_STATUS_CHOICES = [(e.value, e.label) for e in SyncJobStatus]
VALID_SYNC_JOB_STATUSES = {e.value for e in SyncJobStatus}

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
# Field Type and Schemas (Profiling)
# ============================================================================

class FieldType(str, Enum):
    """Field type for profiling stats calculation."""
    NUMERIC = 'numeric'      # Int32, Decimal -> min, max, avg, stddev
    CATEGORICAL = 'categorical'  # Enum, String codes -> unique_count, most_frequent
    DATE = 'date'            # Date -> min, max
    STRING = 'string'        # Free-form strings -> unique_count
    SKIP = 'skip'            # tenant_id, loan_type, inserted_at (metadata)


# Schema definitions: field_name -> FieldType
CREDITS_FIELD_SCHEMA = {
    # Metadata (skip in profiling)
    'tenant_id': FieldType.SKIP,
    'loan_type': FieldType.SKIP,
    'inserted_at': FieldType.SKIP,

    # Identifiers (string profiling)
    'loan_account_number': FieldType.STRING,
    'customer_id': FieldType.STRING,

    # Categorical
    'customer_type': FieldType.CATEGORICAL,
    'loan_product_type': FieldType.CATEGORICAL,
    'loan_status_code': FieldType.CATEGORICAL,
    'loan_status_flag': FieldType.CATEGORICAL,
    'insurance_included': FieldType.CATEGORICAL,
    'sector_code': FieldType.CATEGORICAL,
    'customer_segment': FieldType.CATEGORICAL,
    'risk_class': FieldType.CATEGORICAL,

    # Numeric (integers)
    'days_past_due': FieldType.NUMERIC,
    'total_installment_count': FieldType.NUMERIC,
    'outstanding_installment_count': FieldType.NUMERIC,
    'paid_installment_count': FieldType.NUMERIC,
    'installment_frequency': FieldType.NUMERIC,
    'grace_period_months': FieldType.NUMERIC,

    # Numeric (decimals - financials)
    'original_loan_amount': FieldType.NUMERIC,
    'outstanding_principal_balance': FieldType.NUMERIC,
    'nominal_interest_rate': FieldType.NUMERIC,
    'total_interest_amount': FieldType.NUMERIC,
    'kkdf_rate': FieldType.NUMERIC,
    'kkdf_amount': FieldType.NUMERIC,
    'bsmv_rate': FieldType.NUMERIC,
    'bsmv_amount': FieldType.NUMERIC,
    'default_probability': FieldType.NUMERIC,

    # Dates
    'final_maturity_date': FieldType.DATE,
    'first_payment_date': FieldType.DATE,
    'loan_start_date': FieldType.DATE,
    'loan_closing_date': FieldType.DATE,

    # Ratings (treated as categorical)
    'internal_rating': FieldType.CATEGORICAL,
    'internal_credit_rating': FieldType.CATEGORICAL,
    'external_rating': FieldType.CATEGORICAL,

    # Location codes (categorical)
    'customer_province_code': FieldType.CATEGORICAL,
    'customer_district_code': FieldType.CATEGORICAL,
    'customer_region_code': FieldType.CATEGORICAL,
}

PAYMENTS_FIELD_SCHEMA = {
    # Metadata (skip)
    'tenant_id': FieldType.SKIP,
    'loan_type': FieldType.SKIP,
    'inserted_at': FieldType.SKIP,

    # Identifiers
    'loan_account_number': FieldType.STRING,

    # Numeric
    'installment_number': FieldType.NUMERIC,
    'installment_amount': FieldType.NUMERIC,
    'principal_component': FieldType.NUMERIC,
    'interest_component': FieldType.NUMERIC,
    'kkdf_component': FieldType.NUMERIC,
    'bsmv_component': FieldType.NUMERIC,
    'remaining_principal': FieldType.NUMERIC,
    'remaining_interest': FieldType.NUMERIC,
    'remaining_kkdf': FieldType.NUMERIC,
    'remaining_bsmv': FieldType.NUMERIC,

    # Dates
    'actual_payment_date': FieldType.DATE,
    'scheduled_payment_date': FieldType.DATE,

    # Categorical
    'installment_status': FieldType.CATEGORICAL,
}

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

CREDIT_COLUMNS = (
    'loan_account_number', 'customer_id', 'tenant_id', 'loan_type',
    'customer_type', 'loan_status_code', 'loan_status_flag', 'loan_product_type',
    'final_maturity_date', 'first_payment_date', 'loan_start_date', 'loan_closing_date',
    'original_loan_amount', 'outstanding_principal_balance', 'total_interest_amount',
    'kkdf_amount', 'bsmv_amount', 'nominal_interest_rate', 'kkdf_rate', 'bsmv_rate',
    'total_installment_count', 'outstanding_installment_count', 'paid_installment_count',
    'installment_frequency', 'grace_period_months', 'days_past_due',
    'internal_rating', 'internal_credit_rating', 'external_rating', 
    'default_probability', 'risk_class', 'sector_code', 'customer_segment',
    'customer_province_code', 'customer_district_code', 'customer_region_code',
    'insurance_included'
)

PAYMENT_COLUMNS = (
    'loan_account_number', 'tenant_id', 'loan_type', 'installment_number',
    'actual_payment_date', 'scheduled_payment_date',
    'installment_amount', 'principal_component', 'interest_component',
    'kkdf_component', 'bsmv_component', 'installment_status',
    'remaining_principal', 'remaining_interest', 'remaining_kkdf', 'remaining_bsmv'
)

class LoanCategory(str, Enum):
    """
    High-level category for the Sync Job.
    Determines which pair of files to fetch.
    """
    COMMERCIAL = 'COMMERCIAL'
    RETAIL = 'RETAIL'