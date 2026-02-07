"""
Comprehensive test suite for orchestrator tasks.

Tests cover:
- get_remote_version (HEAD request, version header, errors, timeouts)
- stream_to_staging (credits vs payments, batch inserts, API errors)
- run_validation_suite (ghost loans, orphans, negative balances)
- calculate_profiling_stats (empty table, errors, credits vs payments schema)
- _profile_field (NUMERIC, CATEGORICAL, DATE, STRING, edge cases)
- _safe_float (None, int, float, Decimal, invalid types)
- trigger_sync_logic (version check, CDC, force, double-queue guard)
- check_for_updates (periodic task)
- process_sync (full ETL, ValidationException, generic exception, cleanup)
"""

import json
from decimal import Decimal
from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase, TestCase, override_settings

from django.utils import timezone

from orchestrator.constants import LoanCategory, SyncJobStatus, FieldType
from orchestrator.models import SyncJob, Tenant, SyncReport
from orchestrator.tasks import (
    ValidationException,
    get_remote_version,
    stream_to_staging,
    run_validation_suite,
    calculate_profiling_stats,
    _profile_field,
    _safe_float,
    trigger_sync_logic,
    check_for_updates,
    process_sync,
)


# Use in-memory SQLite for fast tests (no Postgres required)
@override_settings(
    DATABASES={
        'default': {
            'ENGINE': 'django.db.backends.sqlite3',
            'NAME': ':memory:',
        }
    },
    CELERY_TASK_ALWAYS_EAGER=True,  # Run tasks synchronously in tests
)
class TestValidationException(SimpleTestCase):
    """Test custom exception."""

    def test_validation_exception_is_exception(self):
        """ValidationException subclasses Exception."""
        exc = ValidationException("Test error")
        self.assertIsInstance(exc, Exception)
        self.assertEqual(str(exc), "Test error")

    def test_validation_exception_with_json_message(self):
        """ValidationException can wrap JSON for structured errors."""
        errors = ["Error 1", "Error 2"]
        exc = ValidationException(json.dumps(errors))
        parsed = json.loads(str(exc))
        self.assertEqual(parsed, errors)


@override_settings(
    DATABASES={
        'default': {
            'ENGINE': 'django.db.backends.sqlite3',
            'NAME': ':memory:',
        }
    },
)
class TestGetRemoteVersion(SimpleTestCase):
    """Test get_remote_version HEAD request logic."""

    def setUp(self):
        """Create mock tenant."""
        self.tenant = MagicMock()
        self.tenant.api_url = "http://simulator:8000/api/data"
        self.tenant.api_token = "test-token"
        self.tenant.tenant_id = "BANK001"

    @patch('orchestrator.tasks.requests.head')
    def test_returns_version_when_header_present(self, mock_head):
        """200 with X-Data-Version header returns int version."""
        mock_head.return_value.status_code = 200
        mock_head.return_value.headers = {'X-Data-Version': '42'}
        result = get_remote_version(self.tenant, 'commercial_credit')
        self.assertEqual(result, 42)
        mock_head.assert_called_once_with(
            'http://simulator:8000/api/data',
            params={'file_type': 'commercial_credit', 'tenant': 'BANK001'},
            headers={'Authorization': 'Bearer test-token'},
            timeout=5
        )

    @patch('orchestrator.tasks.requests.head')
    def test_returns_none_when_header_missing(self, mock_head):
        """200 without X-Data-Version returns None."""
        mock_head.return_value.status_code = 200
        mock_head.return_value.headers = {}
        result = get_remote_version(self.tenant, 'retail_payment')
        self.assertIsNone(result)

    @patch('orchestrator.tasks.requests.head')
    def test_returns_none_on_non_200(self, mock_head):
        """Non-200 status returns None."""
        mock_head.return_value.status_code = 404
        mock_head.return_value.headers = {}
        result = get_remote_version(self.tenant, 'commercial_credit')
        self.assertIsNone(result)

    @patch('orchestrator.tasks.requests.head')
    def test_returns_none_on_exception(self, mock_head):
        """Network/request exception returns None (logs warning)."""
        mock_head.side_effect = ConnectionError("Connection refused")
        result = get_remote_version(self.tenant, 'commercial_credit')
        self.assertIsNone(result)

    @patch('orchestrator.tasks.requests.head')
    def test_invalid_version_header_returns_none(self, mock_head):
        """Non-numeric X-Data-Version is caught; returns None (logs warning)."""
        mock_head.return_value.status_code = 200
        mock_head.return_value.headers = {'X-Data-Version': 'not-a-number'}
        result = get_remote_version(self.tenant, 'commercial_credit')
        self.assertIsNone(result)


@override_settings(
    DATABASES={
        'default': {
            'ENGINE': 'django.db.backends.sqlite3',
            'NAME': ':memory:',
        }
    },
)
class TestStreamToStaging(SimpleTestCase):
    """Test stream_to_staging ETL flow."""

    def setUp(self):
        """Create mock tenant."""
        self.tenant = MagicMock()
        self.tenant.api_url = "http://simulator:8000/api/data"
        self.tenant.api_token = "token"
        self.tenant.tenant_id = "BANK001"

    @patch('orchestrator.tasks.ch_client')
    @patch('orchestrator.tasks.ijson.items')
    @patch('orchestrator.tasks.requests.get')
    def test_streams_credits_and_inserts_batches(self, mock_get, mock_ijson, mock_ch):
        """Streams credit records, normalizes, inserts in batches."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.raw = MagicMock()
        mock_get.return_value = mock_resp

        records = [
            {'loan_account_number': 'L1', 'customer_id': 'C1'},
            {'loan_account_number': 'L2', 'customer_id': 'C2'},
        ]
        mock_ijson.return_value = records

        with patch('orchestrator.tasks.DataNormalizer') as mock_norm:
            mock_norm.normalize_credit_row.side_effect = lambda r, strict=False: {
                'loan_account_number': r['loan_account_number'],
                'customer_id': r['customer_id'],
                'tenant_id': r['tenant_id'],
                'loan_type': r['loan_type'],
            }

            result = stream_to_staging(
                self.tenant, 'commercial_credit', 'stg_test', 'credits', 'COMMERCIAL'
            )

        self.assertEqual(result, 2)
        mock_ch.insert_batch.assert_called_once()
        call_args = mock_ch.insert_batch.call_args
        self.assertEqual(call_args[0][0], 'stg_test')
        self.assertEqual(len(call_args[0][1]), 2)

    @patch('orchestrator.tasks.ch_client')
    @patch('orchestrator.tasks.ijson.items')
    @patch('orchestrator.tasks.requests.get')
    def test_streams_payments_uses_payment_columns(self, mock_get, mock_ijson, mock_ch):
        """Payments table uses PAYMENT_COLUMNS and normalize_payment_row."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.raw = MagicMock()
        mock_get.return_value = mock_resp
        mock_ijson.return_value = [{'loan_account_number': 'L1'}]

        with patch('orchestrator.tasks.DataNormalizer') as mock_norm:
            mock_norm.normalize_payment_row.side_effect = lambda r, strict=False: {
                'loan_account_number': r['loan_account_number'],
                'tenant_id': r['tenant_id'],
                'loan_type': r['loan_type'],
            }

            stream_to_staging(
                self.tenant, 'retail_payment', 'stg_pay', 'payments', 'RETAIL'
            )

            mock_norm.normalize_payment_row.assert_called()
            mock_norm.normalize_credit_row.assert_not_called()

    @patch('orchestrator.tasks.requests.get')
    def test_raises_on_api_error(self, mock_get):
        """Non-200 API response raises Exception."""
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_resp.text = "Internal Server Error"
        mock_get.return_value = mock_resp

        with self.assertRaises(Exception) as ctx:
            stream_to_staging(
                self.tenant, 'commercial_credit', 'stg_test', 'credits', 'COMMERCIAL'
            )
        self.assertIn("500", str(ctx.exception))

    @patch('orchestrator.tasks.ch_client')
    @patch('orchestrator.tasks.ijson.items')
    @patch('orchestrator.tasks.requests.get')
    def test_empty_stream_returns_zero(self, mock_get, mock_ijson, mock_ch):
        """Empty JSON stream returns 0 rows, no insert called."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.raw = MagicMock()
        mock_get.return_value = mock_resp
        mock_ijson.return_value = []  # No records

        result = stream_to_staging(
            self.tenant, 'commercial_credit', 'stg_test', 'credits', 'COMMERCIAL'
        )
        self.assertEqual(result, 0)
        mock_ch.insert_batch.assert_not_called()

    @patch('orchestrator.tasks.ch_client')
    @patch('orchestrator.tasks.ijson.items')
    @patch('orchestrator.tasks.requests.get')
    def test_batch_flush_at_threshold(self, mock_get, mock_ijson, mock_ch):
        """Batch is flushed when reaching BATCH_SIZE."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.raw = MagicMock()
        mock_get.return_value = mock_resp

        with patch('orchestrator.tasks.BATCH_SIZE', 2):
            mock_ijson.return_value = [
                {'loan_account_number': f'L{i}', 'customer_id': f'C{i}'}
                for i in range(5)
            ]

            with patch('orchestrator.tasks.DataNormalizer') as mock_norm:
                def norm(r, strict=False):
                    return {k: r.get(k) for k in ('loan_account_number', 'customer_id', 'tenant_id', 'loan_type')}
                mock_norm.normalize_credit_row.side_effect = norm

                result = stream_to_staging(
                    self.tenant, 'commercial_credit', 'stg_test', 'credits', 'COMMERCIAL'
                )

        self.assertEqual(result, 5)
        # Batch of 2, batch of 2, then remainder of 1
        self.assertEqual(mock_ch.insert_batch.call_count, 3)


@override_settings(
    DATABASES={
        'default': {
            'ENGINE': 'django.db.backends.sqlite3',
            'NAME': ':memory:',
        }
    },
)
class TestRunValidationSuite(SimpleTestCase):
    """Test run_validation_suite validation logic."""

    @patch('orchestrator.tasks.ch_client')
    def test_ghost_loans_critical_error(self, mock_ch):
        """Rows with empty loan_account_number produce critical error."""
        mock_client = MagicMock()
        mock_client.command.side_effect = [3, 0, 0]  # ghost, orphan, neg
        mock_ch.connect.return_value = mock_client

        critical, warnings = run_validation_suite('stg_credits', 'stg_payments')
        self.assertIn("CRITICAL", critical[0])
        self.assertIn("3", critical[0])
        self.assertIn("Loan Account Number", critical[0])

    @patch('orchestrator.tasks.ch_client')
    def test_orphan_payments_quality_warning(self, mock_ch):
        """Orphan payments produce quality warning, not critical."""
        mock_client = MagicMock()
        mock_client.command.side_effect = [0, 5, 0]
        mock_ch.connect.return_value = mock_client

        critical, warnings = run_validation_suite('stg_credits', 'stg_payments')
        self.assertEqual(len(critical), 0)
        self.assertTrue(any("orphan" in w.lower() for w in warnings))
        self.assertIn("5", warnings[0])

    @patch('orchestrator.tasks.ch_client')
    def test_negative_balances_quality_warning(self, mock_ch):
        """Negative balances produce quality warning."""
        mock_client = MagicMock()
        mock_client.command.side_effect = [0, 0, 2]
        mock_ch.connect.return_value = mock_client

        critical, warnings = run_validation_suite('stg_credits', 'stg_payments')
        self.assertEqual(len(critical), 0)
        self.assertTrue(any("negative" in w.lower() for w in warnings))

    @patch('orchestrator.tasks.ch_client')
    def test_empty_stg_payments_skips_orphan_check(self, mock_ch):
        """When stg_payments is None/empty, orphan check is skipped."""
        mock_client = MagicMock()
        mock_client.command.side_effect = [0, 0]  # ghost, neg only
        mock_ch.connect.return_value = mock_client

        critical, warnings = run_validation_suite('stg_credits', None)
        self.assertEqual(mock_client.command.call_count, 2)

    @patch('orchestrator.tasks.ch_client')
    def test_clean_data_returns_empty_lists(self, mock_ch):
        """No issues returns empty critical and warnings."""
        mock_client = MagicMock()
        mock_client.command.side_effect = [0, 0, 0]
        mock_ch.connect.return_value = mock_client

        critical, warnings = run_validation_suite('stg_credits', 'stg_payments')
        self.assertEqual(critical, [])
        self.assertEqual(warnings, [])


@override_settings(
    DATABASES={
        'default': {
            'ENGINE': 'django.db.backends.sqlite3',
            'NAME': ':memory:',
        }
    },
)
class TestCalculateProfilingStats(SimpleTestCase):
    """Test calculate_profiling_stats."""

    @patch('orchestrator.tasks.ch_client')
    def test_empty_table_returns_meta_only(self, mock_ch):
        """Empty table returns _meta with total_rows 0."""
        mock_client = MagicMock()
        mock_result = MagicMock()
        mock_result.result_rows = [[0]]
        mock_client.query.return_value = mock_result
        mock_ch.connect.return_value = mock_client

        result = calculate_profiling_stats('stg_credits', 'credits')
        self.assertEqual(result, {'_meta': {'total_rows': 0, 'table': 'stg_credits'}})

    @patch('orchestrator.tasks.ch_client')
    def test_query_error_returns_meta_with_error(self, mock_ch):
        """Query failure returns _meta with error key."""
        mock_client = MagicMock()
        mock_client.query.side_effect = Exception("Connection lost")
        mock_ch.connect.return_value = mock_client

        result = calculate_profiling_stats('stg_credits', 'credits')
        self.assertIn('_meta', result)
        self.assertIn('error', result['_meta'])

    @patch('orchestrator.tasks.ch_client')
    def test_credits_schema_used_for_credits(self, mock_ch):
        """Credits table uses CREDITS_FIELD_SCHEMA."""
        mock_client = MagicMock()
        mock_result = MagicMock()
        mock_result.result_rows = [[10]]  # total_rows
        mock_client.query.return_value = mock_result
        mock_ch.connect.return_value = mock_client

        with patch('orchestrator.tasks._profile_field') as mock_profile:
            mock_profile.return_value = {'min': 0, 'max': 100}
            result = calculate_profiling_stats('stg_credits', 'credits')

        self.assertIn('_meta', result)
        self.assertEqual(result['_meta']['total_rows'], 10)
        # SKIP fields should not call _profile_field
        calls = mock_profile.call_args_list
        # All non-SKIP fields from credits schema
        self.assertGreater(len(calls), 0)

    @patch('orchestrator.tasks.ch_client')
    def test_payments_schema_used_for_payments(self, mock_ch):
        """Payments table uses PAYMENTS_FIELD_SCHEMA."""
        mock_client = MagicMock()
        mock_result = MagicMock()
        mock_result.result_rows = [[5]]
        mock_client.query.return_value = mock_result
        mock_ch.connect.return_value = mock_client

        with patch('orchestrator.tasks._profile_field') as mock_profile:
            mock_profile.return_value = {}
            result = calculate_profiling_stats('stg_payments', 'payments')

        self.assertEqual(result['_meta']['table'], 'stg_payments')
        self.assertEqual(result['_meta']['total_rows'], 5)

    @patch('orchestrator.tasks.ch_client')
    def test_field_error_stored_in_stats(self, mock_ch):
        """When _profile_field raises, error is stored for that field."""
        mock_client = MagicMock()
        mock_result = MagicMock()
        mock_result.result_rows = [[10]]
        mock_client.query.return_value = mock_result
        mock_ch.connect.return_value = mock_client

        with patch('orchestrator.tasks._profile_field') as mock_profile:
            mock_profile.side_effect = Exception("SQL error")
            result = calculate_profiling_stats('stg_credits', 'credits')

        # First non-SKIP field will fail
        keys = [k for k in result.keys() if k != '_meta']
        self.assertGreater(len(keys), 0)
        self.assertIn('error', result[keys[0]])


@override_settings(
    DATABASES={
        'default': {
            'ENGINE': 'django.db.backends.sqlite3',
            'NAME': ':memory:',
        }
    },
)
class TestProfileField(SimpleTestCase):
    """Test _profile_field for each field type."""

    def setUp(self):
        """Create mock client."""
        self.client = MagicMock()

    def test_numeric_field(self):
        """NUMERIC field returns min, max, avg, stddev, null_ratio."""
        self.client.query.return_value.result_rows = [
            [10.5, 100.0, 50.25, 20.1, 2]
        ]
        result = _profile_field(
            self.client, 'tbl', 'original_loan_amount', FieldType.NUMERIC, 100
        )
        self.assertEqual(result['min'], 10.5)
        self.assertEqual(result['max'], 100.0)
        self.assertEqual(result['avg'], 50.25)
        self.assertEqual(result['stddev'], 20.1)
        self.assertEqual(result['null_ratio'], 0.02)
        self.assertEqual(result['null_count'], 2)

    def test_numeric_with_zero_total_rows(self):
        """NUMERIC with total_rows=0 avoids division by zero."""
        self.client.query.return_value.result_rows = [[None, None, None, None, 0]]
        result = _profile_field(
            self.client, 'tbl', 'amount', FieldType.NUMERIC, 0
        )
        self.assertEqual(result['null_ratio'], 0)

    def test_categorical_field(self):
        """CATEGORICAL field returns unique_count, most_frequent, etc."""
        self.client.query.return_value.result_rows = [
            [3, 1, 'A', 80]  # unique, null_count, most_freq, most_freq_count
        ]
        result = _profile_field(
            self.client, 'tbl', 'loan_status_code', FieldType.CATEGORICAL, 100
        )
        self.assertEqual(result['unique_count'], 3)
        self.assertEqual(result['most_frequent'], 'A')
        self.assertEqual(result['most_frequent_pct'], 0.8)
        self.assertEqual(result['null_ratio'], 0.01)
        self.assertEqual(result['null_count'], 1)

    def test_categorical_all_null_most_frequent(self):
        """CATEGORICAL with all nulls has most_frequent None."""
        self.client.query.return_value.result_rows = [
            [0, 100, None, 0]
        ]
        result = _profile_field(
            self.client, 'tbl', 'status', FieldType.CATEGORICAL, 100
        )
        self.assertIsNone(result['most_frequent'])
        self.assertEqual(result['most_frequent_pct'], 0)

    def test_date_field(self):
        """DATE field returns min, max, null_ratio."""
        self.client.query.return_value.result_rows = [
            ['2024-01-01', '2025-12-31', 5]
        ]
        result = _profile_field(
            self.client, 'tbl', 'final_maturity_date', FieldType.DATE, 100
        )
        self.assertEqual(result['min'], '2024-01-01')
        self.assertEqual(result['max'], '2025-12-31')
        self.assertEqual(result['null_ratio'], 0.05)
        self.assertEqual(result['null_count'], 5)

    def test_date_null_min_max(self):
        """DATE with null min/max returns None."""
        self.client.query.return_value.result_rows = [
            [None, None, 100]
        ]
        result = _profile_field(
            self.client, 'tbl', 'loan_start_date', FieldType.DATE, 100
        )
        self.assertIsNone(result['min'])
        self.assertIsNone(result['max'])

    def test_string_field(self):
        """STRING field returns unique_count, null_or_empty_ratio."""
        self.client.query.return_value.result_rows = [
            [50, 3]  # unique, null_or_empty_count
        ]
        result = _profile_field(
            self.client, 'tbl', 'loan_account_number', FieldType.STRING, 100
        )
        self.assertEqual(result['unique_count'], 50)
        self.assertEqual(result['null_or_empty_ratio'], 0.03)
        self.assertEqual(result['null_or_empty_count'], 3)

    def test_unknown_field_type_returns_none(self):
        """Unknown/unsupported field type returns None."""
        result = _profile_field(
            self.client, 'tbl', 'unknown', "UNKNOWN_TYPE", 100
        )
        self.assertIsNone(result)


@override_settings(
    DATABASES={
        'default': {
            'ENGINE': 'django.db.backends.sqlite3',
            'NAME': ':memory:',
        }
    },
)
class TestSafeFloat(SimpleTestCase):
    """Test _safe_float conversion."""

    def test_none_returns_none(self):
        """None returns None."""
        self.assertIsNone(_safe_float(None))

    def test_int_converted(self):
        """Int is converted to float."""
        self.assertEqual(_safe_float(42), 42.0)

    def test_float_rounded(self):
        """Float is rounded to 4 decimals."""
        self.assertEqual(_safe_float(3.14159265), 3.1416)

    def test_decimal_converted(self):
        """Decimal is converted to float."""
        self.assertEqual(_safe_float(Decimal("100.5")), 100.5)

    def test_string_number_converted(self):
        """Numeric string is converted."""
        self.assertEqual(_safe_float("123.45"), 123.45)

    def test_invalid_returns_none(self):
        """Invalid value returns None."""
        self.assertIsNone(_safe_float("not-a-number"))
        self.assertIsNone(_safe_float({}))
        self.assertIsNone(_safe_float([]))


@override_settings(
    DATABASES={
        'default': {
            'ENGINE': 'django.db.backends.sqlite3',
            'NAME': ':memory:',
        }
    },
    CELERY_TASK_ALWAYS_EAGER=True,
)
class TestTriggerSyncLogic(TestCase):
    """Test trigger_sync_logic decision logic."""

    def setUp(self):
        """Create tenant."""
        self.tenant = Tenant.objects.create(
            tenant_id='BANK001',
            name='Test Bank',
            slug='bank001',
            api_url='http://simulator:8000',
            api_token='token',
            is_active=True,
        )

    @patch('orchestrator.tasks.process_sync')
    @patch('orchestrator.tasks.get_remote_version')
    def test_triggers_when_no_previous_job(self, mock_version, mock_process):
        """Triggers sync when no successful job exists."""
        mock_version.side_effect = [42, 43]
        job_id = trigger_sync_logic(self.tenant, LoanCategory.COMMERCIAL, force=False)
        self.assertIsNotNone(job_id)
        job = SyncJob.objects.get(id=job_id)
        self.assertEqual(job.remote_version_credit, 42)
        self.assertEqual(job.remote_version_payment, 43)
        self.assertEqual(job.status, SyncJobStatus.PENDING.value)
        mock_process.delay.assert_called_once_with(job_id)

    @patch('orchestrator.tasks.process_sync')
    @patch('orchestrator.tasks.get_remote_version')
    def test_skips_when_api_unreachable(self, mock_version, mock_process):
        """Skips when get_remote_version returns None."""
        mock_version.return_value = None
        job_id = trigger_sync_logic(self.tenant, LoanCategory.COMMERCIAL)
        self.assertIsNone(job_id)
        mock_process.delay.assert_not_called()

    @patch('orchestrator.tasks.process_sync')
    @patch('orchestrator.tasks.get_remote_version')
    def test_triggers_when_version_changed(self, mock_version, mock_process):
        """Triggers when remote version differs from last job."""
        SyncJob.objects.create(
            tenant=self.tenant,
            loan_category=LoanCategory.COMMERCIAL.value,
            status=SyncJobStatus.SUCCESS.value,
            remote_version_credit=1,
            remote_version_payment=1,
            completed_at=timezone.now(),
        )
        mock_version.side_effect = [2, 2]  # New versions
        job_id = trigger_sync_logic(self.tenant, LoanCategory.COMMERCIAL)
        self.assertIsNotNone(job_id)
        mock_process.delay.assert_called_once()

    @patch('orchestrator.tasks.process_sync')
    @patch('orchestrator.tasks.get_remote_version')
    def test_skips_when_version_unchanged(self, mock_version, mock_process):
        """Skips when version matches last successful job."""
        SyncJob.objects.create(
            tenant=self.tenant,
            loan_category=LoanCategory.COMMERCIAL.value,
            status=SyncJobStatus.SUCCESS.value,
            remote_version_credit=42,
            remote_version_payment=43,
            completed_at=timezone.now(),
        )
        mock_version.side_effect = [42, 43]
        job_id = trigger_sync_logic(self.tenant, LoanCategory.COMMERCIAL)
        self.assertIsNone(job_id)
        mock_process.delay.assert_not_called()

    @patch('orchestrator.tasks.process_sync')
    @patch('orchestrator.tasks.get_remote_version')
    def test_force_skips_version_check(self, mock_version, mock_process):
        """Force=True triggers even when version unchanged."""
        SyncJob.objects.create(
            tenant=self.tenant,
            loan_category=LoanCategory.COMMERCIAL.value,
            status=SyncJobStatus.SUCCESS.value,
            remote_version_credit=42,
            remote_version_payment=43,
            completed_at=timezone.now(),
        )
        mock_version.side_effect = [42, 43]
        job_id = trigger_sync_logic(self.tenant, LoanCategory.COMMERCIAL, force=True)
        self.assertIsNotNone(job_id)
        mock_process.delay.assert_called_once()

    @patch('orchestrator.tasks.process_sync')
    @patch('orchestrator.tasks.get_remote_version')
    def test_skips_when_job_already_running(self, mock_version, mock_process):
        """Does not double-queue when job is PENDING or IN_PROGRESS."""
        SyncJob.objects.create(
            tenant=self.tenant,
            loan_category=LoanCategory.COMMERCIAL.value,
            status=SyncJobStatus.IN_PROGRESS.value,
            remote_version_credit=1,
            remote_version_payment=1,
        )
        mock_version.side_effect = [2, 2]
        job_id = trigger_sync_logic(self.tenant, LoanCategory.COMMERCIAL)
        self.assertIsNone(job_id)
        mock_process.delay.assert_not_called()

    @patch('orchestrator.tasks.process_sync')
    @patch('orchestrator.tasks.get_remote_version')
    def test_retail_category(self, mock_version, mock_process):
        """Correctly handles RETAIL category."""
        mock_version.side_effect = [10, 11]
        job_id = trigger_sync_logic(self.tenant, LoanCategory.RETAIL)
        self.assertIsNotNone(job_id)
        job = SyncJob.objects.get(id=job_id)
        self.assertEqual(job.loan_category, LoanCategory.RETAIL.value)


@override_settings(
    DATABASES={
        'default': {
            'ENGINE': 'django.db.backends.sqlite3',
            'NAME': ':memory:',
        }
    },
    CELERY_TASK_ALWAYS_EAGER=True,
)
class TestCheckForUpdates(TestCase):
    """Test check_for_updates periodic task."""

    @patch('orchestrator.tasks.trigger_sync_logic')
    def test_iterates_active_tenants_and_categories(self, mock_trigger):
        """Calls trigger_sync_logic for each active tenant and category."""
        Tenant.objects.create(
            tenant_id='BANK001',
            name='Bank 1',
            slug='bank1',
            api_url='http://x',
            is_active=True,
        )
        Tenant.objects.create(
            tenant_id='BANK002',
            name='Bank 2',
            slug='bank2',
            api_url='http://y',
            is_active=True,
        )
        check_for_updates()
        self.assertEqual(mock_trigger.call_count, 4)  # 2 tenants * 2 categories

    @patch('orchestrator.tasks.trigger_sync_logic')
    def test_skips_inactive_tenants(self, mock_trigger):
        """Inactive tenants are not processed."""
        Tenant.objects.create(
            tenant_id='BANK001',
            name='Bank 1',
            slug='bank1',
            api_url='http://x',
            is_active=False,
        )
        check_for_updates()
        mock_trigger.assert_not_called()


@override_settings(
    DATABASES={
        'default': {
            'ENGINE': 'django.db.backends.sqlite3',
            'NAME': ':memory:',
        }
    },
    CELERY_TASK_ALWAYS_EAGER=True,
)
class TestProcessSync(TestCase):
    """Test process_sync ETL task."""

    def setUp(self):
        """Create tenant and job."""
        self.tenant = Tenant.objects.create(
            tenant_id='BANK001',
            name='Test Bank',
            slug='bank001',
            api_url='http://simulator:8000',
            api_token='token',
            is_active=True,
        )
        self.job = SyncJob.objects.create(
            tenant=self.tenant,
            loan_category=LoanCategory.COMMERCIAL.value,
            remote_version_credit=1,
            remote_version_payment=1,
            status=SyncJobStatus.PENDING.value,
        )

    @patch('orchestrator.tasks.ch_client')
    @patch('orchestrator.tasks.stream_to_staging')
    @patch('orchestrator.tasks.run_validation_suite')
    @patch('orchestrator.tasks.calculate_profiling_stats')
    def test_success_path_updates_job_and_creates_report(
        self, mock_profiling, mock_validation, mock_stream, mock_ch
    ):
        """Successful ETL updates job status and creates SyncReport."""
        mock_ch.prepare_staging.side_effect = ['stg_credits', 'stg_payments']
        mock_stream.side_effect = [100, 200]
        mock_validation.return_value = ([], [])  # No critical, no warnings
        mock_profiling.side_effect = [{}, {}]  # credits result, then payments result
        mock_ch.connect.return_value.command.return_value = None

        process_sync(self.job.id)

        self.job.refresh_from_db()
        self.assertEqual(self.job.status, SyncJobStatus.SUCCESS.value)
        self.assertIsNotNone(self.job.completed_at)

        report = SyncReport.objects.get(job=self.job)
        self.assertEqual(report.total_rows_processed, 300)
        self.assertEqual(report.profiling_stats['credits'], {})
        self.assertEqual(report.profiling_stats['payments'], {})

        mock_ch.swap_partition.assert_called()
        mock_ch.connect.return_value.command.assert_any_call(
            "DROP TABLE IF EXISTS stg_credits"
        )

    @patch('orchestrator.tasks.ch_client')
    @patch('orchestrator.tasks.stream_to_staging')
    @patch('orchestrator.tasks.run_validation_suite')
    def test_validation_failure_sets_failed_and_creates_report(
        self, mock_validation, mock_stream, mock_ch
    ):
        """ValidationException sets FAILED and stores errors in report."""
        mock_ch.prepare_staging.side_effect = ['stg_credits', 'stg_payments']
        mock_stream.side_effect = [100, 200]
        mock_validation.return_value = (
            ["CRITICAL: 5 rows missing Loan Account Number. Sync Aborted."],
            []
        )
        mock_ch.connect.return_value.command.return_value = None

        process_sync(self.job.id)

        self.job.refresh_from_db()
        self.assertEqual(self.job.status, SyncJobStatus.FAILED.value)
        self.assertEqual(self.job.error_message, "Data Validation Failed")

        report = SyncReport.objects.get(job=self.job)
        self.assertIn("CRITICAL", str(report.validation_errors))

        mock_ch.swap_partition.assert_not_called()

    @patch('orchestrator.tasks.ch_client')
    @patch('orchestrator.tasks.stream_to_staging')
    def test_generic_exception_sets_failed(self, mock_stream, mock_ch):
        """Unexpected exception sets FAILED with error message."""
        mock_ch.prepare_staging.side_effect = Exception("ClickHouse connection failed")
        mock_ch.connect.return_value.command.return_value = None

        process_sync(self.job.id)

        self.job.refresh_from_db()
        self.assertEqual(self.job.status, SyncJobStatus.FAILED.value)
        self.assertIn("ClickHouse", self.job.error_message)

    @patch('orchestrator.tasks.ch_client')
    @patch('orchestrator.tasks.stream_to_staging')
    @patch('orchestrator.tasks.run_validation_suite')
    @patch('orchestrator.tasks.calculate_profiling_stats')
    def test_cleanup_drops_staging_tables_on_success(
        self, mock_profiling, mock_validation, mock_stream, mock_ch
    ):
        """Staging tables are dropped in finally block on success."""
        mock_ch.prepare_staging.side_effect = ['stg_credits', 'stg_payments']
        mock_stream.side_effect = [100, 200]
        mock_validation.return_value = ([], [])
        mock_profiling.side_effect = [{'credits': {}}, {'payments': {}}]
        mock_cmd = MagicMock()
        mock_ch.connect.return_value = mock_cmd

        process_sync(self.job.id)

        drop_calls = [c[0][0] for c in mock_cmd.command.call_args_list]
        self.assertIn("DROP TABLE IF EXISTS stg_credits", drop_calls)
        self.assertIn("DROP TABLE IF EXISTS stg_payments", drop_calls)

    @patch('orchestrator.tasks.ch_client')
    @patch('orchestrator.tasks.stream_to_staging')
    @patch('orchestrator.tasks.run_validation_suite')
    @patch('orchestrator.tasks.calculate_profiling_stats')
    def test_selective_load_reuses_partition_when_version_unchanged(
        self, mock_profiling, mock_validation, mock_stream, mock_ch
    ):
        """When version unchanged, copies from base table instead of downloading."""
        SyncJob.objects.create(
            tenant=self.tenant,
            loan_category=LoanCategory.COMMERCIAL.value,
            status=SyncJobStatus.SUCCESS.value,
            remote_version_credit=1,
            remote_version_payment=1,
            completed_at=timezone.now(),
        )
        mock_ch.prepare_staging.side_effect = ['stg_credits', 'stg_payments']

        def command_side_effect(sql):
            if 'SELECT count()' in sql and 'stg_credits' in sql:
                return 50
            if 'SELECT count()' in sql and 'stg_payments' in sql:
                return 75
            return None

        mock_ch.connect.return_value.command.side_effect = command_side_effect
        mock_validation.return_value = ([], [])
        mock_profiling.side_effect = [{'credits': {}}, {'payments': {}}]

        process_sync(self.job.id)

        # stream_to_staging should NOT be called (we reuse partition)
        mock_stream.assert_not_called()
        # INSERT INTO stg_* SELECT * FROM ... should be called
        cmd_calls = [str(c[0][0]) for c in mock_ch.connect.return_value.command.call_args_list]
        self.assertTrue(
            any('INSERT INTO' in c and 'credits_all' in c for c in cmd_calls)
        )

    @patch('orchestrator.tasks.ch_client')
    def test_nonexistent_job_raises(self, mock_ch):
        """ProcessSync with invalid job_id raises."""
        with self.assertRaises(SyncJob.DoesNotExist):
            process_sync(99999)

    @patch('orchestrator.tasks.ch_client')
    @patch('orchestrator.tasks.stream_to_staging')
    @patch('orchestrator.tasks.run_validation_suite')
    @patch('orchestrator.tasks.calculate_profiling_stats')
    def test_quality_warnings_stored_in_report(self, mock_profiling, mock_validation, mock_stream, mock_ch):
        """Quality warnings (orphans, negative balances) are stored in report, sync continues."""
        mock_ch.prepare_staging.side_effect = ['stg_credits', 'stg_payments']
        mock_stream.side_effect = [10, 20]
        mock_validation.return_value = (
            [],  # No critical
            ["WARNING: 3 payments are orphans.", "WARNING: 1 loans have negative balances."]
        )
        mock_profiling.side_effect = [{'credits': {}}, {'payments': {}}]
        mock_ch.connect.return_value.command.return_value = None

        process_sync(self.job.id)

        self.job.refresh_from_db()
        self.assertEqual(self.job.status, SyncJobStatus.SUCCESS.value)
        report = SyncReport.objects.get(job=self.job)
        self.assertEqual(len(report.validation_errors), 2)
        self.assertIn("orphan", report.validation_errors[0].lower())
        self.assertIn("negative", report.validation_errors[1].lower())
