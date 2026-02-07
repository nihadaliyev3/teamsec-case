"""
Data integrity scenario tests.

1. Tenant isolation: BANK001 data must not contain BANK002/BANK003 data (no B2-/B3- prefixed IDs)
2. Replace not append: Load 1000, then 2000 -> warehouse has 2000 rows, not 3000
3. On failure: Old data is kept (validation failure does not replace partition)
"""
import io
import time

import pytest
import requests

import scenario_config as config


def _generate_credit_csv(n_rows: int) -> bytes:
    """Generate commercial_credit CSV with N rows. Semicolon-separated."""
    lines = ["loan_account_number;outstanding_principal_balance;customer_id"]
    for i in range(1, n_rows + 1):
        lines.append(f"LOAN_{i:06d};{1000 + i};cust_{i}")
    return "\n".join(lines).encode("utf-8")


def _generate_payment_csv(n_rows: int) -> bytes:
    """Generate commercial_payment CSV with N rows. Must match credit loan_account_numbers."""
    lines = ["loan_account_number;installment_number;installment_amount"]
    for i in range(1, n_rows + 1):
        lines.append(f"LOAN_{i:06d};1;100")
    return "\n".join(lines).encode("utf-8")


def _generate_credit_csv_with_invalid_row(n_rows: int, invalid_at: int = 1) -> bytes:
    """Same as above but one row has empty loan_account_number -> triggers ghost_loans validation.
    Must use quoted empty string "" so pandas/JSON produce '' (not null). The normalizer turns
    null into str(None)='None', which doesn't match ghost_loans WHERE loan_account_number = ''.
    """
    lines = ["loan_account_number;outstanding_principal_balance;customer_id"]
    for i in range(1, n_rows + 1):
        if i == invalid_at:
            lines.append(f'"";{1000 + i};cust_{i}')  # quoted empty -> '' in JSON -> ghost_loans
        else:
            lines.append(f"LOAN_{i:06d};{1000 + i};cust_{i}")
    return "\n".join(lines).encode("utf-8")


def _upload_file(base_url: str, file_type: str, content: bytes, filename: str) -> None:
    """Upload CSV to external bank /api/upload/."""
    r = requests.post(
        f"{base_url.rstrip('/')}/api/upload/",
        files={"file": (filename, io.BytesIO(content), "text/csv")},
        data={"file_type": file_type},
        timeout=10,
    )
    assert r.status_code == 200, f"Upload failed: {r.status_code} {r.text}"


def _sync(base_url: str, headers: dict, loan_type: str = "COMMERCIAL") -> int:
    """Trigger sync. Returns status code (202 or 409)."""
    r = requests.post(
        f"{base_url.rstrip('/')}/api/sync",
        headers=headers,
        json={"loan_type": loan_type, "force": True},
        timeout=15,
    )
    return r.status_code


def _wait_for_data(
    base_url: str,
    headers: dict,
    expected_count: int,
    loan_type: str = "COMMERCIAL",
    timeout_sec: int = 60,
    poll_interval: float = 2.0,
) -> int:
    """Poll /api/data until count matches expected or timeout. Returns actual count."""
    limit = max(expected_count + 100, 5000)
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        r = requests.get(
            f"{base_url.rstrip('/')}/api/data",
            params={"loan_type": loan_type, "limit": limit},
            headers=headers,
            timeout=5,
        )
        if r.status_code != 200:
            time.sleep(poll_interval)
            continue
        data = r.json()
        count = len(data)
        if count == expected_count:
            return count
        time.sleep(poll_interval)
    r = requests.get(
        f"{base_url.rstrip('/')}/api/data",
        params={"loan_type": loan_type, "limit": limit},
        headers=headers,
        timeout=5,
    )
    return len(r.json()) if r.status_code == 200 else 0


class TestTenantIsolation:
    """BANK001 verisi çekerken BANK002 verisi GELMEMELI."""

    @pytest.fixture
    def bank002_key(self):
        """BANK002 API key. Required for tenant isolation; skip if not set."""
        return config.API_KEY_BANK002

    @pytest.fixture
    def bank002_headers(self, bank002_key):
        return {"X-API-Key": bank002_key, "Content-Type": "application/json"}

    def test_bank001_data_has_no_b2_prefix(
        self, api_base, api_headers, bank002_key, bank002_headers, external_bank_base
    ):
        if not bank002_key:
            pytest.skip(
                "SCENARIO_TEST_API_KEY_BANK002 not set. Set both BANK001 and BANK002 keys for tenant isolation test."
            )
        """
        When fetching BANK001 data, no loan_account_number should start with B2- or B3-.
        BANK002/BANK003 apply those prefixes; BANK001 returns raw IDs.
        """
        # Upload shared CSV
        credit_csv = _generate_credit_csv(50)
        payment_csv = _generate_payment_csv(50)
        _upload_file(external_bank_base, "commercial_credit", credit_csv, "commercial_credit.csv")
        _upload_file(external_bank_base, "commercial_payment", payment_csv, "commercial_payment.csv")

        # Sync both tenants so warehouse has both BANK001 and BANK002 data
        _sync(api_base, api_headers)
        _sync(api_base, bank002_headers)
        time.sleep(3)  # Allow Celery to process

        # Wait for at least some BANK001 data
        _wait_for_data(api_base, api_headers, expected_count=1, timeout_sec=45)

        # Fetch BANK001 data and assert no B2-/B3- prefixed IDs
        r = requests.get(
            f"{api_base}/api/data",
            params={"loan_type": "COMMERCIAL", "limit": 500},
            headers=api_headers,
            timeout=5,
        )
        assert r.status_code == 200
        data = r.json()
        for row in data:
            acc = row.get("loan_account_number") or ""
            assert not acc.startswith("B2-"), (
                f"Tenant isolation violation: BANK001 data must not contain B2- prefixed IDs, got {acc}"
            )
            assert not acc.startswith("B3-"), (
                f"Tenant isolation violation: BANK001 data must not contain B3- prefixed IDs, got {acc}"
            )


class TestWarehouseReplaceNotAppend:
    """1000 kredi yükle, sonra 2000 yükle -> Warehouse'da 2000 olmalı (3000 değil)."""

    def test_replace_not_append(
        self, api_base, api_headers, external_bank_base
    ):
        """
        Load 1000 credits, sync. Then load 2000 credits, sync.
        Warehouse must have 2000 rows, not 3000.
        """
        # Step 1: Upload 1000 rows
        _upload_file(
            external_bank_base,
            "commercial_credit",
            _generate_credit_csv(1000),
            "commercial_credit.csv",
        )
        _upload_file(
            external_bank_base,
            "commercial_payment",
            _generate_payment_csv(1000),
            "commercial_payment.csv",
        )
        _sync(api_base, api_headers)
        count_1 = _wait_for_data(api_base, api_headers, expected_count=1000, timeout_sec=60)
        assert count_1 == 1000, f"Expected 1000 after first sync, got {count_1}"

        # Step 2: Upload 2000 rows (replaces)
        _upload_file(
            external_bank_base,
            "commercial_credit",
            _generate_credit_csv(2000),
            "commercial_credit.csv",
        )
        _upload_file(
            external_bank_base,
            "commercial_payment",
            _generate_payment_csv(2000),
            "commercial_payment.csv",
        )
        _sync(api_base, api_headers)
        count_2 = _wait_for_data(api_base, api_headers, expected_count=2000, timeout_sec=90)
        assert count_2 == 2000, (
            f"Warehouse must have 2000 rows after second load (replace, not append), got {count_2}"
        )


class TestFailureKeepsOldData:
    """On failure, the old data is kept."""

    def test_validation_failure_keeps_previous_data(
        self, api_base, api_headers, external_bank_base
    ):
        """
        Load 1000 valid rows, sync. Then upload invalid data (row with empty loan_account_number).
        Sync will fail validation. Warehouse must still have 1000 rows.
        """
        # Step 1: Upload 1000 valid rows
        _upload_file(
            external_bank_base,
            "commercial_credit",
            _generate_credit_csv(1000),
            "commercial_credit.csv",
        )
        _upload_file(
            external_bank_base,
            "commercial_payment",
            _generate_payment_csv(1000),
            "commercial_payment.csv",
        )
        _sync(api_base, api_headers)
        count_1 = _wait_for_data(api_base, api_headers, expected_count=1000, timeout_sec=60)
        assert count_1 == 1000, f"Expected 1000 after first sync, got {count_1}"

        # Step 2: Upload invalid data (one row with empty loan_account_number -> ghost_loans)
        _upload_file(
            external_bank_base,
            "commercial_credit",
            _generate_credit_csv_with_invalid_row(2000),
            "commercial_credit.csv",
        )
        _upload_file(
            external_bank_base,
            "commercial_payment",
            _generate_payment_csv(2000),
            "commercial_payment.csv",
        )
        _sync(api_base, api_headers)
        time.sleep(15)  # Allow Celery to run and fail

        # Warehouse must still have 1000 (old data kept)
        r = requests.get(
            f"{api_base}/api/data",
            params={"loan_type": "COMMERCIAL", "limit": 5000},
            headers=api_headers,
            timeout=5,
        )
        assert r.status_code == 200
        count_2 = len(r.json())
        assert count_2 == 1000, (
            f"On validation failure, old data must be kept. Expected 1000, got {count_2}"
        )
