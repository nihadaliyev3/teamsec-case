"""
Scenario tests for the Adapter (Django, port 8000).
"""
import requests


class TestAdapterSync:
    """POST /api/sync/ - direct adapter sync trigger."""

    def test_sync_without_api_key_rejected(self, adapter_base):
        """Adapter returns 401 or 403 for missing key (DRF may use 403 for unauthenticated)."""
        r = requests.post(
            f"{adapter_base}/api/sync/",
            json={"loan_category": "COMMERCIAL", "force": True},
            timeout=10,
        )
        assert r.status_code in (401, 403)

    def test_sync_with_invalid_api_key_rejected(self, adapter_base):
        """Adapter returns 401 or 403 for invalid key."""
        r = requests.post(
            f"{adapter_base}/api/sync/",
            headers={"X-API-Key": "invalid-key"},
            json={"loan_category": "COMMERCIAL", "force": True},
            timeout=10,
        )
        assert r.status_code in (401, 403)

    def test_sync_invalid_loan_category_returns_400(self, adapter_base, api_headers):
        r = requests.post(
            f"{adapter_base}/api/sync/",
            headers=api_headers,
            json={"loan_category": "INVALID", "force": True},
            timeout=10,
        )
        assert r.status_code == 400

    def test_sync_valid_returns_202_or_409(self, adapter_base, api_headers):
        r = requests.post(
            f"{adapter_base}/api/sync/",
            headers=api_headers,
            json={"loan_category": "COMMERCIAL", "force": True},
            timeout=10,
        )
        assert r.status_code in (202, 409)
        if r.status_code == 202:
            assert "job_id" in r.json()
