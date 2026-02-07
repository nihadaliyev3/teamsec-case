"""
Scenario tests for the FastAPI Gateway (port 8002).
"""
import pytest
import requests


class TestHealthCheck:
    """Health endpoint - no auth required."""

    def test_health_returns_200(self, api_base):
        r = requests.get(f"{api_base}/health", timeout=5)
        assert r.status_code == 200
        data = r.json()
        assert data.get("status") == "ok"
        assert "api-gateway" in data.get("service", "")

    def test_health_no_auth_required(self, api_base):
        """Health should work without X-API-Key."""
        r = requests.get(f"{api_base}/health", timeout=5)
        assert r.status_code == 200


class TestSyncTrigger:
    """POST /api/sync - triggers sync via adapter."""

    def test_sync_without_api_key_returns_401(self, api_base):
        r = requests.post(
            f"{api_base}/api/sync",
            json={"loan_type": "COMMERCIAL", "force": True},
            timeout=10,
        )
        assert r.status_code == 401
        assert "API" in r.json().get("detail", "")

    def test_sync_with_invalid_api_key_returns_401(self, api_base):
        r = requests.post(
            f"{api_base}/api/sync",
            headers={"X-API-Key": "invalid-key-12345"},
            json={"loan_type": "COMMERCIAL", "force": True},
            timeout=10,
        )
        assert r.status_code == 401

    def test_sync_with_malformed_json_returns_422(self, api_base, api_headers):
        r = requests.post(
            f"{api_base}/api/sync",
            headers=api_headers,
            json={"loan_type": "INVALID_CATEGORY", "force": True},
            timeout=10,
        )
        assert r.status_code == 422

    def test_sync_missing_loan_type_returns_422(self, api_base, api_headers):
        r = requests.post(
            f"{api_base}/api/sync",
            headers=api_headers,
            json={"force": True},
            timeout=10,
        )
        assert r.status_code == 422

    def test_sync_valid_request_returns_202_or_409(self, api_base, api_headers):
        """202 = job started; 409 = external bank unreachable or job already running."""
        r = requests.post(
            f"{api_base}/api/sync",
            headers=api_headers,
            json={"loan_type": "COMMERCIAL", "force": True},
            timeout=10,
        )
        assert r.status_code in (202, 409)
        if r.status_code == 202:
            data = r.json()
            assert "task_id" in data
            assert data.get("status") == "queued"


class TestDataEndpoint:
    """GET /api/data - loan data from ClickHouse."""

    def test_data_without_api_key_returns_401(self, api_base):
        r = requests.get(
            f"{api_base}/api/data?loan_type=COMMERCIAL",
            timeout=5,
        )
        assert r.status_code == 401

    def test_data_without_loan_type_returns_422(self, api_base, api_headers):
        r = requests.get(
            f"{api_base}/api/data",
            headers=api_headers,
            timeout=5,
        )
        assert r.status_code == 422

    def test_data_invalid_loan_type_returns_422(self, api_base, api_headers):
        r = requests.get(
            f"{api_base}/api/data?loan_type=INVALID",
            headers=api_headers,
            timeout=5,
        )
        assert r.status_code == 422

    def test_data_valid_returns_list(self, api_base, api_headers):
        r = requests.get(
            f"{api_base}/api/data?loan_type=COMMERCIAL&limit=5",
            headers=api_headers,
            timeout=5,
        )
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data, list)
        for item in data:
            assert "loan_account_number" in item


class TestProfilingEndpoint:
    """GET /api/profiling - sync reports from Postgres."""

    def test_profiling_without_api_key_returns_401(self, api_base):
        r = requests.get(
            f"{api_base}/api/profiling?loan_type=COMMERCIAL",
            timeout=5,
        )
        assert r.status_code == 401

    def test_profiling_without_loan_type_returns_422(self, api_base, api_headers):
        r = requests.get(
            f"{api_base}/api/profiling",
            headers=api_headers,
            timeout=5,
        )
        assert r.status_code == 422

    def test_profiling_valid_returns_list(self, api_base, api_headers):
        r = requests.get(
            f"{api_base}/api/profiling?loan_type=COMMERCIAL",
            headers=api_headers,
            timeout=5,
        )
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data, list)
