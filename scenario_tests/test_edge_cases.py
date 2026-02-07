"""
Edge-case scenario tests.
"""
import requests


class TestEdgeCasesAPI:
    """Edge cases for API Gateway."""

    def test_empty_body_sync_returns_422(self, api_base, api_headers):
        r = requests.post(
            f"{api_base}/api/sync",
            headers=api_headers,
            json={},
            timeout=5,
        )
        assert r.status_code == 422

    def test_sync_extra_fields_ignored(self, api_base, api_headers):
        """Extra fields in body should not break the request."""
        r = requests.post(
            f"{api_base}/api/sync",
            headers=api_headers,
            json={
                "loan_type": "COMMERCIAL",
                "force": True,
                "extra_field": "ignored",
            },
            timeout=10,
        )
        assert r.status_code in (202, 409)

    def test_sync_force_false_accepted(self, api_base, api_headers):
        r = requests.post(
            f"{api_base}/api/sync",
            headers=api_headers,
            json={"loan_type": "COMMERCIAL", "force": False},
            timeout=10,
        )
        assert r.status_code in (202, 409)

    def test_data_limit_parameter(self, api_base, api_headers):
        r = requests.get(
            f"{api_base}/api/data?loan_type=COMMERCIAL&limit=1",
            headers=api_headers,
            timeout=5,
        )
        assert r.status_code == 200
        assert len(r.json()) <= 1

    def test_profiling_empty_response_ok(self, api_base, api_headers):
        """No reports yet -> empty list, not error."""
        r = requests.get(
            f"{api_base}/api/profiling?loan_type=RETAIL",
            headers=api_headers,
            timeout=5,
        )
        assert r.status_code == 200
        assert r.json() == [] or isinstance(r.json(), list)


class TestEdgeCasesAuth:
    """Auth edge cases."""

    def test_malformed_api_key_header(self, api_base):
        """Empty or malformed key should return 401."""
        r = requests.post(
            f"{api_base}/api/sync",
            headers={"X-API-Key": ""},
            json={"loan_type": "COMMERCIAL", "force": True},
            timeout=5,
        )
        assert r.status_code == 401

    def test_wrong_header_name_returns_401(self, api_base, api_headers):
        """Using Authorization instead of X-API-Key should fail."""
        r = requests.post(
            f"{api_base}/api/sync",
            headers={"Authorization": f"Bearer {api_headers.get('X-API-Key')}"},
            json={"loan_type": "COMMERCIAL", "force": True},
            timeout=5,
        )
        assert r.status_code == 401


class TestEdgeCasesExternalBank:
    """Edge cases for External Bank."""

    def test_invalid_file_type_returns_404(self, external_bank_base):
        r = requests.get(
            f"{external_bank_base}/api/data/",
            params={"file_type": "nonexistent", "tenant": "BANK001"},
            timeout=5,
        )
        assert r.status_code in (400, 404)
