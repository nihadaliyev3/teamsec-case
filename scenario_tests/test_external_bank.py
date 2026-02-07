"""
Scenario tests for the External Bank (Django, port 8001).
"""
import requests


class TestExternalBankData:
    """GET /api/data/ - data download endpoint."""

    def test_data_missing_file_type_returns_400(self, external_bank_base):
        r = requests.get(
            f"{external_bank_base}/api/data/",
            params={"tenant": "BANK001"},
            timeout=5,
        )
        assert r.status_code == 400

    def test_data_missing_tenant_returns_400(self, external_bank_base):
        r = requests.get(
            f"{external_bank_base}/api/data/",
            params={"file_type": "commercial_credit"},
            timeout=5,
        )
        assert r.status_code == 400

    def test_data_invalid_tenant_returns_400(self, external_bank_base):
        r = requests.get(
            f"{external_bank_base}/api/data/",
            params={"file_type": "commercial_credit", "tenant": "INVALID"},
            timeout=5,
        )
        assert r.status_code == 400

    def test_data_valid_params_returns_200_or_404(self, external_bank_base):
        """404 if no file uploaded yet."""
        r = requests.get(
            f"{external_bank_base}/api/data/",
            params={"file_type": "commercial_credit", "tenant": "BANK001"},
            timeout=10,
        )
        assert r.status_code in (200, 404)


class TestExternalBankHead:
    """HEAD /api/data/ - version check."""

    def test_head_missing_file_type_returns_400(self, external_bank_base):
        r = requests.head(
            f"{external_bank_base}/api/data/",
            params={"tenant": "BANK001"},
            timeout=5,
        )
        assert r.status_code == 400

    def test_head_valid_returns_200_or_404(self, external_bank_base):
        r = requests.head(
            f"{external_bank_base}/api/data/",
            params={"file_type": "commercial_credit", "tenant": "BANK001"},
            timeout=5,
        )
        assert r.status_code in (200, 404)
        if r.status_code == 200:
            assert "X-Data-Version" in r.headers
