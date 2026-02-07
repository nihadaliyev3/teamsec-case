"""
Configuration for scenario/E2E tests.
Services must be running (docker compose up).
Set SCENARIO_TEST_API_KEY from init_tenants output.
"""
import os

API_BASE = os.environ.get("SCENARIO_API_URL", "http://localhost:8002")
ADAPTER_BASE = os.environ.get("SCENARIO_ADAPTER_URL", "http://localhost:8000")
EXTERNAL_BANK_BASE = os.environ.get("SCENARIO_EXTERNAL_BANK_URL", "http://localhost:8001")

# API key from a tenant (run init_tenants and paste the printed key)
API_KEY = os.environ.get("SCENARIO_TEST_API_KEY", "")

# Optional: BANK002 key for tenant isolation test (init_tenants prints keys for all tenants)
API_KEY_BANK002 = os.environ.get("SCENARIO_TEST_API_KEY_BANK002", "")

# Valid tenants for external bank
VALID_TENANTS = ["BANK001", "BANK002", "BANK003"]
VALID_LOAN_TYPES = ["COMMERCIAL", "RETAIL"]
VALID_FILE_TYPES = ["commercial_credit", "commercial_payment", "retail_credit", "retail_payment"]
