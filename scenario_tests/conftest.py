"""
Pytest fixtures for scenario tests.
"""
import sys
from pathlib import Path

import pytest

# Ensure scenario_tests is on path when running from project root
sys.path.insert(0, str(Path(__file__).resolve().parent))

import scenario_config as config


@pytest.fixture(scope="session")
def api_key():
    """API key for authenticated requests. Must be set via SCENARIO_TEST_API_KEY."""
    key = config.API_KEY
    if not key:
        pytest.skip(
            "SCENARIO_TEST_API_KEY not set. Run: docker compose exec adapter python manage.py init_tenants "
            "and set the env var with the printed key."
        )
    return key


@pytest.fixture
def api_headers(api_key):
    return {"X-API-Key": api_key, "Content-Type": "application/json"}


@pytest.fixture
def api_base():
    return config.API_BASE.rstrip("/")


@pytest.fixture
def adapter_base():
    return config.ADAPTER_BASE.rstrip("/")


@pytest.fixture
def external_bank_base():
    return config.EXTERNAL_BANK_BASE.rstrip("/")
