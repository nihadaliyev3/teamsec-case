# Scenario Tests (E2E)

End-to-end scenario tests for the TeamSec system. Run against live services (API, Adapter, External Bank).

## Prerequisites

1. **Start all services**
   ```bash
   docker compose up -d
   ```

2. **Run migrations and init tenants**
   ```bash
   docker compose exec adapter python manage.py migrate
   docker compose exec adapter python manage.py init_tenants
   ```
   Save the printed API key for 2 tenants (e.g. BANK001, BANK002).

3. **Set the API key**
   ```bash
   export SCENARIO_TEST_API_KEY="<paste-the-64-char-hex-key-here>"
   export SCENARIO_TEST_API_KEY_BANK002="paste-the-64-char-hex-key-here>"
   ```

## Run Tests

From the project root:

```bash
# 1. Install dependencies (in your venv or global)
pip install -r scenario_tests/requirements.txt

# 2. Run all scenario tests
pytest scenario_tests/ -v

# Or with python -m pytest (if pytest not on PATH)
python -m pytest scenario_tests/ -v

# 3. Run a specific file
pytest scenario_tests/test_api_gateway.py -v

# 4. Run a specific class
pytest scenario_tests/test_api_gateway.py::TestHealthCheck -v

# 5. Run with API key and custom URLs
export SCENARIO_TEST_API_KEY="your-64-char-hex-key"
export SCENARIO_API_URL="http://localhost:8002"
export SCENARIO_ADAPTER_URL="http://localhost:8000"
export SCENARIO_EXTERNAL_BANK_URL="http://localhost:8001"
pytest scenario_tests/ -v
```

## Test Coverage

| File | Focus |
|------|--------|
| `test_api_gateway.py` | FastAPI: health, sync, data, profiling; auth; validation |
| `test_adapter.py` | Adapter: direct sync trigger; auth; validation |
| `test_external_bank.py` | External Bank: data GET/HEAD; param validation |
| `test_integration.py` | API→Adapter proxy; tenant isolation; loan type consistency |
| `test_edge_cases.py` | Empty body, extra fields, limits, malformed auth |
| `test_data_integrity.py` | Data integrity: tenant isolation, replace-not-append, failure keeps old data |

## Skipped Tests

- If `SCENARIO_TEST_API_KEY` is not set, tests that need authentication will be skipped (not fail).
- Tenant isolation test (`test_bank001_data_has_no_b2_prefix`) requires `SCENARIO_TEST_API_KEY_BANK002`. Set it to the BANK002 key printed by `init_tenants` to run this test.
