# Scenario Tests (E2E)

TeamSec sistemi için uçtan uca (end-to-end) senaryo testleri. Bu testler; API Gateway, Adapter ve Harici Banka Simülatörü servisleri canlıyken çalıştırılmak üzere tasarlanmıştır.

## Ön Gereksinimler

1. **Tüm servisleri başlatın:**
   ```bash
   docker compose up -d
   ```

2. **Veritabanı migration'larını çalıştırın ve Tenant'ları oluşturun:**
   ```bash
   docker compose exec adapter python manage.py migrate
   docker compose exec adapter python manage.py init_tenants
   ```
   Not: init_tenants komutu tarafından ekrana basılan 2 farklı Tenant (örn: BANK001, BANK002) için API anahtarlarını kaydedin.

3. **API Anahtarlarını ortam değişkeni (Environment Variable) olarak tanımlayın:**
   ```bash
   export SCENARIO_TEST_API_KEY="<paste-the-64-char-hex-key-here>"
   export SCENARIO_TEST_API_KEY_BANK002="paste-the-64-char-hex-key-here>"
   ```

## Testleri Çalıştırma

Proje ana dizinindeyken aşağıdaki adımları izleyin:

```bash
# 1. Bağımlılıkları yükleyin (Sanal ortam/venv veya global olarak)
pip install -r scenario_tests/requirements.txt

# 2. Tüm senaryo testlerini çalıştırın
pytest scenario_tests/ -v

# Veya python -m pytest ile (eğer pytest PATH üzerinde tanımlı değilse)
python -m pytest scenario_tests/ -v

# 3. Sadece belirli bir dosyayı çalıştırın
pytest scenario_tests/test_api_gateway.py -v

# 4. Sadece belirli bir sınıfı (class) çalıştırın
pytest scenario_tests/test_api_gateway.py::TestHealthCheck -v

# 5. Özel URL'ler ve API anahtarı belirterek çalıştırın
export SCENARIO_TEST_API_KEY="your-64-char-hex-key"
export SCENARIO_API_URL="http://localhost:8002"
export SCENARIO_ADAPTER_URL="http://localhost:8000"
export SCENARIO_EXTERNAL_BANK_URL="http://localhost:8001"
pytest scenario_tests/ -v
```

## Test Kapsamı (Coverage)

| File | Focus |
|------|--------|
| `test_api_gateway.py` | FastAPI: health, sync, data, profiling; auth; validation |
| `test_adapter.py` | Adapter: direct sync trigger; auth; validation |
| `test_external_bank.py` | External Bank: data GET/HEAD; param validation |
| `test_integration.py` | API→Adapter proxy; tenant isolation; loan type consistency |
| `test_edge_cases.py` | Empty body, extra fields, limits, malformed auth |
| `test_data_integrity.py` | Data integrity: tenant isolation, replace-not-append, failure keeps old data |


## Atlanan (Skipped) Testler
- Eğer SCENARIO_TEST_API_KEY tanımlanmamışsa, kimlik doğrulama gerektiren testler başarısız olmaz, otomatik olarak atlanır.

- Tenant izolasyon testi (test_bank001_data_has_no_b2_prefix), SCENARIO_TEST_API_KEY_BANK002 anahtarının tanımlı olmasını gerektirir. Bu testi koşturmak için init_tenants çıktısındaki ikinci anahtarı mutlaka tanımlayın.