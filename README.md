# 🛡️ TeamSec Fintech Case Study
### Scalable ETL & Analytics Platform

Bu proje, yüksek hacimli finansal verilerin (kredi ve ödeme işlemleri) harici bir bankadan (**External Bank Simulator**) güvenli bir şekilde alınıp, doğrulanarak (**Validation**) merkezi bir veri ambarına (**ClickHouse**) aktarılmasını sağlayan ölçeklenebilir bir ETL ve Analitik platformudur.

Proje **Microservices** mimarisi ile tasarlanmış olup; veri bütünlüğü, tenant izolasyonu ve yüksek performans (**Big Data handling**) odaklı geliştirilmiştir.

---

## 🚀 Hızlı Kurulum (Quick Start)

Tüm sistem Docker ve Docker Compose ile konteynerize edilmiştir. 

### 1. Projeyi Klonlayın ve Başlatın
```bash
git clone https://github.com/nihadaliyev3/teamsec-case.git
cd teamsec-case
`.env.example` dosyasini `.env`e kopyalayin.

Bash
# Tüm servisleri build edin ve ayağa kaldırın
docker-compose up -d --build
2. Tenant (Müşteri) Oluşturma ve API Key Üretimi
Sistemin çalışabilmesi için en az bir Tenant (Banka Müşterisi) tanımlanmalıdır.

Bash
# Adapter servisi içinde tenant oluşturma scriptini çalıştırın
docker-compose exec adapter python manage.py init_tenants
[!IMPORTANT]
ÖNEMLİ: Bu komut size 3 API Key (örneğin: sk_a1b2...) verecektir. Bu anahtarları kaydedin; tüm API isteklerinde kimlik doğrulama için kullanılacaktır.

3. Servislerin Durumu

Kurulum tamamlandığında aşağıdaki servisler aktif olacaktır:

Servis	URL	Açıklama
Bank Simulator UI	http://localhost:5173	Harici bankaya veri yükleme paneli
Main Dashboard UI	http://localhost:5174	ETL tetikleme ve Analitik paneli
API Gateway	http://localhost:8002	Ana REST API (FastAPI)
Adapter (Worker)	http://localhost:8000	ETL Motoru (Django)
ClickHouse	localhost:8123	OLAP Veritabanı
🏛️ Multi-Tenancy Tasarım Kararı
Bu projede Logical Separation (Mantıksal İzolasyon) yöntemi tercih edilmiştir.

Yöntem: Shared Database, Shared Schema.

Uygulama: Her veritabanı tablosunda (Postgres & ClickHouse) zorunlu bir tenant_id kolonu bulunur.

Neden Bu Yöntem Seçildi?

Operasyonel Maliyet: Her müşteri için ayrı DB/Schema açmak (Silo yaklaşımı), binlerce müşteri olduğunda yönetim ve migration karmaşıklığını (Complexity) artırır.

Analitik Performans: ClickHouse gibi OLAP veritabanlarında tek bir büyük tabloda PARTITION BY (tenant_id, loan_type) yapmak, ayrı tablolardan çok daha yüksek performans sağlar.

Güvenlik: İzolasyon, API Gateway seviyesinde sağlanır. Kullanıcı Request Body'sinde tenant_id gönderemez. Sistem, X-API-Key header'ından tenant'ı çözümleyip (Resolve), backend'e kendisi enjekte eder. Bu, IDOR (Insecure Direct Object Reference) açıklarını %100 engeller.

🔌 API Kullanım Örnekleri
Tüm isteklerde X-API-Key header'ı zorunludur.

1. Senkronizasyon Tetikleme (Trigger Sync)

Bash
curl -X POST http://localhost:8002/api/sync \
  -H "X-API-Key: <YOUR_API_KEY>" \
  -H "Content-Type: application/json" \
  -d '{"loan_type": "COMMERCIAL", "force": true}'

2. Finansal Veri Çekme (Data Retrieval)

Bash
curl "http://localhost:8002/api/data?loan_type=COMMERCIAL&limit=10" \
  -H "X-API-Key: <YOUR_API_KEY>"
3. Profiling & Validasyon Raporu

Bash
curl "http://localhost:8002/api/profiling?loan_type=COMMERCIAL" \
  -H "X-API-Key: <YOUR_API_KEY>"
  
  
🧪 Testlerin Çalıştırılması

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

Bu proje TeamSec Case Study kapsamında geliştirilmiştir.