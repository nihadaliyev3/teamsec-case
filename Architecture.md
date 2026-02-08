Bu dosya, teknik detayları ve "Neden?" sorularının cevaplarını içerir. Mimari kararlarınızı savunduğunuz yer burasıdır.

```markdown
# System Architecture & Design Decisions

Bu döküman, TeamSec Fintech Case çalışmasının teknik mimarisini, veri akışını ve teknoloji seçimlerinin arkasındaki nedenleri açıklar.

## 1. High-Level Architecture (Yüksek Seviye Mimari)

Sistem **Event-Driven** ve **Microservices** prensiplerine uygun olarak 4 ana bileşene ayrılmıştır:



### Bileşenler:
1.  **External Bank (Source System):**
    * Gerçek dünyadaki bir bankayı simüle eder.
    * Rastgele veri bozulmaları (Data Corruption) ve günlük değişimleri simüle eden "Chaos Engineering" özellikleri barındırır.
    * Stack: Django REST Framework.

2.  **API Gateway (The Entry Point):**
    * Tüm dış trafiği karşılar.
    * **Sorumlulukları:** Authentication (API Key Validation), Rate Limiting, Request Routing ve Tenant Context Injection.
    * **Stack:** FastAPI (Yüksek performanslı Asenkron G/Ç için).

3.  **Adapter Service (The ETL Engine):**
    * İş mantığının (Business Logic) kalbidir.
    * Banka API'sinden veriyi **Stream** ederek çeker (RAM optimizasyonu).
    * Pandas ile validasyon ve transformasyon yapar.
    * **Stack:** Django, Celery (Async Workers), Redis.

4.  **Storage Layer:**
    * **PostgreSQL:** Operasyonel veri (Tenant yönetimi, Sync Job durumları, Hata raporları).
    * **ClickHouse:** Analitik veri (Milyonlarca satırlık kredi verisi). OLAP (Online Analytical Processing) için optimize edilmiştir.
    * **Redis:** Celery Task Queue ve Caching katmanı.

---

## 2. Data Flow (Veri Akışı)

ETL süreci şu adımlardan oluşur:

1.  **Trigger:** Kullanıcı (veya Scheduler) API Gateway'e `/sync` isteği atar.
2.  **Auth & Route:** Gateway, API Key'i doğrular, Tenant ID'yi bulur ve görevi Redis kuyruğuna (Celery) iletir. Gateway hemen `202 Accepted` döner (Non-blocking).
3.  **Extraction (Stream):** Adapter Worker, kuyruktan görevi alır. Harici bankaya bağlanır ve CSV dosyasını `chunk` (parça parça) halinde indirir.
    * *Design Pattern:* Memory-safe processing. 1GB'lık dosya işlenirken RAM kullanımı 100MB'ı geçmez.
4.  **Validation:** Her chunk, tanımlı kurallara (örn: `balance >= 0`) göre taranır. Hatalı satırlar ayrıştırılır.
5.  **Load (Batch Insert):** Geçerli veriler **ClickHouse**'a, validasyon hata raporları **PostgreSQL**'e yazılır.

---

## 3. Key Technical Decisions (Teknik Kararlar)

### A. Neden ClickHouse?
Finansal verilerde (Krediler, Ödemeler) sorgu performansı kritiktir. Standart RDBMS (Postgres/MySQL) büyük veri setlerinde (Big Data) analitik sorgularda yavaş kalır.
* **Columnar Storage:** ClickHouse sütun bazlı saklama yapar, bu da `SUM(balance)` gibi agregasyon sorgularını 100x hızlandırır.
* **Compression:** Veriyi diskte çok daha az yer kaplayacak şekilde sıkıştırır.

### B. Neden Celery & Redis?
Veri senkronizasyonu (Sync) uzun süren bir işlemdir (Long-running process).
* HTTP isteği içinde (Synchronous) yapılırsa Timeout hataları alınır.
* Celery ile işlem arka plana (Background) atılarak sistemin tepki süresi (Responsiveness) korunur.

### C. Neden FastAPI + Django Hybrid?
* **FastAPI (Gateway):** Yüksek concurrency (eşzamanlılık) ve düşük latency için.
* **Django (Adapter):** Güçlü ORM yapısı, Admin paneli ve olgun ekosistemi ile karmaşık iş mantığını yönetmek için.

### D. Frontend Strategy (Micro-Frontends)
İki ayrı React uygulaması geliştirilmiştir:
1.  **External Bank UI:** Kaynak sistemi manipüle etmek için.
2.  **Main Dashboard:** Son kullanıcı analitiği için.
Bu ayrım, "Test Ortamı" ile "Prodüksiyon Ortamı"nın net bir şekilde ayrılmasını sağlar.

---

## 4. Security (Güvenlik)

* **Tenant Isolation:** API Gateway, `tenant_id` parametresini kullanıcının girdisinden değil, güvenli veritabanından (API Key eşleşmesi) alır.
* **Network Policy:** Frontend uygulamaları Docker container isimlerine (`http://adapter`) erişemez, sadece host üzerinden (`http://localhost`) erişir. Docker network izolasyonu uygulanmıştır.