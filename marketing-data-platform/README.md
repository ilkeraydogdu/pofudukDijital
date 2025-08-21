# Marketing Data Platform 🚀

KVKK/GDPR uyumlu, Türkiye ve global şirket verilerini toplayan, normalize eden ve aranabilir hale getiren kurumsal pazarlama verisi platformu.

## 🎯 Özellikler

- **Otomatik Veri Toplama**: Google Search, Google Places, web siteleri ve sosyal medya platformlarından halka açık şirket verilerini toplar
- **Akıllı Deduplikasyon**: Gelişmiş entity resolution algoritmaları ile tekrar eden kayıtları otomatik birleştirir
- **KVKK/GDPR Uyumlu**: Kişisel veri minimizasyonu, silme hakkı (RTBF) ve uyum raporlaması
- **Güçlü Arama**: OpenSearch/Elasticsearch tabanlı hızlı ve esnek arama
- **Veri Kalitesi**: Great Expectations ile otomatik veri kalite kontrolleri
- **Segmentasyon**: Şirketleri otomatik olarak pazarlama segmentlerine ayırır
- **RESTful API**: FastAPI ile modern, hızlı ve dokümante edilmiş API
- **Orkestrasyon**: Apache Airflow ile zamanlanmış ve yönetilebilir ETL pipeline'ları

## 📋 Gereksinimler

- Docker & Docker Compose
- Python 3.11+
- 8GB+ RAM
- 20GB+ Disk alanı

## 🚀 Hızlı Başlangıç

### 1. Projeyi Klonlayın
```bash
git clone https://github.com/your-org/marketing-data-platform.git
cd marketing-data-platform
```

### 2. Ortam Değişkenlerini Ayarlayın
```bash
cp .env.example .env
# .env dosyasını düzenleyerek API anahtarlarınızı ekleyin:
# - GOOGLE_PLACES_API_KEY
# - GOOGLE_CSE_API_KEY (Custom Search Engine)
# - GOOGLE_CSE_CX (Custom Search Engine ID)
```

### 3. Servisleri Başlatın
```bash
make up
```

Bu komut tüm servisleri başlatacaktır:
- PostgreSQL (5432)
- OpenSearch (9200)
- Redis (6379)
- MinIO (9000/9001)
- FastAPI (8000)
- Airflow (8080)
- Grafana (3000)

### 4. Veritabanını Başlatın
```bash
make db-init
make index-create
```

### 5. İlk Veri Toplamayı Başlatın
```bash
make crawl
```

## 📚 Kullanım

### API Endpoints

**Arama:**
```bash
curl -X POST "http://localhost:8000/api/v1/search" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "teknoloji şirketi istanbul",
    "filters": {"city": "İstanbul", "priority_tier": "A"},
    "page": 1,
    "page_size": 20
  }'
```

**Şirket Detayı:**
```bash
curl "http://localhost:8000/api/v1/company/{company_id}"
```

**Veri İhracatı:**
```bash
curl -X POST "http://localhost:8000/api/v1/export" \
  -H "Content-Type: application/json" \
  -d '{
    "format": "csv",
    "filters": {"city": "İstanbul"},
    "limit": 1000
  }' > export.csv
```

**Analitik:**
```bash
curl "http://localhost:8000/api/v1/analytics"
```

### Airflow DAG Yönetimi

Airflow web arayüzüne http://localhost:8080 adresinden erişebilirsiniz:
- Kullanıcı: admin
- Şifre: admin

Mevcut DAG'lar:
- `company_data_etl`: Ana ETL pipeline (günlük)
- Discover → Fetch → Parse → Normalize → Validate → Deduplicate → Enrich → Load

### Veri Kalitesi Kontrolleri

```bash
make quality-check
```

Bu komut Great Expectations kurallarını çalıştırır ve veri kalite raporu oluşturur.

### Arama İndeksi Yönetimi

```bash
# İndeksi yeniden oluştur
make index-reindex

# İndeks istatistikleri
curl "http://localhost:9200/companies/_stats"
```

## 🏗️ Mimari

```
┌─────────────────────────────────────────────────────────────┐
│                         Veri Kaynakları                      │
│  Google Search │ Google Places │ Websites │ Social Media    │
└────────────────┬────────────────────────────────────────────┘
                 │
┌────────────────▼────────────────────────────────────────────┐
│                      Collectors (Python)                     │
│  Rate Limiting │ Robots.txt │ Caching │ PII Filtering       │
└────────────────┬────────────────────────────────────────────┘
                 │
┌────────────────▼────────────────────────────────────────────┐
│                    Apache Airflow (DAG)                      │
│  Discover → Fetch → Parse → Normalize → Validate → Dedupe   │
└────────────────┬────────────────────────────────────────────┘
                 │
         ┌───────┴───────┬──────────┬──────────┐
         ▼               ▼          ▼          ▼
┌──────────────┐ ┌──────────┐ ┌─────────┐ ┌────────┐
│  PostgreSQL  │ │OpenSearch│ │  MinIO  │ │ Redis  │
│   (OLTP)     │ │ (Search) │ │(Storage)│ │(Cache) │
└──────────────┘ └──────────┘ └─────────┘ └────────┘
         │               │          │          │
┌────────┴───────────────┴──────────┴──────────┴─────────────┐
│                      FastAPI (REST API)                      │
│  Search │ Filter │ Export │ Analytics │ Compliance          │
└─────────────────────────────────────────────────────────────┘
```

## 📊 Veri Şeması

### Unified Company Schema v1

```json
{
  "identity": {
    "legal_name": "string",
    "trade_name": "string",
    "company_type": "enum",
    "country": "ISO code",
    "city": "string",
    "district": "string"
  },
  "web_presence": {
    "website_url": "URL",
    "social_links": {},
    "google_places": {}
  },
  "contacts": {
    "emails_public": ["email"],
    "phones_public": ["phone"],
    "address_public": "string"
  },
  "business_meta": {
    "industry": "string",
    "size": "enum",
    "founding_year": "integer",
    "keywords": ["string"]
  }
}
```

## 🔒 KVKK/GDPR Uyumluluk

Platform, veri koruma yönetmeliklerine tam uyum sağlar:

### Veri Minimizasyonu
- Yalnızca halka açık kurumsal veriler toplanır
- Kişisel e-postalar ve telefonlar otomatik filtrelenir
- Kişi odaklı profiller oluşturulmaz

### Silme Hakkı (RTBF)
```bash
curl -X POST "http://localhost:8000/api/v1/compliance" \
  -H "Content-Type: application/json" \
  -d '{
    "action": "delete",
    "identifier": "company-id",
    "requester_email": "legal@company.com"
  }'
```

### Veri İhracat Hakkı
```bash
curl -X POST "http://localhost:8000/api/v1/compliance" \
  -H "Content-Type: application/json" \
  -d '{
    "action": "export",
    "identifier": "company-id",
    "requester_email": "legal@company.com"
  }'
```

### Uyum Raporu
```bash
make test-compliance
```

## 🧪 Test

```bash
# Tüm testleri çalıştır
make test

# Sadece deduplikasyon testleri
make test-unit

# KVKK/GDPR uyum testleri
make test-compliance

# Kod kalitesi kontrolleri
make lint
```

## 📈 İzleme ve Metrikler

### Prometheus Metrikleri
http://localhost:9090

### Grafana Dashboard
http://localhost:3000 (admin/admin)

### Sağlık Kontrolü
```bash
curl http://localhost:8000/api/v1/health
```

## 🛠️ Geliştirme

### Geliştirme Ortamını Başlat
```bash
make dev
```

Bu komut ek olarak başlatır:
- Jupyter Notebook (8888)
- pgAdmin (5050)
- MailHog (8025)

### Yeni Collector Ekleme

1. `src/collectors/` altında yeni collector oluşturun
2. `BaseCollector` sınıfından türetin
3. `collect()` ve `parse()` metodlarını implement edin
4. Airflow DAG'a ekleyin

### Veri Kalite Kuralı Ekleme

1. `src/quality/expectations.py` dosyasını düzenleyin
2. Yeni expectation ekleyin
3. `make quality-check` ile test edin

## 🚢 Production Deployment

### Kubernetes Deployment
```bash
kubectl apply -f k8s/
```

### Docker Swarm
```bash
docker stack deploy -c docker-compose.prod.yml marketing-platform
```

### Yedekleme
```bash
# Veritabanı yedekleme
make db-backup

# Geri yükleme
make db-restore
```

## 📝 Makefile Komutları

```bash
make help         # Tüm komutları listele
make up           # Servisleri başlat
make down         # Servisleri durdur
make logs         # Logları göster
make crawl        # Veri toplamayı başlat
make transform    # dbt dönüşümlerini çalıştır
make test         # Testleri çalıştır
make clean        # Temizlik yap
```

## 🤝 Katkıda Bulunma

1. Fork edin
2. Feature branch oluşturun (`git checkout -b feature/amazing-feature`)
3. Değişikliklerinizi commit edin (`git commit -m 'Add amazing feature'`)
4. Branch'e push edin (`git push origin feature/amazing-feature`)
5. Pull Request açın

## 📜 Lisans

Bu proje MIT lisansı altında lisanslanmıştır. Detaylar için [LICENSE](LICENSE) dosyasına bakın.

## 🆘 Destek

- 📧 Email: support@marketingplatform.com
- 📚 Dokümantasyon: [docs/](docs/)
- 🐛 Bug Report: [GitHub Issues](https://github.com/your-org/marketing-data-platform/issues)

## 🙏 Teşekkürler

Bu platform aşağıdaki açık kaynak projeleri kullanmaktadır:
- Apache Airflow
- OpenSearch
- FastAPI
- dbt
- Great Expectations
- PostgreSQL
- Redis

---
**Not**: Bu platform yalnızca halka açık verileri toplar ve KVKK/GDPR yönetmeliklerine tam uyum sağlar. Platform kullanıcıları, yerel veri koruma yasalarına uymaktan sorumludur.