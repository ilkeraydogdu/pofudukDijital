ROL: Kıdemli Veri Toplama Mühendisi + Veri Mimar + KVKK/GDPR Uyum Uzmanı.
AMAÇ: Türkiye’de ve globalde faaliyet gösteren şirketler (Anonim, Limited, Şahıs, Kolektif, Komandit, vb.) hakkında HALKA AÇIK verileri otomatik toplayan, normalize eden, birleştiren ve aranabilir hale getiren bir “pazarlama verisi platformu” kur.

NET KURALLAR:
- Yalnızca halka açık ve erişimi engellenmemiş sayfalar. Üyelik/oturum gerektiren, ücretli, paywall’lı alan yok.
- Platformların kullanım şartlarına uy. Resmî API varsa önce onu kullan. Yoksa robots.txt’ye, oran sınırlamalarına (rate limit) ve telif kurallarına saygı duy.
- KVKK/GDPR: Kişisel veriyi en aza indir (data minimization). Sadece “kurumsal” veriler, kişi adı varsa işlemenin pazarlama meşru menfaat ve açık rıza gerekliliklerini kontrol et. Talep gelirse sil (RTBF).
- CAPTCHAları otomatik aşma yok. Karşılaşırsan isteğe bağlı manuel onay kuyruğu aç.

HEDEF ÇIKTI:
- Bir ETL/ELT hattı, bir birleşik şirket veri şeması, deduplikasyon/varlık eşleştirme (entity resolution), kalite kuralları, arama ve filtreleme ara yüzü.
- Tüm işlem adımlarını kodla, containerize et (Docker), orkestrasyon (Airflow) ile zamanla.
- Ölçeklenebilir depolama: OLTP için PostgreSQL, arama için OpenSearch/Elasticsearch, ham döküm için S3-compatible object storage.

KAPSAM: Veri Kaynakları (öncelik sırası ve yöntem)
1) Google Arama + Google Custom Search API:
   - site:linkedin.com/company/ “company pages” (yalnızca login gerektirmeyen public sayfa ön izlemeleri/özetleri).
   - site:linkedin.com/in/ kullanılmaz (kişisel veriyi minimize et; sadece şirket sayfaları).
   - site:crunchbase.com/organization (eğer sayfa public ise özet meta; robots.txt’ye uy).
   - site:tr.linkedin.com/company/ varyantları.
   - site:maps.google.com ve Google Places API: Unvan, konum, telefon, web sitesi, kategori, kullanıcı yorumu sayısı/puan (sadece işletme düzeyi).
   - site:instagram.com/* public business account bios (web-bio, link, e-mail görünüyor ise).
   - site:facebook.com/*/about public business pages.
   - site:x.com/* (eski Twitter) işletme hesap bio ve linkleri public ise.
   - Resmî ticaret kaynakları (public erişilebilen): 
     * Türkiye Ticaret Sicil Gazetesi’nde firma başlık ve ilan meta özetleri
     * Açık şirket dizinleri (OpenCorporates API, var ise)
   - Şirket web siteleri: yalnızca ana sayfa ve “iletişim/hakkımızda” sayfaları.

2) Zenginleştirme:
   - WHOIS (müsaitse ve yasal çerçevede): organizasyon adı, yaratılış tarihi (rate limit’e uy).
   - E-posta çıkarımı: sadece web’de açıkça yayınlanmış kurumsal e-postalar (info@, sales@). Kişisel isimli e-postaları toplama.

TOPLANACAK ALANLAR (Unified Schema v1):
- identity:
  * legal_name
  * trade_name (varsa)
  * company_type (Anonim, Limited, Şahıs, vb.)
  * country, city, district
  * registration_hint (kayıt no/ilan referansları gibi sadece public kaynaklardan)
- web_presence:
  * website_url, website_status_code, ssl_issuer, first_seen_ts, last_seen_ts
  * social_links: {linkedin_company, instagram_business, facebook_page, x_account, youtube_channel}
  * google_places: {place_id, rating, reviews_count, opening_hours?}
- contacts (kurumsal):
  * emails_public (sadece site/Google Business’ta açık olan)
  * phones_public
  * address_public (Google Places’ten veya sitenin iletişim sayfasından)
- business_meta:
  * industry_naics_guess, sic_guess (keyword + ML sınıflandırma)
  * headcount_band_guess (LinkedIn public summary’dan sadece arama snippet’i/structured data varsa)
  * founding_year_guess (whois/domain creation veya “hakkımızda” metni)
- seo_signals:
  * title, meta_description, h1_keywords (ana sayfa)
  * alexa/benzeri metrik YOK (kullanımdan kalktı) → yerine basit “indexed_pages_estimate, backlinks_count_guess?” (sadece public ve legal yöntemle)
- provenance (soy kütüğü):
  * source_url, source_type, fetch_ts, hash, parser_version

ÇIKTI FORMATLARI:
- Raw JSON (S3): kaynak başına tek belge.
- Bronze (staging) tablolar: kaynak bazlı normalize.
- Silver (unified) tablo: unified_company.
- Gold (pazarlama görünümleri): segment_* (örn: “İstanbul + Limited + SaaS şüphesi + 5-50 tahmini çalışan”).

MİMARİ:
- Collector’lar: Python + Playwright (yalnızca halka açık sayfa render gerektiğinde), çoğu istek için requests/HTTPx.
- Scrapy pipeline (veya iş başına lightweight collector).
- Orkestrasyon: Airflow (DAG: discover → fetch → parse → validate → dedupe → unify → enrich → index).
- Depo: PostgreSQL (OLTP), OpenSearch/Elasticsearch (arama), MinIO/S3 (ham).
- Dönüşüm: dbt (bronze→silver→gold).
- Kalite: Great Expectations (zorunlu alanlar, regex’ler, domain setleri).
- İzleme: Prometheus + Grafana (iş sayısı, hata oranı, latency, rate limit hitleri).
- API: FastAPI (arama, filtre, ihracat CSV/XLSX).
- UI: Next.js + Tailwind; arama, filtre, CSV export, profil kartı.

DE-DUPLICATION / ENTITY RESOLUTION:
- Deterministik anahtar: normalize(legal_name) + city veya website_domain.
- Fuzzy eşleme: Jaro-Winkler/Levenshtein skorları; alan ağırlıkları: website_domain (0.5), legal_name (0.3), phone (0.1), address (0.1).
- Eşikler: exact ≥0.95 birleş, 0.85–0.95 manuel inceleme kuyruğu, <0.85 ayrı kayıt.
- Çoklu ad/marka: alias tablosu.

ORAN SINIRLAMA & NAZİK TARAYICI (Politeness):
- Global qps: 1–3 req/s/kaynak. Exponential backoff.
- robots.txt kontrolü. Disallow varsa sayfayı atla.
- User-Agent açık/temiz. Crawl-delay’e saygı.
- Caching: ETag/Last-Modified, 7–30 gün TTL.
- CAPTCHAlar: işaretle, manual_review kuyruğuna at.

KVKK/GDPR UYUMU:
- Yalnızca kurumsal veriler. Kişi odaklı profiller yok.
- Silme isteği (RTBF) uçtan uca: suppression_list kontrolü → tüm endekslerde yumuşak silme (soft delete) + ham veride maskeleme.
- Denetim izi: her alan için provenance sakla.
- PII min.: kişisel e-posta/telefon görürsen kaydetme/maskele.

TEST & KABUL:
- 50 örnek şirket listesiyle uçtan uca koş. %95+ dedupe doğruluğu, %98 JSON şema geçerliliği, <%2 hata oranı.
- UI aramada <300 ms P95 latency (Elasticsearch).
- Rate-limit ihlali 0.

TESLİM:
- Docker Compose (dev) + Kubernetes manifest (prod).
- Makefile komutları: make crawl, make transform, make test, make serve.
- Belgeler: README (kurulum), /docs (kaynak bazlı toplama kuralları), /compliance (KVKK).

ŞİMDİ ÜRETECEKLERİN:
1) Python modül iskeleti (collector, parser, normalizer, enricher).
2) Airflow DAG örneği.
3) dbt modelleri için şema örneği.
4) Great Expectations kuralları.
5) FastAPI arama uç noktaları.
6) Elasticsearch mapping + örnek sorgular.
7) Dockerfile’lar + docker-compose.yaml.
8) Test veri seti + dedupe birim testi.
9) README ve uyum/kullanım politikası özeti.
GOOGLE_PLACES_API_KEY=AIzaSyALGE2sGmpW4O10b7EqAldQ
FOURSQUARE_API_KEY=FWCVIVJFV2P5TCKZFFDDFJZGXC3Y51IG2OZGXCSNNABX1TKW
HUNTER_IO_API_KEY=c0f7b88560bb49546459497ea049e64ed9051b89
