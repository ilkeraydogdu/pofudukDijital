# KVKK/GDPR Uyum Politikası

## 1. Giriş

Bu belge, Marketing Data Platform'un Kişisel Verilerin Korunması Kanunu (KVKK) ve Genel Veri Koruma Yönetmeliği (GDPR) ile uyumlu olarak nasıl çalıştığını açıklar.

## 2. Temel İlkeler

### 2.1 Veri Minimizasyonu
- Platform yalnızca **halka açık** ve **kurumsal** verileri toplar
- Kişisel veriler otomatik olarak filtrelenir veya anonimleştirilir
- Gereksiz veri toplanmaz

### 2.2 Amaç Sınırlaması
- Veriler yalnızca B2B pazarlama ve iş geliştirme amaçlı kullanılır
- Kişisel profilleme yapılmaz
- Veri satışı yapılmaz

### 2.3 Şeffaflık
- Tüm veri kaynakları belgelenmiştir
- Veri işleme süreçleri açıktır
- Denetim kayıtları tutulur

## 3. Veri Toplama Kuralları

### 3.1 İzin Verilen Kaynaklar
✅ **Toplanabilir:**
- Şirket web sitelerinin halka açık sayfaları
- Google Business profilleri
- Halka açık sosyal medya işletme sayfaları
- Ticaret sicil gazeteleri
- Halka açık dizinler

❌ **Toplanmaz:**
- Üyelik gerektiren içerikler
- Ücretli/paywall arkasındaki veriler
- Kişisel sosyal medya profilleri
- CAPTCHA korumalı sayfalar (otomatik)
- robots.txt ile yasaklanan sayfalar

### 3.2 Veri Türleri

**Kurumsal Veriler (Toplanır):**
- Şirket unvanı ve ticari isim
- Kurumsal e-posta adresleri (info@, contact@, sales@ vb.)
- İşletme telefon numaraları
- İşletme adresleri
- Web sitesi ve sosyal medya linkleri
- Sektör ve faaliyet bilgileri

**Kişisel Veriler (Filtrelenir):**
- Kişi isimleri (CEO, çalışan vb.)
- Kişisel e-postalar (isim.soyisim@ formatı)
- Kişisel telefon numaraları
- TC kimlik numaraları
- Finansal bilgiler

## 4. Teknik Güvenlik Önlemleri

### 4.1 Veri Güvenliği
- Tüm veriler şifreli olarak saklanır
- SSL/TLS ile güvenli iletişim
- Düzenli güvenlik güncellemeleri
- Erişim kontrolü ve yetkilendirme

### 4.2 PII Filtreleme
```python
# Otomatik PII filtreleme örneği
def filter_pii(data):
    # TC Kimlik numarası kontrolü
    data = re.sub(r'\b\d{11}\b', '[REDACTED]', data)
    
    # Kişisel e-posta kontrolü
    if is_personal_email(email):
        return None
    
    # Kişi ismi kontrolü
    if is_personal_name(text):
        return '[REDACTED]'
```

### 4.3 Veri Saklama Süreleri
- Aktif veriler: 365 gün
- Arşiv verileri: 730 gün
- Silme talepleri: 30 gün içinde işlenir
- Denetim logları: 1095 gün

## 5. Veri Sahiplerinin Hakları

### 5.1 Bilgi Edinme Hakkı
Veri sahipleri, kendileri hakkında toplanan verileri öğrenme hakkına sahiptir.

**API Endpoint:**
```bash
POST /api/v1/compliance
{
  "action": "export",
  "identifier": "company-id",
  "requester_email": "legal@company.com"
}
```

### 5.2 Silme Hakkı (RTBF - Right to be Forgotten)
Veri sahipleri, verilerinin silinmesini talep edebilir.

**API Endpoint:**
```bash
POST /api/v1/compliance
{
  "action": "delete",
  "identifier": "company-id",
  "reason": "GDPR Article 17",
  "requester_email": "legal@company.com"
}
```

### 5.3 Düzeltme Hakkı
Yanlış veya eksik verilerin düzeltilmesi talep edilebilir.

### 5.4 İşlemeyi Durdurma Hakkı
Veri işlemenin durdurulması (suppression) talep edilebilir.

**API Endpoint:**
```bash
POST /api/v1/compliance
{
  "action": "suppress",
  "identifier": "company-id",
  "requester_email": "legal@company.com"
}
```

## 6. Veri İşleme Süreci

### 6.1 Toplama Aşaması
1. robots.txt kontrolü
2. Rate limiting uygulama
3. Halka açık veri kontrolü
4. PII filtreleme

### 6.2 İşleme Aşaması
1. Veri normalizasyonu
2. Deduplikasyon
3. Kalite kontrolleri
4. Anonimleştirme

### 6.3 Saklama Aşaması
1. Şifreli depolama
2. Erişim loglaması
3. Periyodik temizlik
4. Yedekleme

## 7. Oran Sınırlama ve Etik Tarama

### 7.1 Rate Limiting
- Global: 2 istek/saniye
- Site başına: 1 istek/saniye
- Exponential backoff
- Crawl-delay'e uyum

### 7.2 User-Agent
```
MarketingPlatform/1.0 (+https://platform.com/bot)
```

### 7.3 robots.txt Uyumu
- Her site için robots.txt kontrolü
- Disallow kurallarına tam uyum
- Crawl-delay'e saygı

## 8. Denetim ve Raporlama

### 8.1 Denetim Logları
Tüm veri işleme aktiviteleri loglanır:
- Kim, ne zaman, hangi veriyi işledi
- Hangi kaynaktan veri toplandı
- Silme/düzeltme talepleri

### 8.2 Uyum Raporları
Aylık uyum raporları içerir:
- Toplanan veri miktarı
- PII filtreleme istatistikleri
- Silme talepleri
- Veri ihlali bildirimleri (varsa)

### 8.3 Veri Kalite Metrikleri
```python
{
  "total_records": 10000,
  "pii_detected": 12,
  "suppressed_records": 5,
  "data_quality_score": 98.5,
  "compliance_score": 99.8
}
```

## 9. Veri İhlali Prosedürü

### 9.1 Tespit
- Otomatik anomali tespiti
- 7/24 monitoring
- Anlık alertler

### 9.2 Müdahale (İlk 24 saat)
1. İhlali durdur
2. Kapsamı belirle
3. Etkilenenleri tespit et
4. Geçici önlemler al

### 9.3 Bildirim (72 saat içinde)
1. KVKK/GDPR otoritelerine bildir
2. Etkilenen veri sahiplerini bilgilendir
3. Kamuoyunu bilgilendir (gerekirse)

### 9.4 İyileştirme
1. Kök neden analizi
2. Kalıcı düzeltmeler
3. Prosedür güncelleme
4. Eğitim

## 10. Üçüncü Taraf Entegrasyonları

### 10.1 Veri İşleyiciler
- Google APIs: Veri işleyici sözleşmesi mevcut
- AWS/MinIO: Şifreli depolama
- OpenSearch: On-premise kurulum

### 10.2 Veri Paylaşımı
- Veri satışı yapılmaz
- Yalnızca yasal zorunluluklar dahilinde paylaşım
- Anonimleştirilmiş agregat veriler paylaşılabilir

## 11. Coğrafi Kısıtlamalar

### 11.1 Veri Lokasyonu
- Birincil: Türkiye
- Yedek: AB bölgesi
- ABD'ye veri transferi yapılmaz

### 11.2 Erişim Kısıtlamaları
- GDPR kapsamındaki ülkelerden erişim
- IP bazlı kısıtlamalar
- Geo-blocking uygulaması

## 12. Eğitim ve Farkındalık

### 12.1 Çalışan Eğitimi
- KVKK/GDPR temel eğitimi
- Veri güvenliği eğitimi
- Yıllık güncelleme eğitimleri

### 12.2 Dokümantasyon
- Güncel uyum politikaları
- Teknik dokümantasyon
- Kullanıcı kılavuzları

## 13. İletişim

### Veri Koruma Yetkilisi (DPO)
- Email: dpo@marketingplatform.com
- Telefon: +90 XXX XXX XX XX
- Adres: [Şirket Adresi]

### Uyum Talepleri
- Email: compliance@marketingplatform.com
- Web Form: https://platform.com/compliance
- Yanıt Süresi: Maksimum 30 gün

## 14. Politika Güncellemeleri

Bu politika düzenli olarak gözden geçirilir ve güncellenir:
- Son Güncelleme: 2024-01-01
- Sonraki İnceleme: 2024-07-01
- Versiyon: 1.0.0

## 15. Yasal Dayanak

### KVKK Maddeleri
- Madde 4: Kişisel verilerin işlenmesi
- Madde 5: Genel ilkeler
- Madde 6: İşleme şartları
- Madde 11: Veri sahibinin hakları

### GDPR Maddeleri
- Article 5: İşleme ilkeleri
- Article 6: İşlemenin yasallığı
- Article 17: Silme hakkı
- Article 25: Tasarımda ve varsayılanda veri koruma

---

**Not**: Bu politika, yasal gerekliliklerdeki değişikliklere göre güncellenebilir. Güncel versiyonu her zaman platform üzerinden erişilebilir durumdadır.