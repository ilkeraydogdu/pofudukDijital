"""
Tests for entity resolution and deduplication
"""

import pytest
from datetime import datetime
from typing import List

from src.models.schemas import (
    UnifiedCompany,
    CompanyIdentity,
    WebPresence,
    ContactInfo,
    CompanyType
)
from src.deduplication.entity_resolver import EntityResolver
from src.normalizers.company_normalizer import CompanyNormalizer


class TestEntityResolver:
    """Test entity resolution and deduplication"""
    
    @pytest.fixture
    def resolver(self):
        """Create entity resolver instance"""
        return EntityResolver(
            exact_threshold=0.95,
            review_threshold=0.85,
            min_threshold=0.70
        )
    
    @pytest.fixture
    def normalizer(self):
        """Create normalizer instance"""
        return CompanyNormalizer()
    
    @pytest.fixture
    def sample_companies(self) -> List[UnifiedCompany]:
        """Create sample company data for testing"""
        companies = [
            # Exact duplicate - same company with slight variations
            UnifiedCompany(
                id="1",
                identity=CompanyIdentity(
                    legal_name="ABC Teknoloji A.Ş.",
                    city="İstanbul",
                    company_type=CompanyType.ANONIM
                ),
                web_presence=WebPresence(
                    website_url="https://www.abcteknoloji.com"
                ),
                contacts=ContactInfo(
                    emails_public=["info@abcteknoloji.com"],
                    phones_public=["+902123456789"]
                )
            ),
            UnifiedCompany(
                id="2",
                identity=CompanyIdentity(
                    legal_name="ABC TEKNOLOJİ ANONİM ŞİRKETİ",
                    city="Istanbul",
                    company_type=CompanyType.ANONIM
                ),
                web_presence=WebPresence(
                    website_url="http://abcteknoloji.com"
                ),
                contacts=ContactInfo(
                    emails_public=["contact@abcteknoloji.com"],
                    phones_public=["02123456789"]
                )
            ),
            
            # Similar but different company
            UnifiedCompany(
                id="3",
                identity=CompanyIdentity(
                    legal_name="ABC Yazılım Ltd. Şti.",
                    city="İstanbul",
                    company_type=CompanyType.LIMITED
                ),
                web_presence=WebPresence(
                    website_url="https://www.abcyazilim.com"
                ),
                contacts=ContactInfo(
                    emails_public=["info@abcyazilim.com"],
                    phones_public=["+902129876543"]
                )
            ),
            
            # Different company entirely
            UnifiedCompany(
                id="4",
                identity=CompanyIdentity(
                    legal_name="XYZ Danışmanlık Ltd. Şti.",
                    city="Ankara",
                    company_type=CompanyType.LIMITED
                ),
                web_presence=WebPresence(
                    website_url="https://www.xyzdanismanlik.com"
                ),
                contacts=ContactInfo(
                    emails_public=["info@xyzdanismanlik.com"],
                    phones_public=["+903121234567"]
                )
            ),
            
            # Company with minimal information
            UnifiedCompany(
                id="5",
                identity=CompanyIdentity(
                    legal_name="Minimal Şirket",
                    city="İzmir"
                )
            ),
            
            # Potential duplicate with different domain
            UnifiedCompany(
                id="6",
                identity=CompanyIdentity(
                    legal_name="ABC Technology Inc.",
                    city="İstanbul"
                ),
                web_presence=WebPresence(
                    website_url="https://www.abc-tech.com.tr"
                ),
                contacts=ContactInfo(
                    phones_public=["+902123456789"]  # Same phone as company 1
                )
            )
        ]
        
        return companies
    
    def test_exact_match_detection(self, resolver, sample_companies):
        """Test detection of exact matches"""
        # Companies 1 and 2 should be detected as duplicates
        matches = resolver.find_duplicates(sample_companies[:2])
        
        assert len(matches) == 1
        assert matches[0].match_score >= 0.85
        assert matches[0].match_type in ['exact', 'fuzzy']
        assert set([matches[0].company_a_id, matches[0].company_b_id]) == {'1', '2'}
    
    def test_fuzzy_match_detection(self, resolver, sample_companies):
        """Test fuzzy matching capabilities"""
        # Test with companies that have similar names
        companies = [sample_companies[0], sample_companies[2]]  # ABC Teknoloji vs ABC Yazılım
        matches = resolver.find_duplicates(companies)
        
        # Should not match as they have different domains and types
        if matches:
            assert matches[0].match_score < resolver.exact_threshold
    
    def test_phone_based_matching(self, resolver, sample_companies):
        """Test matching based on phone numbers"""
        # Companies 1 and 6 have the same phone
        companies = [sample_companies[0], sample_companies[5]]
        matches = resolver.find_duplicates(companies)
        
        assert len(matches) == 1
        assert matches[0].match_fields['phone'] == 1.0  # Exact phone match
    
    def test_no_false_positives(self, resolver, sample_companies):
        """Test that different companies are not matched"""
        # Companies 3 and 4 are completely different
        companies = [sample_companies[2], sample_companies[3]]
        matches = resolver.find_duplicates(companies)
        
        # Should not find any matches above minimum threshold
        assert len(matches) == 0
    
    def test_company_merging(self, resolver, sample_companies):
        """Test merging of duplicate companies"""
        company1 = sample_companies[0]
        company2 = sample_companies[1]
        
        merged = resolver.merge_companies(company1, company2)
        
        # Check that data is properly merged
        assert merged.identity.legal_name == company1.identity.legal_name
        assert len(merged.contacts.emails_public) == 2  # Both emails preserved
        assert len(merged.contacts.phones_public) >= 1  # At least one phone
        assert merged.web_presence.website_url  # Website preserved
    
    def test_batch_deduplication(self, resolver, sample_companies):
        """Test batch deduplication process"""
        deduplicated, matches = resolver.resolve_duplicates(
            sample_companies,
            auto_merge=True
        )
        
        # Should have fewer companies after deduplication
        assert len(deduplicated) < len(sample_companies)
        
        # Check that non-duplicates are preserved
        company_names = [c.identity.legal_name for c in deduplicated]
        assert "XYZ Danışmanlık Ltd. Şti." in company_names
        assert "Minimal Şirket" in company_names
    
    def test_blocking_key_generation(self, resolver, sample_companies):
        """Test blocking key generation for efficient matching"""
        for company in sample_companies:
            key = resolver.create_blocking_key(company)
            assert key  # Should always generate a key
            assert '|' in key or key == 'unknown'  # Should be formatted correctly
    
    def test_similarity_calculation(self, resolver, sample_companies):
        """Test similarity score calculation"""
        company1 = sample_companies[0]
        company2 = sample_companies[1]
        
        score, field_scores = resolver.calculate_company_similarity(company1, company2)
        
        assert 0.0 <= score <= 1.0
        assert 'legal_name' in field_scores
        assert 'domain' in field_scores
        assert field_scores['domain'] == 1.0  # Same domain after normalization
    
    def test_confidence_scoring(self, resolver, sample_companies):
        """Test confidence scoring in matches"""
        matches = resolver.find_duplicates(sample_companies)
        
        for match in matches:
            assert 0.0 <= match.match_score <= 1.0
            
            # Check review flag based on score
            if resolver.review_threshold <= match.match_score < resolver.exact_threshold:
                assert match.requires_review
            elif match.match_score >= resolver.exact_threshold:
                assert not match.requires_review


class TestCompanyNormalizer:
    """Test company data normalization"""
    
    @pytest.fixture
    def normalizer(self):
        """Create normalizer instance"""
        return CompanyNormalizer()
    
    def test_name_normalization(self, normalizer):
        """Test company name normalization"""
        test_cases = [
            ("ABC   Teknoloji  A.Ş.", "ABC TEKNOLOJI"),
            ("XYZ LTD. ŞTİ.", "XYZ"),
            ("  Test  Company  ", "TEST COMPANY"),
            ("İstanbul Bilişim Ltd.Şti.", "İSTANBUL BİLİŞİM"),
        ]
        
        for input_name, expected in test_cases:
            normalized = normalizer.normalize_company_name(input_name)
            assert expected in normalized
    
    def test_phone_normalization(self, normalizer):
        """Test phone number normalization"""
        test_cases = [
            ("0212 345 67 89", "+902123456789"),
            ("+90 (212) 345-6789", "+902123456789"),
            ("02123456789", "+902123456789"),
            ("5551234567", "+905551234567"),
            ("+905551234567", "+905551234567"),
        ]
        
        for input_phone, expected in test_cases:
            normalized = normalizer.normalize_phone(input_phone)
            assert normalized == expected
    
    def test_email_normalization(self, normalizer):
        """Test email normalization"""
        test_cases = [
            ("INFO@EXAMPLE.COM", "info@example.com"),
            ("  test@example.com  ", "test@example.com"),
            ("mailto:contact@example.com", "contact@example.com"),
            ("invalid-email", ""),
        ]
        
        for input_email, expected in test_cases:
            normalized = normalizer.normalize_email(input_email)
            assert normalized == expected
    
    def test_url_normalization(self, normalizer):
        """Test URL normalization"""
        test_cases = [
            ("www.example.com", "https://example.com"),
            ("http://www.example.com/", "http://example.com"),
            ("HTTPS://EXAMPLE.COM", "https://example.com"),
            ("example.com/path", "https://example.com/path"),
        ]
        
        for input_url, expected in test_cases:
            normalized = normalizer.normalize_url(input_url)
            assert normalized == expected
    
    def test_city_normalization(self, normalizer):
        """Test Turkish city name normalization"""
        test_cases = [
            ("istanbul", "İstanbul"),
            ("ANKARA", "Ankara"),
            ("izmir", "İzmir"),
            ("Unknown City", "Unknown City"),
        ]
        
        for input_city, expected in test_cases:
            normalized = normalizer.normalize_city(input_city)
            assert normalized == expected
    
    def test_company_type_extraction(self, normalizer):
        """Test extraction of company type from name"""
        test_cases = [
            ("ABC Teknoloji A.Ş.", CompanyType.ANONIM),
            ("XYZ Limited Şirketi", CompanyType.LIMITED),
            ("Test Komandit Şirketi", CompanyType.KOMANDIT),
            ("Regular Company Name", None),
        ]
        
        for name, expected_type in test_cases:
            extracted = normalizer.extract_company_type(name)
            assert extracted == expected_type
    
    def test_batch_normalization(self, normalizer):
        """Test batch normalization of companies"""
        companies = [
            UnifiedCompany(
                identity=CompanyIdentity(
                    legal_name="test company a.ş.",
                    city="istanbul"
                ),
                contacts=ContactInfo(
                    emails_public=["INFO@TEST.COM"],
                    phones_public=["02121234567"]
                )
            ),
            UnifiedCompany(
                identity=CompanyIdentity(
                    legal_name="another ltd. şti.",
                    city="ANKARA"
                )
            )
        ]
        
        normalized = normalizer.normalize_batch(companies)
        
        assert len(normalized) == len(companies)
        assert normalized[0].identity.legal_name == "TEST COMPANY"
        assert normalized[0].identity.city == "İstanbul"
        assert normalized[0].contacts.emails_public[0] == "info@test.com"
        assert normalized[0].contacts.phones_public[0] == "+902121234567"