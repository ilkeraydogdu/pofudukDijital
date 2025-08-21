"""
Company data normalization module
"""

import re
import unicodedata
from typing import Dict, List, Optional

from ..models.schemas import CompanyType, UnifiedCompany


class CompanyNormalizer:
    """Normalize company data for consistency and deduplication"""
    
    def __init__(self):
        # Turkish company type patterns
        self.company_type_patterns = {
            CompanyType.ANONIM: [
                r'\bA\.Ş\.?\b',
                r'\bANONİM\s+ŞİRKET[İI]?\b',
                r'\bA\.S\.?\b'
            ],
            CompanyType.LIMITED: [
                r'\bLTD\.?\s*ŞT[İI]\.?\b',
                r'\bL[İI]M[İI]TED\s+ŞİRKET[İI]?\b',
                r'\bLTD\.?\b'
            ],
            CompanyType.SAHIS: [
                r'\bŞAHIS\s+ŞİRKET[İI]?\b',
                r'\bŞAHIS\b'
            ],
            CompanyType.KOLEKTIF: [
                r'\bKOLEKT[İI]F\s+ŞİRKET[İI]?\b',
                r'\bKOL\.?\s*ŞT[İI]\.?\b'
            ],
            CompanyType.KOMANDIT: [
                r'\bKOMAND[İI]T\s+ŞİRKET[İI]?\b',
                r'\bKOM\.?\s*ŞT[İI]\.?\b'
            ]
        }
        
        # Common abbreviations to expand
        self.abbreviations = {
            'TIC': 'TİCARET',
            'SAN': 'SANAYİ',
            'PAZ': 'PAZARLAMA',
            'MÜH': 'MÜHENDİSLİK',
            'İNŞ': 'İNŞAAT',
            'BİLİŞ': 'BİLİŞİM',
            'TEK': 'TEKNOLOJİ',
            'TURZ': 'TURİZM',
            'LOJ': 'LOJİSTİK',
            'İTH': 'İTHALAT',
            'İHR': 'İHRACAT',
        }
        
        # Turkish character normalization
        self.turkish_chars = {
            'ı': 'i', 'İ': 'I',
            'ğ': 'g', 'Ğ': 'G',
            'ü': 'u', 'Ü': 'U',
            'ş': 's', 'Ş': 'S',
            'ö': 'o', 'Ö': 'O',
            'ç': 'c', 'Ç': 'C'
        }
    
    def normalize_company_name(self, name: str) -> str:
        """Normalize company name for consistency"""
        if not name:
            return ""
        
        # Convert to uppercase
        name = name.upper()
        
        # Remove extra whitespace
        name = ' '.join(name.split())
        
        # Expand common abbreviations
        for abbr, full in self.abbreviations.items():
            name = re.sub(r'\b' + abbr + r'\b', full, name)
        
        # Remove company type suffixes for matching
        for patterns in self.company_type_patterns.values():
            for pattern in patterns:
                name = re.sub(pattern, '', name, flags=re.IGNORECASE)
        
        # Remove punctuation except for essential ones
        name = re.sub(r'[^\w\s&-]', ' ', name)
        
        # Remove extra whitespace again
        name = ' '.join(name.split())
        
        return name.strip()
    
    def normalize_for_matching(self, name: str) -> str:
        """Normalize name for fuzzy matching (more aggressive)"""
        name = self.normalize_company_name(name)
        
        # Convert Turkish characters to ASCII
        for tr_char, ascii_char in self.turkish_chars.items():
            name = name.replace(tr_char.upper(), ascii_char)
        
        # Remove all non-alphanumeric
        name = re.sub(r'[^A-Z0-9]', '', name)
        
        return name
    
    def extract_company_type(self, name: str) -> Optional[CompanyType]:
        """Extract company type from name"""
        name_upper = name.upper()
        
        for company_type, patterns in self.company_type_patterns.items():
            for pattern in patterns:
                if re.search(pattern, name_upper, re.IGNORECASE):
                    return company_type
        
        return None
    
    def normalize_phone(self, phone: str) -> str:
        """Normalize phone number to E.164 format"""
        if not phone:
            return ""
        
        # Remove all non-digits
        phone = re.sub(r'\D', '', phone)
        
        # Handle Turkish numbers
        if phone.startswith('90'):
            phone = '+' + phone
        elif phone.startswith('0'):
            phone = '+90' + phone[1:]
        elif len(phone) == 10:  # Turkish number without country code
            phone = '+90' + phone
        elif not phone.startswith('+'):
            phone = '+90' + phone
        
        return phone
    
    def normalize_email(self, email: str) -> str:
        """Normalize email address"""
        if not email:
            return ""
        
        email = email.lower().strip()
        
        # Remove mailto: prefix if present
        email = email.replace('mailto:', '')
        
        # Basic validation
        if '@' not in email or '.' not in email.split('@')[1]:
            return ""
        
        return email
    
    def normalize_url(self, url: str) -> str:
        """Normalize URL"""
        if not url:
            return ""
        
        url = url.lower().strip()
        
        # Add protocol if missing
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        # Remove trailing slash
        url = url.rstrip('/')
        
        # Remove www. for consistency
        url = re.sub(r'://www\.', '://', url)
        
        return url
    
    def normalize_city(self, city: str) -> str:
        """Normalize Turkish city names"""
        if not city:
            return ""
        
        city = city.strip().title()
        
        # Common variations
        city_mappings = {
            'Istanbul': 'İstanbul',
            'Ankara': 'Ankara',
            'Izmir': 'İzmir',
            'Bursa': 'Bursa',
            'Antalya': 'Antalya',
            'Adana': 'Adana',
            'Konya': 'Konya',
            'Gaziantep': 'Gaziantep',
            'Kocaeli': 'Kocaeli',
            'Kayseri': 'Kayseri',
        }
        
        # Try to match with common variations
        for variant, normalized in city_mappings.items():
            if city.upper() == variant.upper():
                return normalized
        
        return city
    
    def normalize_address(self, address: str) -> str:
        """Normalize address"""
        if not address:
            return ""
        
        # Remove extra whitespace
        address = ' '.join(address.split())
        
        # Standardize common abbreviations
        address_abbr = {
            'Mah.': 'Mahallesi',
            'Cad.': 'Caddesi',
            'Sok.': 'Sokak',
            'Apt.': 'Apartmanı',
            'Blok': 'Blok',
            'Kat': 'Kat',
            'No:': 'No:',
        }
        
        for abbr, full in address_abbr.items():
            address = address.replace(abbr, full)
        
        return address.strip()
    
    def normalize_company(self, company: UnifiedCompany) -> UnifiedCompany:
        """Normalize all fields in a company record"""
        # Normalize identity
        if company.identity:
            company.identity.legal_name = self.normalize_company_name(
                company.identity.legal_name
            )
            if company.identity.trade_name:
                company.identity.trade_name = self.normalize_company_name(
                    company.identity.trade_name
                )
            if company.identity.city:
                company.identity.city = self.normalize_city(company.identity.city)
            
            # Extract company type if not set
            if not company.identity.company_type:
                company.identity.company_type = self.extract_company_type(
                    company.identity.legal_name
                )
        
        # Normalize web presence
        if company.web_presence:
            if company.web_presence.website_url:
                company.web_presence.website_url = self.normalize_url(
                    str(company.web_presence.website_url)
                )
            
            # Normalize social links
            if company.web_presence.social_links:
                for platform, link in company.web_presence.social_links.items():
                    if link:
                        company.web_presence.social_links[platform] = self.normalize_url(link)
        
        # Normalize contacts
        if company.contacts:
            if company.contacts.emails_public:
                company.contacts.emails_public = [
                    self.normalize_email(email) 
                    for email in company.contacts.emails_public
                    if self.normalize_email(email)
                ]
            
            if company.contacts.phones_public:
                company.contacts.phones_public = [
                    self.normalize_phone(phone)
                    for phone in company.contacts.phones_public
                    if self.normalize_phone(phone)
                ]
            
            if company.contacts.address_public:
                company.contacts.address_public = self.normalize_address(
                    company.contacts.address_public
                )
        
        return company
    
    def normalize_batch(self, companies: List[UnifiedCompany]) -> List[UnifiedCompany]:
        """Normalize a batch of companies"""
        return [self.normalize_company(company) for company in companies]