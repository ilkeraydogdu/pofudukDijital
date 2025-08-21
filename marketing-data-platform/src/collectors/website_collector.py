"""
Website collector for company websites
"""

import re
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from ..models.schemas import (
    BusinessMeta,
    CompanyIdentity,
    ContactInfo,
    DataSource,
    SEOSignals,
    UnifiedCompany,
    WebPresence,
)
from .base_collector import BaseCollector


class WebsiteCollector(BaseCollector):
    """Collector for company websites"""
    
    def __init__(self, **kwargs):
        super().__init__(DataSource.WEBSITE, **kwargs)
        
        # Email regex pattern (corporate emails only)
        self.email_pattern = re.compile(
            r'\b(?:info|contact|sales|support|hello|admin|office)@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b',
            re.IGNORECASE
        )
        
        # Phone regex patterns for Turkey
        self.phone_patterns = [
            re.compile(r'(?:\+90|0)?[\s-]?\(?(?:212|216|312|232|224|262|282|322|342|352|362|372|382|392|422|432|442|452|462|472|482)\)?[\s-]?\d{3}[\s-]?\d{2}[\s-]?\d{2}'),  # Landline
            re.compile(r'(?:\+90|0)?[\s-]?\(?5\d{2}\)?[\s-]?\d{3}[\s-]?\d{2}[\s-]?\d{2}'),  # Mobile
        ]
    
    async def collect(self, url: str, **kwargs) -> List[UnifiedCompany]:
        """Collect company data from website"""
        companies = []
        
        try:
            # Normalize URL
            if not url.startswith(('http://', 'https://')):
                url = f'https://{url}'
            
            parsed_url = urlparse(url)
            base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
            
            # Fetch main page
            content, metadata = await self.fetch(url)
            if not content:
                return companies
            
            # Parse main page
            main_data = await self.parse_page(content, url, metadata)
            
            # Try to fetch contact/about pages
            contact_urls = [
                urljoin(base_url, '/iletisim'),
                urljoin(base_url, '/contact'),
                urljoin(base_url, '/hakkimizda'),
                urljoin(base_url, '/about'),
                urljoin(base_url, '/kurumsal'),
            ]
            
            contact_data = {}
            for contact_url in contact_urls:
                contact_content, contact_meta = await self.fetch(contact_url, use_cache=True)
                if contact_content and contact_meta.get('status_code') == 200:
                    contact_data.update(await self.parse_page(contact_content, contact_url, contact_meta))
                    break
            
            # Merge data
            merged_data = {**main_data, **contact_data}
            
            # Create company object
            company = self.create_company_from_data(merged_data, url, metadata)
            if company:
                companies.append(company)
        
        except Exception as e:
            self.logger.error(f"Error collecting from website {url}: {e}")
        
        return companies
    
    async def parse_page(self, content: str, url: str, metadata: Dict) -> Dict:
        """Parse a web page for company information"""
        data = {}
        
        try:
            soup = BeautifulSoup(content, 'lxml')
            
            # Extract SEO signals
            title = soup.find('title')
            data['title'] = title.text.strip() if title else None
            
            meta_desc = soup.find('meta', attrs={'name': 'description'})
            data['meta_description'] = meta_desc.get('content', '').strip() if meta_desc else None
            
            # Extract H1 keywords
            h1_tags = soup.find_all('h1')
            data['h1_keywords'] = [h1.text.strip() for h1 in h1_tags[:3]]
            
            # Extract company name from various sources
            og_title = soup.find('meta', property='og:title')
            if og_title:
                data['company_name'] = og_title.get('content', '').strip()
            elif title:
                # Try to extract from title
                title_text = title.text.strip()
                if '|' in title_text:
                    data['company_name'] = title_text.split('|')[0].strip()
                elif '-' in title_text:
                    data['company_name'] = title_text.split('-')[0].strip()
                else:
                    data['company_name'] = title_text
            
            # Extract emails
            emails = set()
            text_content = soup.get_text()
            for email in self.email_pattern.findall(text_content):
                emails.add(email.lower())
            
            # Also check mailto links
            for mailto in soup.find_all('a', href=re.compile(r'^mailto:')):
                email = mailto.get('href', '').replace('mailto:', '').strip()
                if self.email_pattern.match(email):
                    emails.add(email.lower())
            
            data['emails'] = list(emails)
            
            # Extract phones
            phones = set()
            for pattern in self.phone_patterns:
                for phone in pattern.findall(text_content):
                    # Normalize phone number
                    phone = re.sub(r'[\s()-]', '', phone)
                    if not phone.startswith('+'):
                        phone = '+90' + phone.lstrip('0')
                    phones.add(phone)
            
            # Also check tel: links
            for tel in soup.find_all('a', href=re.compile(r'^tel:')):
                phone = tel.get('href', '').replace('tel:', '').strip()
                phone = re.sub(r'[\s()-]', '', phone)
                if phone:
                    phones.add(phone)
            
            data['phones'] = list(phones)
            
            # Extract address
            address_keywords = ['adres', 'address', 'location', 'konum']
            for keyword in address_keywords:
                address_elem = soup.find(text=re.compile(keyword, re.IGNORECASE))
                if address_elem:
                    parent = address_elem.parent
                    if parent:
                        address_text = parent.get_text().strip()
                        if len(address_text) > 20 and len(address_text) < 500:
                            data['address'] = address_text
                            break
            
            # Extract social media links
            social_links = {}
            social_patterns = {
                'linkedin': r'linkedin\.com/company/([^/\s]+)',
                'instagram': r'instagram\.com/([^/\s]+)',
                'facebook': r'facebook\.com/([^/\s]+)',
                'twitter': r'(?:twitter|x)\.com/([^/\s]+)',
                'youtube': r'youtube\.com/(?:c/|channel/|user/)([^/\s]+)',
            }
            
            for link in soup.find_all('a', href=True):
                href = link.get('href', '')
                for platform, pattern in social_patterns.items():
                    match = re.search(pattern, href)
                    if match:
                        social_links[platform] = href
            
            data['social_links'] = social_links
            
            # Extract SSL information
            if metadata.get('headers', {}).get('strict-transport-security'):
                data['ssl_enabled'] = True
            
            # Extract keywords from content
            # Simple keyword extraction - can be improved with NLP
            words = text_content.lower().split()
            word_freq = {}
            stop_words = {'ve', 'ile', 'bir', 'bu', 'da', 'de', 'için', 'olan', 'olarak', 'the', 'and', 'or', 'for', 'with', 'as'}
            
            for word in words:
                word = re.sub(r'[^\w\s]', '', word)
                if len(word) > 3 and word not in stop_words:
                    word_freq[word] = word_freq.get(word, 0) + 1
            
            # Get top 20 keywords
            top_keywords = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)[:20]
            data['keywords'] = [kw[0] for kw in top_keywords]
            
        except Exception as e:
            self.logger.error(f"Error parsing page {url}: {e}")
        
        return data
    
    def create_company_from_data(self, data: Dict, url: str, metadata: Dict) -> Optional[UnifiedCompany]:
        """Create UnifiedCompany from parsed data"""
        try:
            if not data.get('company_name'):
                # Try to extract from domain
                parsed_url = urlparse(url)
                domain = parsed_url.netloc.replace('www.', '')
                data['company_name'] = domain.split('.')[0].title()
            
            company = UnifiedCompany(
                identity=CompanyIdentity(
                    legal_name=data.get('company_name', 'Unknown'),
                    country="TR"
                ),
                web_presence=WebPresence(
                    website_url=url,
                    website_status_code=metadata.get('status_code'),
                    ssl_issuer=metadata.get('headers', {}).get('server'),
                    social_links=data.get('social_links', {})
                ),
                contacts=ContactInfo(
                    emails_public=data.get('emails', []),
                    phones_public=data.get('phones', []),
                    address_public=data.get('address')
                ),
                business_meta=BusinessMeta(
                    keywords=data.get('keywords', [])
                ),
                seo_signals=SEOSignals(
                    title=data.get('title'),
                    meta_description=data.get('meta_description'),
                    h1_keywords=data.get('h1_keywords', [])
                ),
                provenance=[self.create_provenance(url, metadata)]
            )
            
            # Filter PII
            company_dict = company.dict()
            filtered_dict = self.filter_pii(company_dict)
            
            return UnifiedCompany(**filtered_dict)
            
        except Exception as e:
            self.logger.error(f"Error creating company from data: {e}")
            return None
    
    async def parse(self, content: str, metadata: Dict) -> List[Dict]:
        """Parse content into structured data"""
        data = await self.parse_page(content, metadata.get('url', ''), metadata)
        return [data] if data else []