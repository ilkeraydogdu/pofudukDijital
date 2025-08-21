"""
WHOIS data enricher for domain information
"""

import asyncio
import re
from datetime import datetime
from typing import Dict, List, Optional

import whois
from dateutil import parser

from ..models.schemas import UnifiedCompany
from ..utils.compliance import ComplianceChecker


class WhoisEnricher:
    """Enrich company data with WHOIS information"""
    
    def __init__(self, rate_limit: int = 1):
        self.rate_limit = rate_limit
        self.compliance = ComplianceChecker()
        self.cache = {}
    
    async def enrich_company(self, company: UnifiedCompany) -> UnifiedCompany:
        """Enrich a single company with WHOIS data"""
        if not company.web_presence or not company.web_presence.website_url:
            return company
        
        try:
            # Extract domain from URL
            url = str(company.web_presence.website_url)
            domain = self._extract_domain(url)
            
            if not domain:
                return company
            
            # Check cache
            if domain in self.cache:
                whois_data = self.cache[domain]
            else:
                # Fetch WHOIS data
                whois_data = await self._fetch_whois(domain)
                self.cache[domain] = whois_data
            
            if whois_data:
                # Update company with WHOIS data
                company = self._update_company_with_whois(company, whois_data)
        
        except Exception as e:
            print(f"Error enriching company with WHOIS: {e}")
        
        return company
    
    async def enrich_batch(self, companies: List[UnifiedCompany]) -> List[UnifiedCompany]:
        """Enrich a batch of companies"""
        enriched = []
        
        for company in companies:
            enriched_company = await self.enrich_company(company)
            enriched.append(enriched_company)
            
            # Rate limiting
            await asyncio.sleep(1 / self.rate_limit)
        
        return enriched
    
    def _extract_domain(self, url: str) -> Optional[str]:
        """Extract domain from URL"""
        try:
            # Remove protocol
            domain = re.sub(r'^https?://', '', url)
            # Remove path
            domain = domain.split('/')[0]
            # Remove www
            domain = domain.replace('www.', '')
            # Remove port
            domain = domain.split(':')[0]
            
            return domain
        except:
            return None
    
    async def _fetch_whois(self, domain: str) -> Optional[Dict]:
        """Fetch WHOIS data for domain"""
        try:
            # Run in executor since whois is blocking
            loop = asyncio.get_event_loop()
            whois_data = await loop.run_in_executor(None, whois.whois, domain)
            
            # Convert to dict if needed
            if hasattr(whois_data, '__dict__'):
                whois_data = whois_data.__dict__
            
            return whois_data
        except Exception as e:
            print(f"Error fetching WHOIS for {domain}: {e}")
            return None
    
    def _update_company_with_whois(
        self,
        company: UnifiedCompany,
        whois_data: Dict
    ) -> UnifiedCompany:
        """Update company with WHOIS data"""
        
        # Extract organization name (filter PII)
        org_name = whois_data.get('org') or whois_data.get('organization')
        if org_name and not self.compliance.is_personal_name(org_name):
            if not company.identity.trade_name:
                company.identity.trade_name = org_name
        
        # Extract creation date for founding year guess
        creation_date = whois_data.get('creation_date')
        if creation_date:
            if isinstance(creation_date, list):
                creation_date = creation_date[0]
            
            if isinstance(creation_date, str):
                try:
                    creation_date = parser.parse(creation_date)
                except:
                    pass
            
            if isinstance(creation_date, datetime):
                founding_year = creation_date.year
                if company.business_meta:
                    if not company.business_meta.founding_year_guess:
                        company.business_meta.founding_year_guess = founding_year
        
        # Extract country
        country = whois_data.get('country')
        if country and not company.identity.country:
            # Map to ISO code
            country_map = {
                'turkey': 'TR',
                'türkiye': 'TR',
                'united states': 'US',
                'united kingdom': 'GB',
                'germany': 'DE',
                'france': 'FR',
            }
            country_lower = country.lower()
            for key, code in country_map.items():
                if key in country_lower:
                    company.identity.country = code
                    break
        
        # Extract city
        city = whois_data.get('city')
        if city and not company.identity.city:
            company.identity.city = city
        
        # Extract emails (corporate only)
        emails = whois_data.get('emails')
        if emails:
            if isinstance(emails, str):
                emails = [emails]
            
            corporate_emails = []
            for email in emails:
                if self.compliance.is_corporate_email(email):
                    corporate_emails.append(email.lower())
            
            if corporate_emails and company.contacts:
                existing_emails = set(company.contacts.emails_public)
                existing_emails.update(corporate_emails)
                company.contacts.emails_public = list(existing_emails)
        
        # Update SSL information
        if company.web_presence:
            company.web_presence.ssl_issuer = whois_data.get('registrar')
        
        return company