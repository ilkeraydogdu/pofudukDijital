"""
Google Search and Google Places API collectors
"""

import asyncio
import os
from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus

import googlemaps
from bs4 import BeautifulSoup
from googleapiclient.discovery import build

from ..models.schemas import (
    BusinessMeta,
    CompanyIdentity,
    CompanyType,
    ContactInfo,
    DataSource,
    UnifiedCompany,
    WebPresence,
)
from .base_collector import BaseCollector


class GoogleSearchCollector(BaseCollector):
    """Collector for Google Custom Search API"""
    
    def __init__(self, api_key: str, cse_id: str, **kwargs):
        super().__init__(DataSource.GOOGLE_SEARCH, **kwargs)
        self.api_key = api_key
        self.cse_id = cse_id
        self.service = build("customsearch", "v1", developerKey=api_key)
    
    async def collect(self, query: str, **kwargs) -> List[UnifiedCompany]:
        """Collect companies from Google Search"""
        companies = []
        
        # Define search queries for different sources
        search_queries = [
            f'site:linkedin.com/company/ {query}',
            f'site:tr.linkedin.com/company/ {query}',
            f'site:crunchbase.com/organization/ {query}',
            f'site:instagram.com {query} business',
            f'site:facebook.com {query} about',
            f'{query} "anonim şirket" OR "limited şirket"',
        ]
        
        for search_query in search_queries:
            try:
                # Use Google Custom Search API
                result = await self._search_google(search_query)
                
                for item in result.get('items', []):
                    company_data = await self.parse_search_result(item)
                    if company_data:
                        companies.append(company_data)
                
                # Rate limiting between queries
                await asyncio.sleep(1)
                
            except Exception as e:
                self.logger.error(f"Error searching Google for '{search_query}': {e}")
        
        return companies
    
    async def _search_google(self, query: str, num_results: int = 10) -> Dict:
        """Execute Google Custom Search API request"""
        try:
            # Run in executor since it's a blocking call
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                lambda: self.service.cse().list(
                    q=query,
                    cx=self.cse_id,
                    num=num_results
                ).execute()
            )
            return result
        except Exception as e:
            self.logger.error(f"Google CSE API error: {e}")
            return {}
    
    async def parse_search_result(self, item: Dict) -> Optional[UnifiedCompany]:
        """Parse a single search result into UnifiedCompany"""
        try:
            title = item.get('title', '')
            link = item.get('link', '')
            snippet = item.get('snippet', '')
            
            # Extract company name from title
            company_name = title.split('|')[0].strip()
            company_name = company_name.replace(' - LinkedIn', '').strip()
            company_name = company_name.replace(' | Crunchbase', '').strip()
            
            # Determine company type from snippet
            company_type = None
            if 'anonim şirket' in snippet.lower():
                company_type = CompanyType.ANONIM
            elif 'limited şirket' in snippet.lower():
                company_type = CompanyType.LIMITED
            
            # Extract location if available
            city = None
            if 'Istanbul' in snippet or 'İstanbul' in snippet:
                city = 'İstanbul'
            elif 'Ankara' in snippet:
                city = 'Ankara'
            elif 'Izmir' in snippet or 'İzmir' in snippet:
                city = 'İzmir'
            
            # Create company object
            company = UnifiedCompany(
                identity=CompanyIdentity(
                    legal_name=company_name,
                    company_type=company_type,
                    city=city,
                    country="TR"
                ),
                web_presence=WebPresence(
                    social_links={'source_url': link}
                ),
                business_meta=BusinessMeta(
                    keywords=snippet.split()[:10]  # First 10 words as keywords
                ),
                provenance=[self.create_provenance(link, {'snippet': snippet})]
            )
            
            return company
            
        except Exception as e:
            self.logger.error(f"Error parsing search result: {e}")
            return None
    
    async def parse(self, content: str, metadata: Dict) -> List[Dict]:
        """Parse HTML content (not used for API-based collection)"""
        return []


class GooglePlacesCollector(BaseCollector):
    """Collector for Google Places API"""
    
    def __init__(self, api_key: str, **kwargs):
        super().__init__(DataSource.GOOGLE_PLACES, **kwargs)
        self.api_key = api_key
        self.gmaps = googlemaps.Client(key=api_key)
    
    async def collect(self, query: str, location: Optional[str] = None, **kwargs) -> List[UnifiedCompany]:
        """Collect business data from Google Places"""
        companies = []
        
        try:
            # Search for places
            if location:
                places_result = await self._search_places(f"{query} {location}")
            else:
                places_result = await self._search_places(query)
            
            for place in places_result.get('results', []):
                company = await self._parse_place(place)
                if company:
                    companies.append(company)
            
            # Handle pagination if next_page_token exists
            next_token = places_result.get('next_page_token')
            if next_token:
                await asyncio.sleep(2)  # Required delay for next page
                next_results = await self._get_next_page(next_token)
                for place in next_results.get('results', []):
                    company = await self._parse_place(place)
                    if company:
                        companies.append(company)
        
        except Exception as e:
            self.logger.error(f"Error collecting from Google Places: {e}")
        
        return companies
    
    async def _search_places(self, query: str) -> Dict:
        """Search places using Google Places API"""
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                lambda: self.gmaps.places(
                    query=query,
                    language='tr',
                    region='tr'
                )
            )
            return result
        except Exception as e:
            self.logger.error(f"Google Places API error: {e}")
            return {}
    
    async def _get_next_page(self, page_token: str) -> Dict:
        """Get next page of results"""
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                lambda: self.gmaps.places(page_token=page_token)
            )
            return result
        except Exception as e:
            self.logger.error(f"Google Places next page error: {e}")
            return {}
    
    async def _get_place_details(self, place_id: str) -> Dict:
        """Get detailed information about a place"""
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                lambda: self.gmaps.place(
                    place_id=place_id,
                    fields=[
                        'name', 'formatted_address', 'formatted_phone_number',
                        'website', 'rating', 'user_ratings_total', 'types',
                        'opening_hours', 'business_status'
                    ],
                    language='tr'
                )
            )
            return result.get('result', {})
        except Exception as e:
            self.logger.error(f"Google Places details error: {e}")
            return {}
    
    async def _parse_place(self, place: Dict) -> Optional[UnifiedCompany]:
        """Parse Google Place into UnifiedCompany"""
        try:
            place_id = place.get('place_id')
            if not place_id:
                return None
            
            # Get detailed information
            details = await self._get_place_details(place_id)
            if not details:
                details = place
            
            # Extract company information
            name = details.get('name', '')
            address = details.get('formatted_address', '')
            phone = details.get('formatted_phone_number', '')
            website = details.get('website', '')
            rating = details.get('rating')
            reviews_count = details.get('user_ratings_total', 0)
            types = details.get('types', [])
            
            # Parse address to get city
            city = None
            if address:
                parts = address.split(',')
                if len(parts) >= 2:
                    city = parts[-2].strip()
            
            # Create company object
            company = UnifiedCompany(
                identity=CompanyIdentity(
                    legal_name=name,
                    trade_name=name,
                    city=city,
                    country="TR"
                ),
                web_presence=WebPresence(
                    website_url=website if website else None,
                    google_places={
                        'place_id': place_id,
                        'rating': rating,
                        'reviews_count': reviews_count,
                        'types': types,
                        'business_status': details.get('business_status', 'OPERATIONAL')
                    }
                ),
                contacts=ContactInfo(
                    phones_public=[phone] if phone else [],
                    address_public=address
                ),
                business_meta=BusinessMeta(
                    keywords=types
                ),
                provenance=[self.create_provenance(
                    f"https://maps.google.com/maps/place/?q=place_id:{place_id}",
                    {'place_id': place_id}
                )]
            )
            
            # Filter PII
            company_dict = company.dict()
            filtered_dict = self.filter_pii(company_dict)
            
            return UnifiedCompany(**filtered_dict)
            
        except Exception as e:
            self.logger.error(f"Error parsing place: {e}")
            return None
    
    async def parse(self, content: str, metadata: Dict) -> List[Dict]:
        """Parse content (not used for API-based collection)"""
        return []