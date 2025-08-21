"""
Base collector class with rate limiting, caching, and compliance features
"""

import asyncio
import hashlib
import json
import time
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import httpx
import structlog
from diskcache import Cache
from fake_useragent import UserAgent
from pyrate_limiter import Duration, Limiter, RequestRate
from tenacity import retry, stop_after_attempt, wait_exponential

from ..models.schemas import DataProvenance, DataSource, UnifiedCompany
from ..utils.compliance import ComplianceChecker

logger = structlog.get_logger()


class BaseCollector(ABC):
    """Base class for all data collectors"""
    
    def __init__(
        self,
        source_type: DataSource,
        rate_limit: int = 2,
        cache_ttl: int = 86400,  # 24 hours
        respect_robots: bool = True,
        user_agent: Optional[str] = None
    ):
        self.source_type = source_type
        self.rate_limit = rate_limit
        self.cache_ttl = cache_ttl
        self.respect_robots = respect_robots
        
        # Set up user agent
        if user_agent:
            self.user_agent = user_agent
        else:
            ua = UserAgent()
            self.user_agent = f"MarketingPlatform/1.0 (compatible; {ua.random})"
        
        # Set up rate limiter
        self.limiter = Limiter(
            RequestRate(rate_limit, Duration.SECOND)
        )
        
        # Set up cache
        self.cache = Cache(f'.cache/{source_type.value}')
        
        # Set up HTTP client
        self.client = httpx.AsyncClient(
            headers={'User-Agent': self.user_agent},
            timeout=30.0,
            follow_redirects=True
        )
        
        # Compliance checker
        self.compliance = ComplianceChecker()
        
        # Robots.txt parsers cache
        self.robots_cache: Dict[str, RobotFileParser] = {}
        
        self.logger = logger.bind(collector=self.__class__.__name__)
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()
    
    def _get_cache_key(self, url: str, params: Optional[Dict] = None) -> str:
        """Generate cache key for URL and parameters"""
        key_data = {'url': url}
        if params:
            key_data['params'] = params
        key_str = json.dumps(key_data, sort_keys=True)
        return hashlib.md5(key_str.encode()).hexdigest()
    
    async def _check_robots_txt(self, url: str) -> bool:
        """Check if URL is allowed by robots.txt"""
        if not self.respect_robots:
            return True
        
        parsed_url = urlparse(url)
        robots_url = f"{parsed_url.scheme}://{parsed_url.netloc}/robots.txt"
        
        if robots_url not in self.robots_cache:
            try:
                rp = RobotFileParser()
                rp.set_url(robots_url)
                
                # Fetch robots.txt
                response = await self.client.get(robots_url)
                if response.status_code == 200:
                    rp.parse(response.text.splitlines())
                else:
                    # No robots.txt means allow all
                    return True
                
                self.robots_cache[robots_url] = rp
            except Exception as e:
                self.logger.warning(f"Failed to fetch robots.txt: {e}")
                # Be conservative - allow if can't fetch
                return True
        
        rp = self.robots_cache[robots_url]
        can_fetch = rp.can_fetch(self.user_agent, url)
        
        # Check crawl delay
        delay = rp.crawl_delay(self.user_agent)
        if delay:
            await asyncio.sleep(delay)
        
        return can_fetch
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    async def fetch(
        self,
        url: str,
        params: Optional[Dict] = None,
        use_cache: bool = True
    ) -> Tuple[Optional[str], Dict[str, Any]]:
        """Fetch URL with rate limiting, caching, and retries"""
        
        # Check robots.txt
        if not await self._check_robots_txt(url):
            self.logger.warning(f"Blocked by robots.txt: {url}")
            return None, {"error": "Blocked by robots.txt"}
        
        # Check cache
        cache_key = self._get_cache_key(url, params)
        if use_cache and cache_key in self.cache:
            cached_data = self.cache[cache_key]
            if cached_data['timestamp'] > time.time() - self.cache_ttl:
                self.logger.debug(f"Cache hit for {url}")
                return cached_data['content'], cached_data['metadata']
        
        # Rate limiting
        self.limiter.try_acquire(url)
        
        # Fetch
        try:
            self.logger.info(f"Fetching {url}")
            response = await self.client.get(url, params=params)
            response.raise_for_status()
            
            content = response.text
            metadata = {
                'status_code': response.status_code,
                'headers': dict(response.headers),
                'fetch_time': datetime.utcnow().isoformat(),
                'url': str(response.url)
            }
            
            # Cache the result
            if use_cache:
                self.cache[cache_key] = {
                    'content': content,
                    'metadata': metadata,
                    'timestamp': time.time()
                }
            
            return content, metadata
            
        except httpx.HTTPStatusError as e:
            self.logger.error(f"HTTP error fetching {url}: {e}")
            return None, {"error": str(e), "status_code": e.response.status_code}
        except Exception as e:
            self.logger.error(f"Error fetching {url}: {e}")
            return None, {"error": str(e)}
    
    def create_provenance(self, source_url: str, metadata: Dict) -> DataProvenance:
        """Create provenance record for collected data"""
        return DataProvenance(
            source_url=source_url,
            source_type=self.source_type,
            fetch_ts=datetime.utcnow(),
            parser_version="1.0.0",
            hash=hashlib.md5(json.dumps(metadata, sort_keys=True).encode()).hexdigest()
        )
    
    @abstractmethod
    async def collect(self, query: str, **kwargs) -> List[UnifiedCompany]:
        """Collect data based on query - must be implemented by subclasses"""
        pass
    
    @abstractmethod
    async def parse(self, content: str, metadata: Dict) -> List[Dict]:
        """Parse content into structured data - must be implemented by subclasses"""
        pass
    
    def filter_pii(self, data: Dict) -> Dict:
        """Filter out PII from collected data"""
        return self.compliance.filter_pii(data)
    
    async def close(self):
        """Clean up resources"""
        await self.client.aclose()
        self.cache.close()