"""
Unified data schemas for the marketing platform
"""

from datetime import datetime
from typing import Optional, List, Dict, Any
from enum import Enum
from pydantic import BaseModel, Field, EmailStr, HttpUrl, validator
import hashlib
import json


class CompanyType(str, Enum):
    """Turkish company types"""
    ANONIM = "Anonim Şirket"
    LIMITED = "Limited Şirket"
    SAHIS = "Şahıs Şirketi"
    KOLEKTIF = "Kolektif Şirket"
    KOMANDIT = "Komandit Şirket"
    KOOPERATIF = "Kooperatif"
    DERNEK = "Dernek"
    VAKIF = "Vakıf"
    OTHER = "Diğer"


class DataSource(str, Enum):
    """Data source types"""
    GOOGLE_SEARCH = "google_search"
    GOOGLE_PLACES = "google_places"
    LINKEDIN = "linkedin"
    INSTAGRAM = "instagram"
    FACEBOOK = "facebook"
    TWITTER = "twitter"
    WEBSITE = "website"
    WHOIS = "whois"
    TRADE_REGISTRY = "trade_registry"
    MANUAL = "manual"


class CompanyIdentity(BaseModel):
    """Company identity information"""
    legal_name: str = Field(..., description="Legal company name")
    trade_name: Optional[str] = Field(None, description="Trade/brand name")
    company_type: Optional[CompanyType] = None
    country: str = Field(default="TR", description="ISO country code")
    city: Optional[str] = None
    district: Optional[str] = None
    registration_hint: Optional[str] = Field(None, description="Registration number hints from public sources")
    
    @validator('legal_name')
    def normalize_name(cls, v):
        """Normalize company name for consistency"""
        if v:
            return ' '.join(v.strip().split())
        return v


class WebPresence(BaseModel):
    """Company web presence information"""
    website_url: Optional[HttpUrl] = None
    website_status_code: Optional[int] = None
    ssl_issuer: Optional[str] = None
    first_seen_ts: Optional[datetime] = None
    last_seen_ts: Optional[datetime] = None
    social_links: Dict[str, Optional[str]] = Field(default_factory=dict)
    google_places: Optional[Dict[str, Any]] = None


class ContactInfo(BaseModel):
    """Company contact information (corporate only)"""
    emails_public: List[EmailStr] = Field(default_factory=list)
    phones_public: List[str] = Field(default_factory=list)
    address_public: Optional[str] = None
    
    @validator('emails_public')
    def filter_corporate_emails(cls, v):
        """Filter out personal emails, keep only corporate ones"""
        corporate_prefixes = ['info', 'contact', 'sales', 'support', 'hello', 'admin', 'office']
        filtered = []
        for email in v:
            local_part = email.split('@')[0].lower()
            # Keep if it starts with corporate prefix or doesn't contain personal name patterns
            if any(local_part.startswith(prefix) for prefix in corporate_prefixes):
                filtered.append(email)
            elif not any(char.isdigit() for char in local_part) and '.' not in local_part:
                # Likely corporate, not firstname.lastname pattern
                filtered.append(email)
        return filtered


class BusinessMeta(BaseModel):
    """Business metadata"""
    industry_naics_guess: Optional[str] = None
    sic_guess: Optional[str] = None
    headcount_band_guess: Optional[str] = Field(None, description="e.g., 1-10, 11-50, 51-200")
    founding_year_guess: Optional[int] = None
    keywords: List[str] = Field(default_factory=list)


class SEOSignals(BaseModel):
    """SEO and web signals"""
    title: Optional[str] = None
    meta_description: Optional[str] = None
    h1_keywords: List[str] = Field(default_factory=list)
    indexed_pages_estimate: Optional[int] = None
    
    
class DataProvenance(BaseModel):
    """Data source and lineage tracking"""
    source_url: HttpUrl
    source_type: DataSource
    fetch_ts: datetime = Field(default_factory=datetime.utcnow)
    parser_version: str = "1.0.0"
    hash: Optional[str] = None
    
    def calculate_hash(self, data: dict) -> str:
        """Calculate hash of the data for change detection"""
        data_str = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(data_str.encode()).hexdigest()


class UnifiedCompany(BaseModel):
    """Unified company schema - the main entity"""
    id: Optional[str] = Field(None, description="Internal UUID")
    identity: CompanyIdentity
    web_presence: Optional[WebPresence] = None
    contacts: Optional[ContactInfo] = None
    business_meta: Optional[BusinessMeta] = None
    seo_signals: Optional[SEOSignals] = None
    provenance: List[DataProvenance] = Field(default_factory=list)
    
    # Deduplication fields
    canonical_id: Optional[str] = Field(None, description="ID of canonical record if this is a duplicate")
    confidence_score: float = Field(1.0, ge=0.0, le=1.0)
    
    # Compliance fields
    gdpr_suppressed: bool = False
    suppression_date: Optional[datetime] = None
    last_updated: datetime = Field(default_factory=datetime.utcnow)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class CompanyMatch(BaseModel):
    """Company matching result for deduplication"""
    company_a_id: str
    company_b_id: str
    match_score: float = Field(..., ge=0.0, le=1.0)
    match_fields: Dict[str, float] = Field(default_factory=dict)
    match_type: str = Field(..., description="exact, fuzzy, or potential")
    requires_review: bool = False
    reviewed_at: Optional[datetime] = None
    reviewed_by: Optional[str] = None
    
    
class CrawlJob(BaseModel):
    """Crawl job tracking"""
    job_id: str
    source: DataSource
    target_url: Optional[HttpUrl] = None
    search_query: Optional[str] = None
    status: str = Field(default="pending", description="pending, running, completed, failed")
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    records_found: int = 0
    errors: List[str] = Field(default_factory=list)
    rate_limit_hits: int = 0
    
    
class ComplianceLog(BaseModel):
    """GDPR/KVKK compliance audit log"""
    action: str = Field(..., description="deletion_request, data_export, consent_update")
    entity_id: str
    requester_info: Dict[str, Any]
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    status: str = Field(default="pending")
    completion_date: Optional[datetime] = None
    notes: Optional[str] = None