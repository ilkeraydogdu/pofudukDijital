"""
Entity resolution and deduplication module
"""

import hashlib
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from fuzzywuzzy import fuzz
from recordlinkage import Index, Compare
from recordlinkage.preprocessing import clean

from ..models.schemas import CompanyMatch, UnifiedCompany
from ..normalizers.company_normalizer import CompanyNormalizer


class EntityResolver:
    """Resolve and deduplicate company entities"""
    
    def __init__(
        self,
        exact_threshold: float = 0.95,
        review_threshold: float = 0.85,
        min_threshold: float = 0.70
    ):
        self.exact_threshold = exact_threshold
        self.review_threshold = review_threshold
        self.min_threshold = min_threshold
        self.normalizer = CompanyNormalizer()
        
        # Field weights for matching
        self.field_weights = {
            'domain': 0.35,
            'legal_name': 0.25,
            'phone': 0.15,
            'email': 0.15,
            'address': 0.10
        }
    
    def create_blocking_key(self, company: UnifiedCompany) -> str:
        """Create blocking key for initial candidate selection"""
        keys = []
        
        # Use domain as primary key
        if company.web_presence and company.web_presence.website_url:
            domain = str(company.web_presence.website_url).split('/')[2]
            domain = domain.replace('www.', '')
            keys.append(f"domain:{domain}")
        
        # Use normalized name prefix
        if company.identity:
            name_normalized = self.normalizer.normalize_for_matching(
                company.identity.legal_name
            )
            if len(name_normalized) >= 3:
                keys.append(f"name:{name_normalized[:3]}")
        
        # Use phone prefix
        if company.contacts and company.contacts.phones_public:
            for phone in company.contacts.phones_public[:1]:  # First phone only
                phone_normalized = self.normalizer.normalize_phone(phone)
                if len(phone_normalized) >= 6:
                    keys.append(f"phone:{phone_normalized[:6]}")
        
        # Use city
        if company.identity and company.identity.city:
            keys.append(f"city:{company.identity.city.upper()}")
        
        return '|'.join(keys) if keys else 'unknown'
    
    def calculate_field_similarity(
        self,
        value1: Optional[str],
        value2: Optional[str],
        method: str = 'fuzzy'
    ) -> float:
        """Calculate similarity between two field values"""
        if not value1 or not value2:
            return 0.0
        
        if method == 'exact':
            return 1.0 if value1.lower() == value2.lower() else 0.0
        elif method == 'fuzzy':
            return fuzz.ratio(value1.lower(), value2.lower()) / 100.0
        elif method == 'token':
            return fuzz.token_sort_ratio(value1.lower(), value2.lower()) / 100.0
        else:
            return 0.0
    
    def calculate_company_similarity(
        self,
        company1: UnifiedCompany,
        company2: UnifiedCompany
    ) -> Tuple[float, Dict[str, float]]:
        """Calculate overall similarity between two companies"""
        field_scores = {}
        
        # Domain similarity (exact match preferred)
        if (company1.web_presence and company1.web_presence.website_url and
            company2.web_presence and company2.web_presence.website_url):
            domain1 = str(company1.web_presence.website_url).split('/')[2].replace('www.', '')
            domain2 = str(company2.web_presence.website_url).split('/')[2].replace('www.', '')
            field_scores['domain'] = self.calculate_field_similarity(domain1, domain2, 'exact')
        else:
            field_scores['domain'] = 0.0
        
        # Name similarity (fuzzy match)
        if company1.identity and company2.identity:
            name1 = self.normalizer.normalize_company_name(company1.identity.legal_name)
            name2 = self.normalizer.normalize_company_name(company2.identity.legal_name)
            field_scores['legal_name'] = self.calculate_field_similarity(name1, name2, 'token')
        else:
            field_scores['legal_name'] = 0.0
        
        # Phone similarity
        phone_scores = []
        if company1.contacts and company2.contacts:
            for phone1 in company1.contacts.phones_public:
                for phone2 in company2.contacts.phones_public:
                    phone1_norm = self.normalizer.normalize_phone(phone1)
                    phone2_norm = self.normalizer.normalize_phone(phone2)
                    if phone1_norm and phone2_norm:
                        score = self.calculate_field_similarity(phone1_norm, phone2_norm, 'exact')
                        phone_scores.append(score)
        field_scores['phone'] = max(phone_scores) if phone_scores else 0.0
        
        # Email similarity
        email_scores = []
        if company1.contacts and company2.contacts:
            for email1 in company1.contacts.emails_public:
                for email2 in company2.contacts.emails_public:
                    email1_norm = self.normalizer.normalize_email(email1)
                    email2_norm = self.normalizer.normalize_email(email2)
                    if email1_norm and email2_norm:
                        # Check domain match
                        domain1 = email1_norm.split('@')[1]
                        domain2 = email2_norm.split('@')[1]
                        if domain1 == domain2:
                            email_scores.append(1.0)
                        else:
                            email_scores.append(0.0)
        field_scores['email'] = max(email_scores) if email_scores else 0.0
        
        # Address similarity
        if (company1.contacts and company1.contacts.address_public and
            company2.contacts and company2.contacts.address_public):
            addr1 = self.normalizer.normalize_address(company1.contacts.address_public)
            addr2 = self.normalizer.normalize_address(company2.contacts.address_public)
            field_scores['address'] = self.calculate_field_similarity(addr1, addr2, 'fuzzy')
        else:
            field_scores['address'] = 0.0
        
        # Calculate weighted average
        total_score = 0.0
        total_weight = 0.0
        
        for field, score in field_scores.items():
            weight = self.field_weights.get(field, 0.0)
            total_score += score * weight
            total_weight += weight
        
        final_score = total_score / total_weight if total_weight > 0 else 0.0
        
        return final_score, field_scores
    
    def find_duplicates(
        self,
        companies: List[UnifiedCompany]
    ) -> List[CompanyMatch]:
        """Find duplicate companies in a list"""
        matches = []
        
        # Create blocking index for efficiency
        blocking_index = {}
        for i, company in enumerate(companies):
            blocking_key = self.create_blocking_key(company)
            if blocking_key not in blocking_index:
                blocking_index[blocking_key] = []
            blocking_index[blocking_key].append((i, company))
        
        # Compare companies within same blocks
        processed_pairs = set()
        
        for block_key, block_companies in blocking_index.items():
            for i, (idx1, company1) in enumerate(block_companies):
                for idx2, company2 in block_companies[i+1:]:
                    # Skip if already processed
                    pair_key = tuple(sorted([idx1, idx2]))
                    if pair_key in processed_pairs:
                        continue
                    processed_pairs.add(pair_key)
                    
                    # Calculate similarity
                    score, field_scores = self.calculate_company_similarity(company1, company2)
                    
                    # Only create match if above minimum threshold
                    if score >= self.min_threshold:
                        match_type = 'exact' if score >= self.exact_threshold else 'fuzzy'
                        requires_review = self.review_threshold <= score < self.exact_threshold
                        
                        match = CompanyMatch(
                            company_a_id=company1.id or str(idx1),
                            company_b_id=company2.id or str(idx2),
                            match_score=score,
                            match_fields=field_scores,
                            match_type=match_type,
                            requires_review=requires_review
                        )
                        matches.append(match)
        
        return matches
    
    def merge_companies(
        self,
        company1: UnifiedCompany,
        company2: UnifiedCompany
    ) -> UnifiedCompany:
        """Merge two duplicate companies into one"""
        # Use company1 as base, fill missing fields from company2
        merged = company1.copy(deep=True)
        
        # Merge identity
        if company2.identity:
            if not merged.identity.trade_name and company2.identity.trade_name:
                merged.identity.trade_name = company2.identity.trade_name
            if not merged.identity.company_type and company2.identity.company_type:
                merged.identity.company_type = company2.identity.company_type
            if not merged.identity.city and company2.identity.city:
                merged.identity.city = company2.identity.city
            if not merged.identity.district and company2.identity.district:
                merged.identity.district = company2.identity.district
        
        # Merge web presence
        if company2.web_presence:
            if not merged.web_presence:
                merged.web_presence = company2.web_presence
            else:
                if not merged.web_presence.website_url and company2.web_presence.website_url:
                    merged.web_presence.website_url = company2.web_presence.website_url
                
                # Merge social links
                if company2.web_presence.social_links:
                    if not merged.web_presence.social_links:
                        merged.web_presence.social_links = {}
                    merged.web_presence.social_links.update(company2.web_presence.social_links)
                
                # Merge Google Places data
                if company2.web_presence.google_places and not merged.web_presence.google_places:
                    merged.web_presence.google_places = company2.web_presence.google_places
        
        # Merge contacts
        if company2.contacts:
            if not merged.contacts:
                merged.contacts = company2.contacts
            else:
                # Merge emails (unique)
                all_emails = set(merged.contacts.emails_public)
                all_emails.update(company2.contacts.emails_public)
                merged.contacts.emails_public = list(all_emails)
                
                # Merge phones (unique)
                all_phones = set(merged.contacts.phones_public)
                all_phones.update(company2.contacts.phones_public)
                merged.contacts.phones_public = list(all_phones)
                
                # Use longer address
                if company2.contacts.address_public:
                    if not merged.contacts.address_public:
                        merged.contacts.address_public = company2.contacts.address_public
                    elif len(company2.contacts.address_public) > len(merged.contacts.address_public):
                        merged.contacts.address_public = company2.contacts.address_public
        
        # Merge business meta
        if company2.business_meta:
            if not merged.business_meta:
                merged.business_meta = company2.business_meta
            else:
                # Merge keywords (unique)
                all_keywords = set(merged.business_meta.keywords)
                all_keywords.update(company2.business_meta.keywords)
                merged.business_meta.keywords = list(all_keywords)[:50]  # Limit to 50
                
                # Use non-null values
                for field in ['industry_naics_guess', 'sic_guess', 'headcount_band_guess', 'founding_year_guess']:
                    if not getattr(merged.business_meta, field) and getattr(company2.business_meta, field):
                        setattr(merged.business_meta, field, getattr(company2.business_meta, field))
        
        # Merge provenance (keep all sources)
        merged.provenance.extend(company2.provenance)
        
        # Update confidence score (average)
        merged.confidence_score = (merged.confidence_score + company2.confidence_score) / 2
        
        return merged
    
    def resolve_duplicates(
        self,
        companies: List[UnifiedCompany],
        auto_merge: bool = True
    ) -> Tuple[List[UnifiedCompany], List[CompanyMatch]]:
        """Resolve duplicates in company list"""
        # Find duplicates
        matches = self.find_duplicates(companies)
        
        if not matches or not auto_merge:
            return companies, matches
        
        # Build merge groups
        merge_groups = {}
        for match in matches:
            if match.match_score >= self.exact_threshold and not match.requires_review:
                # Add to merge group
                id_a = match.company_a_id
                id_b = match.company_b_id
                
                # Find existing groups
                group_a = merge_groups.get(id_a)
                group_b = merge_groups.get(id_b)
                
                if group_a and group_b:
                    # Merge two groups
                    if group_a != group_b:
                        for company_id in merge_groups:
                            if merge_groups[company_id] == group_b:
                                merge_groups[company_id] = group_a
                elif group_a:
                    merge_groups[id_b] = group_a
                elif group_b:
                    merge_groups[id_a] = group_b
                else:
                    # Create new group
                    new_group = max(merge_groups.values(), default=-1) + 1
                    merge_groups[id_a] = new_group
                    merge_groups[id_b] = new_group
        
        # Create company index
        company_index = {company.id or str(i): company for i, company in enumerate(companies)}
        
        # Merge companies in same group
        merged_companies = {}
        for company_id, group_id in merge_groups.items():
            if group_id not in merged_companies:
                merged_companies[group_id] = company_index[company_id]
            else:
                merged_companies[group_id] = self.merge_companies(
                    merged_companies[group_id],
                    company_index[company_id]
                )
        
        # Add non-duplicate companies
        result_companies = list(merged_companies.values())
        for i, company in enumerate(companies):
            company_id = company.id or str(i)
            if company_id not in merge_groups:
                result_companies.append(company)
        
        return result_companies, matches