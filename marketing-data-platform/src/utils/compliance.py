"""
GDPR/KVKK compliance utilities
"""

import hashlib
import re
from typing import Any, Dict, List, Optional


class ComplianceChecker:
    """Check and enforce GDPR/KVKK compliance"""
    
    def __init__(self):
        # Personal name patterns
        self.personal_name_patterns = [
            r'\b[A-Z][a-z]+\s+[A-Z][a-z]+\b',  # Firstname Lastname
            r'\b[A-Z]\.\s*[A-Z][a-z]+\b',  # F. Lastname
            r'\b[A-Z][a-z]+\s+[A-Z]\.\b',  # Firstname L.
        ]
        
        # Corporate email prefixes
        self.corporate_prefixes = [
            'info', 'contact', 'sales', 'support', 'hello', 'admin',
            'office', 'inquiry', 'service', 'help', 'team', 'hr',
            'career', 'job', 'press', 'media', 'partner', 'business'
        ]
        
        # PII field patterns
        self.pii_patterns = {
            'tc_kimlik': r'\b\d{11}\b',  # Turkish ID number
            'credit_card': r'\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b',
            'iban': r'\b[A-Z]{2}\d{2}[A-Z0-9]{4}\d{7}([A-Z0-9]?){0,16}\b',
            'passport': r'\b[A-Z][0-9]{8}\b',
        }
        
        # Suppression list (for RTBF requests)
        self.suppression_list = set()
    
    def is_personal_name(self, text: str) -> bool:
        """Check if text appears to be a personal name"""
        if not text:
            return False
        
        # Check against patterns
        for pattern in self.personal_name_patterns:
            if re.search(pattern, text):
                # Additional checks to reduce false positives
                words = text.split()
                if len(words) == 2:
                    # Check if both words are capitalized (likely personal name)
                    if all(word[0].isupper() for word in words):
                        # Check if not a company name
                        company_keywords = ['Ltd', 'Inc', 'Corp', 'Company', 'Group', 'A.Ş.', 'Ltd.Şti.']
                        if not any(keyword in text for keyword in company_keywords):
                            return True
        
        return False
    
    def is_corporate_email(self, email: str) -> bool:
        """Check if email is corporate (not personal)"""
        if not email or '@' not in email:
            return False
        
        local_part = email.split('@')[0].lower()
        
        # Check if starts with corporate prefix
        for prefix in self.corporate_prefixes:
            if local_part.startswith(prefix):
                return True
        
        # Check if contains personal name pattern (firstname.lastname)
        if '.' in local_part:
            parts = local_part.split('.')
            if len(parts) == 2 and all(part.isalpha() for part in parts):
                # Likely personal email
                return False
        
        # Check if contains numbers (often personal)
        if any(char.isdigit() for char in local_part):
            return False
        
        return True
    
    def detect_pii(self, text: str) -> List[str]:
        """Detect potential PII in text"""
        detected = []
        
        for pii_type, pattern in self.pii_patterns.items():
            if re.search(pattern, text, re.IGNORECASE):
                detected.append(pii_type)
        
        # Check for personal names
        if self.is_personal_name(text):
            detected.append('personal_name')
        
        return detected
    
    def mask_pii(self, text: str) -> str:
        """Mask PII in text"""
        masked = text
        
        # Mask patterns
        for pii_type, pattern in self.pii_patterns.items():
            masked = re.sub(pattern, '[REDACTED]', masked, flags=re.IGNORECASE)
        
        # Mask email addresses that appear personal
        email_pattern = r'\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b'
        for email in re.findall(email_pattern, masked):
            if not self.is_corporate_email(email):
                masked = masked.replace(email, '[EMAIL_REDACTED]')
        
        return masked
    
    def filter_pii(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Filter PII from data dictionary"""
        filtered = {}
        
        for key, value in data.items():
            if value is None:
                filtered[key] = value
                continue
            
            if isinstance(value, str):
                # Check if value contains PII
                if self.detect_pii(value):
                    filtered[key] = self.mask_pii(value)
                else:
                    filtered[key] = value
            elif isinstance(value, dict):
                filtered[key] = self.filter_pii(value)
            elif isinstance(value, list):
                filtered[key] = []
                for item in value:
                    if isinstance(item, dict):
                        filtered[key].append(self.filter_pii(item))
                    elif isinstance(item, str):
                        if not self.detect_pii(item):
                            filtered[key].append(item)
                    else:
                        filtered[key].append(item)
            else:
                filtered[key] = value
        
        return filtered
    
    def hash_identifier(self, identifier: str) -> str:
        """Create anonymized hash of identifier"""
        return hashlib.sha256(identifier.encode()).hexdigest()
    
    def add_to_suppression(self, identifier: str):
        """Add identifier to suppression list (RTBF)"""
        self.suppression_list.add(self.hash_identifier(identifier))
    
    def is_suppressed(self, identifier: str) -> bool:
        """Check if identifier is in suppression list"""
        return self.hash_identifier(identifier) in self.suppression_list
    
    def check_data_minimization(self, data: Dict[str, Any]) -> Dict[str, str]:
        """Check data minimization compliance"""
        issues = {}
        
        # Check for unnecessary personal data
        if 'personal_emails' in data and data.get('personal_emails'):
            issues['personal_emails'] = "Personal emails should not be collected"
        
        if 'personal_phones' in data and data.get('personal_phones'):
            issues['personal_phones'] = "Personal phone numbers should not be collected"
        
        # Check for sensitive data
        sensitive_fields = ['religion', 'political_views', 'health_data', 'biometric_data']
        for field in sensitive_fields:
            if field in data:
                issues[field] = f"Sensitive data field '{field}' should not be collected"
        
        return issues
    
    def generate_compliance_report(self, companies: List[Dict]) -> Dict:
        """Generate compliance report for collected data"""
        report = {
            'total_records': len(companies),
            'pii_detected': 0,
            'suppressed_records': 0,
            'data_minimization_issues': [],
            'recommendations': []
        }
        
        for company in companies:
            # Check for PII
            company_str = str(company)
            if self.detect_pii(company_str):
                report['pii_detected'] += 1
            
            # Check suppression
            if company.get('id') and self.is_suppressed(company['id']):
                report['suppressed_records'] += 1
            
            # Check data minimization
            issues = self.check_data_minimization(company)
            if issues:
                report['data_minimization_issues'].append({
                    'company_id': company.get('id'),
                    'issues': issues
                })
        
        # Add recommendations
        if report['pii_detected'] > 0:
            report['recommendations'].append(
                f"Review and remove PII from {report['pii_detected']} records"
            )
        
        if report['data_minimization_issues']:
            report['recommendations'].append(
                "Review data collection to ensure only necessary business data is collected"
            )
        
        return report