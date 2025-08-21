"""
Airflow DAG for company data ETL pipeline
"""

from datetime import datetime, timedelta
from typing import Dict, List

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

# Default arguments for the DAG
default_args = {
    'owner': 'marketing-platform',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'company_data_etl',
    default_args=default_args,
    description='ETL pipeline for company data collection and processing',
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'companies', 'marketing'],
)


def discover_companies(**context):
    """Discover new companies to collect"""
    import asyncio
    from src.collectors.google_collector import GoogleSearchCollector
    
    # Get search queries from config or generate
    queries = [
        'teknoloji şirketi istanbul',
        'yazılım firması ankara',
        'e-ticaret şirketi türkiye',
        'startup türkiye',
        'bilişim limited şirketi',
    ]
    
    companies = []
    collector = GoogleSearchCollector(
        api_key=context['params']['google_api_key'],
        cse_id=context['params']['google_cse_id']
    )
    
    async def collect_all():
        for query in queries:
            results = await collector.collect(query)
            companies.extend(results)
    
    asyncio.run(collect_all())
    
    # Push to XCom
    context['task_instance'].xcom_push(key='discovered_companies', value=companies)
    return f"Discovered {len(companies)} companies"


def fetch_company_data(**context):
    """Fetch detailed data for discovered companies"""
    import asyncio
    from src.collectors.google_collector import GooglePlacesCollector
    from src.collectors.website_collector import WebsiteCollector
    
    # Get companies from previous task
    companies = context['task_instance'].xcom_pull(
        task_ids='discover_companies',
        key='discovered_companies'
    )
    
    if not companies:
        return "No companies to fetch"
    
    enriched_companies = []
    
    # Google Places enrichment
    places_collector = GooglePlacesCollector(
        api_key=context['params']['google_places_api_key']
    )
    
    # Website collector
    website_collector = WebsiteCollector()
    
    async def enrich_all():
        for company in companies[:10]:  # Limit for testing
            # Try to get more data from Google Places
            if company.identity.legal_name:
                places_results = await places_collector.collect(
                    company.identity.legal_name,
                    location=company.identity.city
                )
                if places_results:
                    # Merge data
                    company = merge_company_data(company, places_results[0])
            
            # Try to get website data
            if company.web_presence and company.web_presence.website_url:
                website_results = await website_collector.collect(
                    str(company.web_presence.website_url)
                )
                if website_results:
                    company = merge_company_data(company, website_results[0])
            
            enriched_companies.append(company)
    
    asyncio.run(enrich_all())
    
    context['task_instance'].xcom_push(key='fetched_companies', value=enriched_companies)
    return f"Fetched data for {len(enriched_companies)} companies"


def merge_company_data(company1, company2):
    """Helper to merge company data"""
    # Simple merge - in production, use the deduplication module
    if company2.web_presence:
        if not company1.web_presence:
            company1.web_presence = company2.web_presence
        else:
            if company2.web_presence.website_url:
                company1.web_presence.website_url = company2.web_presence.website_url
    
    if company2.contacts:
        if not company1.contacts:
            company1.contacts = company2.contacts
        else:
            company1.contacts.emails_public.extend(company2.contacts.emails_public or [])
            company1.contacts.phones_public.extend(company2.contacts.phones_public or [])
    
    return company1


def parse_and_normalize(**context):
    """Parse and normalize collected data"""
    from src.normalizers.company_normalizer import CompanyNormalizer
    
    companies = context['task_instance'].xcom_pull(
        task_ids='fetch_company_data',
        key='fetched_companies'
    )
    
    if not companies:
        return "No companies to normalize"
    
    normalizer = CompanyNormalizer()
    normalized = normalizer.normalize_batch(companies)
    
    context['task_instance'].xcom_push(key='normalized_companies', value=normalized)
    return f"Normalized {len(normalized)} companies"


def validate_data(**context):
    """Validate data quality"""
    from great_expectations import DataContext
    
    companies = context['task_instance'].xcom_pull(
        task_ids='parse_and_normalize',
        key='normalized_companies'
    )
    
    if not companies:
        return "No companies to validate"
    
    # Convert to DataFrame for validation
    import pandas as pd
    df = pd.DataFrame([c.dict() for c in companies])
    
    # Basic validation rules
    validation_results = {
        'total_records': len(df),
        'valid_records': 0,
        'invalid_records': 0,
        'issues': []
    }
    
    for idx, row in df.iterrows():
        issues = []
        
        # Check required fields
        if not row.get('identity', {}).get('legal_name'):
            issues.append('Missing legal_name')
        
        # Check data quality
        if row.get('web_presence', {}).get('website_url'):
            url = str(row['web_presence']['website_url'])
            if not url.startswith(('http://', 'https://')):
                issues.append('Invalid website URL')
        
        if issues:
            validation_results['invalid_records'] += 1
            validation_results['issues'].append({
                'record': idx,
                'issues': issues
            })
        else:
            validation_results['valid_records'] += 1
    
    context['task_instance'].xcom_push(key='validation_results', value=validation_results)
    return f"Validated {len(df)} records: {validation_results['valid_records']} valid, {validation_results['invalid_records']} invalid"


def deduplicate(**context):
    """Deduplicate companies"""
    from src.deduplication.entity_resolver import EntityResolver
    
    companies = context['task_instance'].xcom_pull(
        task_ids='parse_and_normalize',
        key='normalized_companies'
    )
    
    if not companies:
        return "No companies to deduplicate"
    
    resolver = EntityResolver()
    deduplicated, matches = resolver.resolve_duplicates(companies, auto_merge=True)
    
    context['task_instance'].xcom_push(key='deduplicated_companies', value=deduplicated)
    context['task_instance'].xcom_push(key='duplicate_matches', value=matches)
    
    return f"Deduplicated {len(companies)} to {len(deduplicated)} companies ({len(matches)} matches found)"


def enrich_data(**context):
    """Enrich company data with additional sources"""
    import asyncio
    from src.enrichers.whois_enricher import WhoisEnricher
    
    companies = context['task_instance'].xcom_pull(
        task_ids='deduplicate',
        key='deduplicated_companies'
    )
    
    if not companies:
        return "No companies to enrich"
    
    enricher = WhoisEnricher()
    
    async def enrich_all():
        return await enricher.enrich_batch(companies)
    
    enriched = asyncio.run(enrich_all())
    
    context['task_instance'].xcom_push(key='enriched_companies', value=enriched)
    return f"Enriched {len(enriched)} companies"


def load_to_database(**context):
    """Load processed data to PostgreSQL"""
    companies = context['task_instance'].xcom_pull(
        task_ids='enrich_data',
        key='enriched_companies'
    )
    
    if not companies:
        return "No companies to load"
    
    # Get database connection
    pg_hook = PostgresHook(postgres_conn_id='marketing_platform_db')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    # Insert companies
    inserted = 0
    updated = 0
    
    for company in companies:
        try:
            # Convert to dict for storage
            company_dict = company.dict()
            
            # Check if exists
            cursor.execute(
                "SELECT id FROM unified_companies WHERE legal_name = %s AND city = %s",
                (company.identity.legal_name, company.identity.city)
            )
            existing = cursor.fetchone()
            
            if existing:
                # Update existing
                cursor.execute("""
                    UPDATE unified_companies 
                    SET data = %s, updated_at = NOW()
                    WHERE id = %s
                """, (json.dumps(company_dict), existing[0]))
                updated += 1
            else:
                # Insert new
                cursor.execute("""
                    INSERT INTO unified_companies (legal_name, city, data, created_at)
                    VALUES (%s, %s, %s, NOW())
                """, (
                    company.identity.legal_name,
                    company.identity.city,
                    json.dumps(company_dict)
                ))
                inserted += 1
        
        except Exception as e:
            print(f"Error loading company: {e}")
            continue
    
    conn.commit()
    cursor.close()
    conn.close()
    
    return f"Loaded {inserted} new and updated {updated} existing companies"


def index_to_search(**context):
    """Index data to OpenSearch/Elasticsearch"""
    from opensearchpy import OpenSearch
    import json
    
    companies = context['task_instance'].xcom_pull(
        task_ids='enrich_data',
        key='enriched_companies'
    )
    
    if not companies:
        return "No companies to index"
    
    # Connect to OpenSearch
    client = OpenSearch(
        hosts=[{'host': 'localhost', 'port': 9200}],
        http_auth=('admin', 'admin'),
        use_ssl=False,
        verify_certs=False
    )
    
    # Create index if not exists
    index_name = 'companies'
    if not client.indices.exists(index=index_name):
        client.indices.create(
            index=index_name,
            body={
                'mappings': {
                    'properties': {
                        'legal_name': {'type': 'text', 'analyzer': 'turkish'},
                        'trade_name': {'type': 'text', 'analyzer': 'turkish'},
                        'city': {'type': 'keyword'},
                        'company_type': {'type': 'keyword'},
                        'website_url': {'type': 'keyword'},
                        'emails': {'type': 'keyword'},
                        'phones': {'type': 'keyword'},
                        'keywords': {'type': 'text', 'analyzer': 'turkish'},
                        'created_at': {'type': 'date'},
                        'updated_at': {'type': 'date'}
                    }
                }
            }
        )
    
    # Index companies
    indexed = 0
    for company in companies:
        try:
            doc = {
                'legal_name': company.identity.legal_name,
                'trade_name': company.identity.trade_name,
                'city': company.identity.city,
                'company_type': company.identity.company_type.value if company.identity.company_type else None,
                'website_url': str(company.web_presence.website_url) if company.web_presence and company.web_presence.website_url else None,
                'emails': company.contacts.emails_public if company.contacts else [],
                'phones': company.contacts.phones_public if company.contacts else [],
                'keywords': company.business_meta.keywords if company.business_meta else [],
                'created_at': company.created_at.isoformat(),
                'updated_at': company.last_updated.isoformat()
            }
            
            client.index(
                index=index_name,
                body=doc,
                id=company.id
            )
            indexed += 1
        
        except Exception as e:
            print(f"Error indexing company: {e}")
            continue
    
    return f"Indexed {indexed} companies to search"


def check_compliance(**context):
    """Check GDPR/KVKK compliance"""
    from src.utils.compliance import ComplianceChecker
    
    companies = context['task_instance'].xcom_pull(
        task_ids='enrich_data',
        key='enriched_companies'
    )
    
    if not companies:
        return "No companies to check"
    
    checker = ComplianceChecker()
    
    # Convert to dicts for checking
    company_dicts = [c.dict() for c in companies]
    
    # Generate compliance report
    report = checker.generate_compliance_report(company_dicts)
    
    context['task_instance'].xcom_push(key='compliance_report', value=report)
    
    return f"Compliance check complete: {report['pii_detected']} PII issues found"


# Define task dependencies
with dag:
    # Discovery phase
    discover_task = PythonOperator(
        task_id='discover_companies',
        python_callable=discover_companies,
        params={
            'google_api_key': '{{ var.value.google_api_key }}',
            'google_cse_id': '{{ var.value.google_cse_id }}',
        }
    )
    
    # Fetch phase
    fetch_task = PythonOperator(
        task_id='fetch_company_data',
        python_callable=fetch_company_data,
        params={
            'google_places_api_key': '{{ var.value.google_places_api_key }}',
        }
    )
    
    # Processing phase
    with TaskGroup('processing') as processing_group:
        parse_task = PythonOperator(
            task_id='parse_and_normalize',
            python_callable=parse_and_normalize
        )
        
        validate_task = PythonOperator(
            task_id='validate_data',
            python_callable=validate_data
        )
        
        dedupe_task = PythonOperator(
            task_id='deduplicate',
            python_callable=deduplicate
        )
        
        parse_task >> validate_task >> dedupe_task
    
    # Enrichment phase
    enrich_task = PythonOperator(
        task_id='enrich_data',
        python_callable=enrich_data
    )
    
    # Loading phase
    with TaskGroup('loading') as loading_group:
        load_db_task = PythonOperator(
            task_id='load_to_database',
            python_callable=load_to_database
        )
        
        index_search_task = PythonOperator(
            task_id='index_to_search',
            python_callable=index_to_search
        )
        
        [load_db_task, index_search_task]
    
    # Compliance check
    compliance_task = PythonOperator(
        task_id='check_compliance',
        python_callable=check_compliance,
        trigger_rule='none_failed'
    )
    
    # Set dependencies
    discover_task >> fetch_task >> processing_group >> enrich_task >> loading_group >> compliance_task