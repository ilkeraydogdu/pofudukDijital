"""
FastAPI application for marketing data platform
"""

import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd
from fastapi import FastAPI, HTTPException, Query, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text
from opensearchpy import OpenSearch
import redis
import json
import io

from ..models.schemas import UnifiedCompany, CompanyType
from ..utils.compliance import ComplianceChecker

# Initialize FastAPI app
app = FastAPI(
    title="Marketing Data Platform API",
    description="KVKK/GDPR compliant company data search and export API",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database connection
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://marketing_user:secure_password@localhost:5432/marketing_platform"
)
engine = create_engine(DATABASE_URL)

# OpenSearch connection
opensearch_client = OpenSearch(
    hosts=[{
        'host': os.getenv('OPENSEARCH_HOST', 'localhost'),
        'port': int(os.getenv('OPENSEARCH_PORT', 9200))
    }],
    http_auth=(
        os.getenv('OPENSEARCH_USER', 'admin'),
        os.getenv('OPENSEARCH_PASSWORD', 'admin')
    ),
    use_ssl=False,
    verify_certs=False
)

# Redis cache
redis_client = redis.Redis(
    host=os.getenv('REDIS_HOST', 'localhost'),
    port=int(os.getenv('REDIS_PORT', 6379)),
    db=int(os.getenv('REDIS_DB', 0)),
    decode_responses=True
)

# Compliance checker
compliance = ComplianceChecker()


# Request/Response models
class SearchRequest(BaseModel):
    """Search request model"""
    query: str = Field(..., description="Search query")
    filters: Optional[Dict] = Field(None, description="Filter criteria")
    page: int = Field(1, ge=1, description="Page number")
    page_size: int = Field(20, ge=1, le=100, description="Results per page")
    sort_by: Optional[str] = Field("relevance", description="Sort field")
    sort_order: Optional[str] = Field("desc", description="Sort order (asc/desc)")


class CompanyResponse(BaseModel):
    """Company response model"""
    id: str
    legal_name: str
    trade_name: Optional[str]
    city: Optional[str]
    company_type: Optional[str]
    website: Optional[str]
    emails: List[str]
    phones: List[str]
    rating: Optional[float]
    industry: Optional[str]
    size: Optional[str]
    priority_tier: Optional[str]
    last_updated: datetime


class SearchResponse(BaseModel):
    """Search response model"""
    total: int
    page: int
    page_size: int
    results: List[CompanyResponse]
    facets: Optional[Dict]
    query_time_ms: int


class ExportRequest(BaseModel):
    """Export request model"""
    format: str = Field("csv", description="Export format (csv/xlsx/json)")
    filters: Optional[Dict] = Field(None, description="Filter criteria")
    fields: Optional[List[str]] = Field(None, description="Fields to export")
    limit: int = Field(1000, le=10000, description="Maximum records to export")


class ComplianceRequest(BaseModel):
    """GDPR/KVKK compliance request"""
    action: str = Field(..., description="Action type (delete/export/suppress)")
    identifier: str = Field(..., description="Company identifier")
    reason: Optional[str] = Field(None, description="Request reason")
    requester_email: str = Field(..., description="Requester email")


class AnalyticsResponse(BaseModel):
    """Analytics response model"""
    total_companies: int
    cities_covered: int
    industries: Dict[str, int]
    company_sizes: Dict[str, int]
    priority_distribution: Dict[str, int]
    data_quality_score: float
    last_update: datetime


# Helper functions
def get_cache_key(request: SearchRequest) -> str:
    """Generate cache key for search request"""
    key_data = {
        'query': request.query,
        'filters': request.filters,
        'page': request.page,
        'page_size': request.page_size,
        'sort_by': request.sort_by
    }
    return f"search:{json.dumps(key_data, sort_keys=True)}"


async def search_opensearch(request: SearchRequest) -> Dict:
    """Search companies in OpenSearch"""
    # Build query
    query = {
        "bool": {
            "must": []
        }
    }
    
    # Add text search
    if request.query:
        query["bool"]["must"].append({
            "multi_match": {
                "query": request.query,
                "fields": ["legal_name^2", "trade_name", "keywords", "city"],
                "type": "best_fields",
                "fuzziness": "AUTO"
            }
        })
    
    # Add filters
    if request.filters:
        for field, value in request.filters.items():
            if isinstance(value, list):
                query["bool"]["must"].append({
                    "terms": {field: value}
                })
            else:
                query["bool"]["must"].append({
                    "term": {field: value}
                })
    
    # Build aggregations for facets
    aggs = {
        "cities": {"terms": {"field": "city", "size": 20}},
        "industries": {"terms": {"field": "industry", "size": 15}},
        "company_types": {"terms": {"field": "company_type", "size": 10}},
        "priority_tiers": {"terms": {"field": "priority_tier", "size": 4}}
    }
    
    # Execute search
    response = opensearch_client.search(
        index="companies",
        body={
            "query": query,
            "aggs": aggs,
            "from": (request.page - 1) * request.page_size,
            "size": request.page_size,
            "sort": [
                {"_score": {"order": "desc"}} if request.sort_by == "relevance"
                else {request.sort_by: {"order": request.sort_order}}
            ]
        }
    )
    
    return response


# API Endpoints
@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "name": "Marketing Data Platform API",
        "version": "1.0.0",
        "status": "operational",
        "endpoints": {
            "search": "/api/v1/search",
            "company": "/api/v1/company/{id}",
            "export": "/api/v1/export",
            "analytics": "/api/v1/analytics",
            "compliance": "/api/v1/compliance"
        }
    }


@app.post("/api/v1/search", response_model=SearchResponse)
async def search_companies(request: SearchRequest):
    """Search companies with filters and pagination"""
    start_time = datetime.now()
    
    # Check cache
    cache_key = get_cache_key(request)
    cached_result = redis_client.get(cache_key)
    
    if cached_result:
        return json.loads(cached_result)
    
    try:
        # Search in OpenSearch
        search_result = await search_opensearch(request)
        
        # Parse results
        companies = []
        for hit in search_result['hits']['hits']:
            source = hit['_source']
            companies.append(CompanyResponse(
                id=hit['_id'],
                legal_name=source.get('legal_name', ''),
                trade_name=source.get('trade_name'),
                city=source.get('city'),
                company_type=source.get('company_type'),
                website=source.get('website_url'),
                emails=source.get('emails', []),
                phones=source.get('phones', []),
                rating=source.get('rating'),
                industry=source.get('industry'),
                size=source.get('company_size'),
                priority_tier=source.get('priority_tier'),
                last_updated=datetime.fromisoformat(source.get('updated_at', datetime.now().isoformat()))
            ))
        
        # Parse facets
        facets = {}
        for facet_name, facet_data in search_result.get('aggregations', {}).items():
            facets[facet_name] = [
                {"value": bucket['key'], "count": bucket['doc_count']}
                for bucket in facet_data.get('buckets', [])
            ]
        
        # Build response
        response = SearchResponse(
            total=search_result['hits']['total']['value'],
            page=request.page,
            page_size=request.page_size,
            results=companies,
            facets=facets,
            query_time_ms=int((datetime.now() - start_time).total_seconds() * 1000)
        )
        
        # Cache result (TTL: 5 minutes)
        redis_client.setex(
            cache_key,
            300,
            json.dumps(response.dict(), default=str)
        )
        
        return response
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/company/{company_id}")
async def get_company(company_id: str):
    """Get detailed company information"""
    try:
        # Get from OpenSearch
        result = opensearch_client.get(index="companies", id=company_id)
        
        if not result['found']:
            raise HTTPException(status_code=404, detail="Company not found")
        
        source = result['_source']
        
        # Get additional data from PostgreSQL
        with engine.connect() as conn:
            query = text("""
                SELECT data 
                FROM unified_companies 
                WHERE id = :company_id
            """)
            db_result = conn.execute(query, {"company_id": company_id}).fetchone()
            
            if db_result:
                additional_data = json.loads(db_result[0])
                source.update(additional_data)
        
        # Filter PII before returning
        filtered_data = compliance.filter_pii(source)
        
        return filtered_data
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/export")
async def export_companies(request: ExportRequest):
    """Export companies in various formats"""
    try:
        # Build query
        query = {"match_all": {}}
        if request.filters:
            query = {
                "bool": {
                    "must": [
                        {"term": {k: v}} for k, v in request.filters.items()
                    ]
                }
            }
        
        # Search with limit
        search_result = opensearch_client.search(
            index="companies",
            body={
                "query": query,
                "size": request.limit
            }
        )
        
        # Prepare data
        data = []
        for hit in search_result['hits']['hits']:
            source = hit['_source']
            if request.fields:
                source = {k: source.get(k) for k in request.fields}
            data.append(source)
        
        # Export based on format
        if request.format == "json":
            return data
        
        elif request.format == "csv":
            df = pd.DataFrame(data)
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            
            return StreamingResponse(
                io.BytesIO(csv_buffer.getvalue().encode()),
                media_type="text/csv",
                headers={
                    "Content-Disposition": f"attachment; filename=companies_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                }
            )
        
        elif request.format == "xlsx":
            df = pd.DataFrame(data)
            excel_buffer = io.BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Companies', index=False)
            excel_buffer.seek(0)
            
            return StreamingResponse(
                excel_buffer,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={
                    "Content-Disposition": f"attachment; filename=companies_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                }
            )
        
        else:
            raise HTTPException(status_code=400, detail="Invalid export format")
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/analytics", response_model=AnalyticsResponse)
async def get_analytics():
    """Get platform analytics and statistics"""
    try:
        with engine.connect() as conn:
            # Get statistics from database
            stats_query = text("""
                SELECT 
                    COUNT(DISTINCT company_id) as total_companies,
                    COUNT(DISTINCT city) as cities_covered,
                    COUNT(DISTINCT industry) as industries_count,
                    AVG(CASE WHEN email_count > 0 THEN 1 ELSE 0 END) as email_coverage,
                    AVG(CASE WHEN website_domain IS NOT NULL THEN 1 ELSE 0 END) as website_coverage,
                    MAX(last_updated) as last_update
                FROM gold.company_segments
            """)
            stats = conn.execute(stats_query).fetchone()
            
            # Get industry distribution
            industry_query = text("""
                SELECT industry, COUNT(*) as count
                FROM gold.company_segments
                GROUP BY industry
                ORDER BY count DESC
            """)
            industries = {row[0]: row[1] for row in conn.execute(industry_query)}
            
            # Get size distribution
            size_query = text("""
                SELECT company_size, COUNT(*) as count
                FROM gold.company_segments
                GROUP BY company_size
                ORDER BY count DESC
            """)
            sizes = {row[0]: row[1] for row in conn.execute(size_query)}
            
            # Get priority distribution
            priority_query = text("""
                SELECT priority_tier, COUNT(*) as count
                FROM gold.company_segments
                GROUP BY priority_tier
                ORDER BY priority_tier
            """)
            priorities = {row[0]: row[1] for row in conn.execute(priority_query)}
            
            # Calculate data quality score
            data_quality = (
                (stats[3] * 0.3) +  # Email coverage
                (stats[4] * 0.3) +  # Website coverage
                (0.4 if stats[0] > 1000 else stats[0] / 2500)  # Volume score
            )
            
            return AnalyticsResponse(
                total_companies=stats[0],
                cities_covered=stats[1],
                industries=industries,
                company_sizes=sizes,
                priority_distribution=priorities,
                data_quality_score=round(data_quality * 100, 2),
                last_update=stats[5]
            )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/compliance")
async def handle_compliance_request(
    request: ComplianceRequest,
    background_tasks: BackgroundTasks
):
    """Handle GDPR/KVKK compliance requests"""
    try:
        if request.action == "delete":
            # Add to suppression list
            compliance.add_to_suppression(request.identifier)
            
            # Schedule deletion in background
            background_tasks.add_task(
                delete_company_data,
                request.identifier,
                request.requester_email
            )
            
            return {
                "status": "accepted",
                "message": "Deletion request received and will be processed",
                "reference_id": f"DEL-{datetime.now().strftime('%Y%m%d%H%M%S')}-{request.identifier[:8]}"
            }
        
        elif request.action == "export":
            # Export company data
            with engine.connect() as conn:
                query = text("""
                    SELECT data 
                    FROM unified_companies 
                    WHERE id = :company_id OR legal_name = :company_name
                """)
                result = conn.execute(
                    query,
                    {
                        "company_id": request.identifier,
                        "company_name": request.identifier
                    }
                ).fetchone()
                
                if not result:
                    raise HTTPException(status_code=404, detail="Company not found")
                
                data = json.loads(result[0])
                filtered_data = compliance.filter_pii(data)
                
                return {
                    "status": "completed",
                    "data": filtered_data,
                    "exported_at": datetime.now().isoformat()
                }
        
        elif request.action == "suppress":
            # Add to suppression list without deletion
            compliance.add_to_suppression(request.identifier)
            
            return {
                "status": "completed",
                "message": "Company added to suppression list",
                "suppressed_at": datetime.now().isoformat()
            }
        
        else:
            raise HTTPException(status_code=400, detail="Invalid action")
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def delete_company_data(company_id: str, requester_email: str):
    """Background task to delete company data"""
    try:
        # Delete from OpenSearch
        opensearch_client.delete(index="companies", id=company_id, ignore=[404])
        
        # Soft delete from PostgreSQL
        with engine.connect() as conn:
            query = text("""
                UPDATE unified_companies 
                SET 
                    gdpr_suppressed = true,
                    suppression_date = NOW(),
                    data = jsonb_set(data, '{suppression_requester}', :requester)
                WHERE id = :company_id
            """)
            conn.execute(
                query,
                {
                    "company_id": company_id,
                    "requester": json.dumps(requester_email)
                }
            )
            conn.commit()
        
        # Log compliance action
        print(f"Company {company_id} data deleted per GDPR request from {requester_email}")
        
    except Exception as e:
        print(f"Error deleting company data: {e}")


@app.get("/api/v1/health")
async def health_check():
    """Health check endpoint"""
    health_status = {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "services": {}
    }
    
    # Check database
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        health_status["services"]["database"] = "healthy"
    except:
        health_status["services"]["database"] = "unhealthy"
        health_status["status"] = "degraded"
    
    # Check OpenSearch
    try:
        if opensearch_client.ping():
            health_status["services"]["opensearch"] = "healthy"
        else:
            health_status["services"]["opensearch"] = "unhealthy"
            health_status["status"] = "degraded"
    except:
        health_status["services"]["opensearch"] = "unhealthy"
        health_status["status"] = "degraded"
    
    # Check Redis
    try:
        redis_client.ping()
        health_status["services"]["redis"] = "healthy"
    except:
        health_status["services"]["redis"] = "unhealthy"
        health_status["status"] = "degraded"
    
    return health_status


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)