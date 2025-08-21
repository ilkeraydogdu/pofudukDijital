"""
OpenSearch/Elasticsearch query examples and utilities
"""

from typing import Dict, List, Optional, Any
from opensearchpy import OpenSearch
from datetime import datetime, timedelta


class SearchQueryBuilder:
    """Build complex search queries for OpenSearch"""
    
    def __init__(self, client: OpenSearch):
        self.client = client
        self.index = "companies"
    
    def basic_search(self, query: str, size: int = 10) -> Dict:
        """Basic text search across all fields"""
        return {
            "query": {
                "multi_match": {
                    "query": query,
                    "fields": [
                        "legal_name^3",
                        "trade_name^2",
                        "keywords",
                        "city",
                        "address",
                        "industry"
                    ],
                    "type": "best_fields",
                    "fuzziness": "AUTO"
                }
            },
            "size": size,
            "highlight": {
                "fields": {
                    "legal_name": {},
                    "trade_name": {},
                    "keywords": {}
                }
            }
        }
    
    def advanced_search(
        self,
        query: Optional[str] = None,
        filters: Optional[Dict] = None,
        sort: Optional[List] = None,
        from_: int = 0,
        size: int = 20
    ) -> Dict:
        """Advanced search with filters and sorting"""
        must_clauses = []
        filter_clauses = []
        
        # Text search
        if query:
            must_clauses.append({
                "multi_match": {
                    "query": query,
                    "fields": [
                        "legal_name^3",
                        "trade_name^2",
                        "keywords",
                        "city.text",
                        "industry.text"
                    ],
                    "type": "cross_fields",
                    "operator": "and"
                }
            })
        
        # Apply filters
        if filters:
            for field, value in filters.items():
                if isinstance(value, list):
                    filter_clauses.append({"terms": {field: value}})
                elif isinstance(value, dict):
                    # Range filter
                    if "min" in value or "max" in value:
                        range_filter = {}
                        if "min" in value:
                            range_filter["gte"] = value["min"]
                        if "max" in value:
                            range_filter["lte"] = value["max"]
                        filter_clauses.append({"range": {field: range_filter}})
                else:
                    filter_clauses.append({"term": {field: value}})
        
        # Build query
        query_body = {
            "query": {
                "bool": {
                    "must": must_clauses if must_clauses else [{"match_all": {}}],
                    "filter": filter_clauses
                }
            },
            "from": from_,
            "size": size
        }
        
        # Add sorting
        if sort:
            query_body["sort"] = sort
        else:
            query_body["sort"] = ["_score", {"updated_at": "desc"}]
        
        return query_body
    
    def segment_search(
        self,
        city: Optional[str] = None,
        industry: Optional[str] = None,
        company_size: Optional[str] = None,
        priority_tier: Optional[str] = None,
        min_rating: Optional[float] = None
    ) -> Dict:
        """Search by marketing segments"""
        filter_clauses = []
        
        if city:
            filter_clauses.append({"term": {"city": city}})
        
        if industry:
            filter_clauses.append({"term": {"industry": industry}})
        
        if company_size:
            filter_clauses.append({"term": {"company_size": company_size}})
        
        if priority_tier:
            filter_clauses.append({"term": {"priority_tier": priority_tier}})
        
        if min_rating:
            filter_clauses.append({"range": {"rating": {"gte": min_rating}}})
        
        return {
            "query": {
                "bool": {
                    "filter": filter_clauses
                }
            },
            "aggs": {
                "segments": {
                    "terms": {
                        "field": "segment",
                        "size": 50
                    },
                    "aggs": {
                        "avg_rating": {"avg": {"field": "rating"}},
                        "avg_engagement": {"avg": {"field": "engagement_score"}},
                        "company_count": {"value_count": {"field": "company_id"}}
                    }
                }
            },
            "size": 100
        }
    
    def geo_search(
        self,
        lat: float,
        lon: float,
        distance: str = "10km"
    ) -> Dict:
        """Search companies by geographic location"""
        return {
            "query": {
                "bool": {
                    "filter": {
                        "geo_distance": {
                            "distance": distance,
                            "location": {
                                "lat": lat,
                                "lon": lon
                            }
                        }
                    }
                }
            },
            "sort": [
                {
                    "_geo_distance": {
                        "location": {
                            "lat": lat,
                            "lon": lon
                        },
                        "order": "asc",
                        "unit": "km"
                    }
                }
            ]
        }
    
    def competitor_analysis(
        self,
        company_name: str,
        industry: Optional[str] = None,
        city: Optional[str] = None
    ) -> Dict:
        """Find similar companies (competitors)"""
        should_clauses = []
        
        # Similar name
        should_clauses.append({
            "match": {
                "legal_name": {
                    "query": company_name,
                    "fuzziness": "AUTO"
                }
            }
        })
        
        # Same industry
        if industry:
            should_clauses.append({
                "term": {"industry": {"value": industry, "boost": 2}}
            })
        
        # Same city
        if city:
            should_clauses.append({
                "term": {"city": {"value": city, "boost": 1.5}}
            })
        
        return {
            "query": {
                "bool": {
                    "should": should_clauses,
                    "minimum_should_match": 1
                }
            },
            "size": 20
        }
    
    def high_value_targets(
        self,
        min_reviews: int = 50,
        min_rating: float = 4.0,
        industries: Optional[List[str]] = None
    ) -> Dict:
        """Find high-value marketing targets"""
        must_clauses = [
            {"range": {"reviews_count": {"gte": min_reviews}}},
            {"range": {"rating": {"gte": min_rating}}},
            {"terms": {"priority_tier": ["A", "B"]}},
            {"exists": {"field": "website_domain"}},
            {"exists": {"field": "emails"}}
        ]
        
        if industries:
            must_clauses.append({"terms": {"industry": industries}})
        
        return {
            "query": {
                "bool": {
                    "must": must_clauses
                }
            },
            "sort": [
                {"engagement_score": "desc"},
                {"reviews_count": "desc"}
            ],
            "size": 100
        }
    
    def recent_updates(
        self,
        days: int = 7,
        min_confidence: float = 0.8
    ) -> Dict:
        """Find recently updated companies"""
        cutoff_date = (datetime.now() - timedelta(days=days)).isoformat()
        
        return {
            "query": {
                "bool": {
                    "filter": [
                        {"range": {"updated_at": {"gte": cutoff_date}}},
                        {"range": {"confidence_score": {"gte": min_confidence}}}
                    ]
                }
            },
            "sort": [{"updated_at": "desc"}],
            "size": 50
        }
    
    def data_quality_check(self) -> Dict:
        """Query to check data quality issues"""
        return {
            "size": 0,
            "aggs": {
                "missing_emails": {
                    "missing": {"field": "emails"}
                },
                "missing_phones": {
                    "missing": {"field": "phones"}
                },
                "missing_website": {
                    "missing": {"field": "website_domain"}
                },
                "missing_city": {
                    "missing": {"field": "city"}
                },
                "low_confidence": {
                    "range": {
                        "field": "confidence_score",
                        "ranges": [
                            {"to": 0.5, "key": "very_low"},
                            {"from": 0.5, "to": 0.7, "key": "low"},
                            {"from": 0.7, "to": 0.9, "key": "medium"},
                            {"from": 0.9, "key": "high"}
                        ]
                    }
                },
                "data_sources_distribution": {
                    "terms": {
                        "field": "data_sources",
                        "size": 20
                    }
                }
            }
        }
    
    def autocomplete(self, prefix: str, size: int = 10) -> Dict:
        """Autocomplete/suggest query for company names"""
        return {
            "suggest": {
                "company_suggest": {
                    "prefix": prefix,
                    "completion": {
                        "field": "suggest",
                        "size": size,
                        "fuzzy": {
                            "fuzziness": "AUTO"
                        }
                    }
                }
            },
            "_source": ["legal_name", "trade_name", "city", "company_type"]
        }
    
    def aggregation_dashboard(self) -> Dict:
        """Comprehensive aggregations for dashboard"""
        return {
            "size": 0,
            "aggs": {
                "total_companies": {
                    "cardinality": {"field": "company_id"}
                },
                "cities": {
                    "terms": {
                        "field": "city",
                        "size": 20
                    },
                    "aggs": {
                        "avg_rating": {"avg": {"field": "rating"}},
                        "company_types": {
                            "terms": {"field": "company_type", "size": 5}
                        }
                    }
                },
                "industries": {
                    "terms": {
                        "field": "industry",
                        "size": 15
                    },
                    "aggs": {
                        "sizes": {
                            "terms": {"field": "company_size", "size": 5}
                        },
                        "avg_engagement": {"avg": {"field": "engagement_score"}}
                    }
                },
                "priority_distribution": {
                    "terms": {
                        "field": "priority_tier",
                        "size": 4
                    },
                    "aggs": {
                        "count": {"value_count": {"field": "company_id"}},
                        "with_email": {
                            "filter": {"exists": {"field": "emails"}}
                        },
                        "with_website": {
                            "filter": {"exists": {"field": "website_domain"}}
                        }
                    }
                },
                "rating_distribution": {
                    "histogram": {
                        "field": "rating",
                        "interval": 0.5,
                        "min_doc_count": 1
                    }
                },
                "recent_activity": {
                    "date_histogram": {
                        "field": "updated_at",
                        "calendar_interval": "day",
                        "min_doc_count": 1
                    }
                },
                "data_completeness": {
                    "filters": {
                        "filters": {
                            "has_email": {"exists": {"field": "emails"}},
                            "has_phone": {"exists": {"field": "phones"}},
                            "has_website": {"exists": {"field": "website_domain"}},
                            "has_address": {"exists": {"field": "address"}},
                            "has_social": {"exists": {"field": "social_links"}}
                        }
                    }
                }
            }
        }
    
    def export_query(
        self,
        filters: Optional[Dict] = None,
        fields: Optional[List[str]] = None,
        limit: int = 10000
    ) -> Dict:
        """Query for data export"""
        query = {
            "query": {"match_all": {}},
            "size": limit
        }
        
        if filters:
            filter_clauses = []
            for field, value in filters.items():
                if isinstance(value, list):
                    filter_clauses.append({"terms": {field: value}})
                else:
                    filter_clauses.append({"term": {field: value}})
            
            query["query"] = {
                "bool": {
                    "filter": filter_clauses
                }
            }
        
        if fields:
            query["_source"] = fields
        
        return query


class SearchExecutor:
    """Execute search queries and handle results"""
    
    def __init__(self, client: OpenSearch):
        self.client = client
        self.builder = SearchQueryBuilder(client)
        self.index = "companies"
    
    async def execute_search(
        self,
        query: Dict,
        index: Optional[str] = None
    ) -> Dict:
        """Execute a search query"""
        try:
            response = self.client.search(
                index=index or self.index,
                body=query
            )
            return self._process_response(response)
        except Exception as e:
            return {"error": str(e), "results": []}
    
    def _process_response(self, response: Dict) -> Dict:
        """Process OpenSearch response"""
        processed = {
            "total": response["hits"]["total"]["value"],
            "max_score": response["hits"].get("max_score"),
            "results": [],
            "aggregations": {}
        }
        
        # Process hits
        for hit in response["hits"]["hits"]:
            result = {
                "id": hit["_id"],
                "score": hit.get("_score"),
                **hit["_source"]
            }
            
            # Add highlights if present
            if "highlight" in hit:
                result["highlights"] = hit["highlight"]
            
            processed["results"].append(result)
        
        # Process aggregations
        if "aggregations" in response:
            processed["aggregations"] = response["aggregations"]
        
        # Process suggestions
        if "suggest" in response:
            processed["suggestions"] = response["suggest"]
        
        return processed
    
    async def bulk_update(
        self,
        updates: List[Dict],
        index: Optional[str] = None
    ) -> Dict:
        """Bulk update documents"""
        from opensearchpy import helpers
        
        try:
            actions = []
            for update in updates:
                action = {
                    "_op_type": "update",
                    "_index": index or self.index,
                    "_id": update["id"],
                    "doc": update["data"],
                    "doc_as_upsert": True
                }
                actions.append(action)
            
            success, failed = helpers.bulk(
                self.client,
                actions,
                raise_on_error=False
            )
            
            return {
                "success": success,
                "failed": failed
            }
        except Exception as e:
            return {"error": str(e)}
    
    async def create_index(
        self,
        index_name: str,
        mappings: Dict,
        settings: Optional[Dict] = None
    ) -> bool:
        """Create a new index with mappings"""
        try:
            body = {"mappings": mappings}
            if settings:
                body["settings"] = settings
            
            self.client.indices.create(
                index=index_name,
                body=body
            )
            return True
        except Exception as e:
            print(f"Error creating index: {e}")
            return False
    
    async def reindex(
        self,
        source_index: str,
        target_index: str,
        query: Optional[Dict] = None
    ) -> Dict:
        """Reindex data from one index to another"""
        try:
            body = {
                "source": {"index": source_index},
                "dest": {"index": target_index}
            }
            
            if query:
                body["source"]["query"] = query
            
            response = self.client.reindex(body=body)
            return response
        except Exception as e:
            return {"error": str(e)}