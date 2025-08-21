"""
Great Expectations data quality rules for marketing platform
"""

import great_expectations as ge
from great_expectations.core import ExpectationConfiguration, ExpectationSuite
from great_expectations.data_context import DataContext
from typing import Dict, List


class DataQualityExpectations:
    """Define and manage data quality expectations"""
    
    def __init__(self):
        self.context = DataContext()
    
    def create_company_data_suite(self) -> ExpectationSuite:
        """Create expectation suite for company data"""
        suite_name = "company_data_quality"
        suite = self.context.create_expectation_suite(
            expectation_suite_name=suite_name,
            overwrite_existing=True
        )
        
        # Required fields expectations
        required_fields = [
            ExpectationConfiguration(
                expectation_type="expect_column_to_exist",
                kwargs={"column": "legal_name"}
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "legal_name", "mostly": 0.99}
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_value_lengths_to_be_between",
                kwargs={"column": "legal_name", "min_value": 2, "max_value": 200}
            ),
        ]
        
        # Company type validation
        company_type_expectations = [
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={
                    "column": "company_type",
                    "value_set": [
                        "Anonim Şirket", "Limited Şirket", "Şahıs Şirketi",
                        "Kolektif Şirket", "Komandit Şirket", "Kooperatif",
                        "Dernek", "Vakıf", "Diğer", None
                    ]
                }
            ),
        ]
        
        # Location validation
        location_expectations = [
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={
                    "column": "country",
                    "value_set": ["TR", "US", "GB", "DE", "FR", None],
                    "mostly": 0.95
                }
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_match_regex",
                kwargs={
                    "column": "city",
                    "regex": r"^[A-ZÇĞİÖŞÜa-zçğıöşü\s\-]+$",
                    "mostly": 0.95
                }
            ),
        ]
        
        # Contact information validation
        contact_expectations = [
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_match_regex",
                kwargs={
                    "column": "email",
                    "regex": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
                    "mostly": 0.98
                }
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_match_regex",
                kwargs={
                    "column": "phone",
                    "regex": r"^\+?[0-9\s\-\(\)]+$",
                    "mostly": 0.95
                }
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_value_lengths_to_be_between",
                kwargs={"column": "phone", "min_value": 10, "max_value": 20}
            ),
        ]
        
        # Website validation
        website_expectations = [
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_match_regex",
                kwargs={
                    "column": "website_url",
                    "regex": r"^https?://[a-zA-Z0-9\-\.]+\.[a-zA-Z]{2,}",
                    "mostly": 0.95
                }
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "website_status_code",
                    "min_value": 200,
                    "max_value": 599,
                    "mostly": 0.90
                }
            ),
        ]
        
        # Business metadata validation
        business_expectations = [
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "founding_year",
                    "min_value": 1900,
                    "max_value": 2024,
                    "mostly": 0.95
                }
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={
                    "column": "headcount_band",
                    "value_set": [
                        "1-10", "11-50", "51-200", "201-500",
                        "501-1000", "1001-5000", "5000+", None
                    ]
                }
            ),
        ]
        
        # Rating and review validation
        rating_expectations = [
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "rating",
                    "min_value": 1.0,
                    "max_value": 5.0,
                    "mostly": 1.0
                }
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "reviews_count",
                    "min_value": 0,
                    "max_value": 100000,
                    "mostly": 0.99
                }
            ),
        ]
        
        # Deduplication validation
        dedup_expectations = [
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_unique",
                kwargs={"column": "company_id"}
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "confidence_score",
                    "min_value": 0.0,
                    "max_value": 1.0,
                    "mostly": 1.0
                }
            ),
        ]
        
        # Compliance validation
        compliance_expectations = [
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_match_regex",
                kwargs={
                    "column": "legal_name",
                    "regex": r"\d{11}",  # Turkish ID number pattern
                    "mostly": 1.0
                }
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_match_regex",
                kwargs={
                    "column": "email",
                    "regex": r"^[a-z]+\.[a-z]+@",  # Personal email pattern
                    "mostly": 0.95
                }
            ),
        ]
        
        # Add all expectations to suite
        all_expectations = (
            required_fields +
            company_type_expectations +
            location_expectations +
            contact_expectations +
            website_expectations +
            business_expectations +
            rating_expectations +
            dedup_expectations +
            compliance_expectations
        )
        
        for expectation in all_expectations:
            suite.add_expectation(expectation)
        
        self.context.save_expectation_suite(suite)
        return suite
    
    def create_segment_suite(self) -> ExpectationSuite:
        """Create expectation suite for segmented data"""
        suite_name = "segment_data_quality"
        suite = self.context.create_expectation_suite(
            expectation_suite_name=suite_name,
            overwrite_existing=True
        )
        
        segment_expectations = [
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={
                    "column": "industry",
                    "value_set": [
                        "Technology", "E-Commerce", "Consulting", "Manufacturing",
                        "Logistics", "Tourism", "Education", "Healthcare",
                        "Construction", "Food & Beverage", "Other"
                    ]
                }
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={
                    "column": "company_size",
                    "value_set": ["Micro", "Small", "Medium", "Large", "Unknown"]
                }
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={
                    "column": "engagement_level",
                    "value_set": ["Very Low", "Low", "Medium", "High"]
                }
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={
                    "column": "priority_tier",
                    "value_set": ["A", "B", "C", "D"]
                }
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "engagement_score",
                    "min_value": 0.0,
                    "max_value": 100.0,
                    "mostly": 0.99
                }
            ),
        ]
        
        for expectation in segment_expectations:
            suite.add_expectation(expectation)
        
        self.context.save_expectation_suite(suite)
        return suite
    
    def create_checkpoint(self, suite_name: str, datasource_name: str) -> Dict:
        """Create a checkpoint for running validations"""
        checkpoint_config = {
            "name": f"{suite_name}_checkpoint",
            "config_version": 1.0,
            "class_name": "Checkpoint",
            "validations": [
                {
                    "batch_request": {
                        "datasource_name": datasource_name,
                        "data_connector_name": "default_configured_data_connector",
                        "data_asset_name": "unified_companies",
                    },
                    "expectation_suite_name": suite_name,
                }
            ],
        }
        
        self.context.add_checkpoint(**checkpoint_config)
        return checkpoint_config
    
    def validate_data(self, checkpoint_name: str) -> Dict:
        """Run validation using checkpoint"""
        results = self.context.run_checkpoint(checkpoint_name=checkpoint_name)
        
        # Extract validation results
        validation_summary = {
            "success": results.success,
            "statistics": {},
            "failed_expectations": []
        }
        
        for validation_result in results.run_results.values():
            stats = validation_result["validation_result"]["statistics"]
            validation_summary["statistics"] = {
                "evaluated_expectations": stats["evaluated_expectations"],
                "successful_expectations": stats["successful_expectations"],
                "unsuccessful_expectations": stats["unsuccessful_expectations"],
                "success_percent": stats["success_percent"]
            }
            
            # Get failed expectations
            for result in validation_result["validation_result"]["results"]:
                if not result["success"]:
                    validation_summary["failed_expectations"].append({
                        "expectation": result["expectation_config"]["expectation_type"],
                        "kwargs": result["expectation_config"]["kwargs"],
                        "result": result.get("result", {})
                    })
        
        return validation_summary
    
    def create_data_quality_report(self, validation_results: List[Dict]) -> Dict:
        """Create comprehensive data quality report"""
        report = {
            "total_validations": len(validation_results),
            "successful_validations": sum(1 for r in validation_results if r["success"]),
            "failed_validations": sum(1 for r in validation_results if not r["success"]),
            "overall_success_rate": 0.0,
            "common_issues": {},
            "recommendations": []
        }
        
        if report["total_validations"] > 0:
            report["overall_success_rate"] = (
                report["successful_validations"] / report["total_validations"] * 100
            )
        
        # Analyze common issues
        issue_counts = {}
        for result in validation_results:
            for failed in result.get("failed_expectations", []):
                expectation_type = failed["expectation"]
                issue_counts[expectation_type] = issue_counts.get(expectation_type, 0) + 1
        
        report["common_issues"] = dict(sorted(
            issue_counts.items(),
            key=lambda x: x[1],
            reverse=True
        )[:10])
        
        # Generate recommendations
        if "expect_column_values_to_not_be_null" in issue_counts:
            report["recommendations"].append(
                "Review data collection to ensure required fields are captured"
            )
        
        if "expect_column_values_to_match_regex" in issue_counts:
            report["recommendations"].append(
                "Improve data validation at collection time"
            )
        
        if "expect_column_values_to_be_unique" in issue_counts:
            report["recommendations"].append(
                "Review deduplication logic to prevent duplicate records"
            )
        
        if report["overall_success_rate"] < 90:
            report["recommendations"].append(
                "Critical: Data quality below acceptable threshold. Review entire pipeline"
            )
        
        return report