{{ config(
    materialized='view'
) }}

WITH segment_metrics AS (
    SELECT
        segment,
        city,
        industry,
        company_size,
        priority_tier,
        COUNT(*) AS company_count,
        AVG(rating) AS avg_rating,
        SUM(total_reviews) AS total_reviews,
        AVG(engagement_score) AS avg_engagement_score,
        COUNT(DISTINCT website_domain) AS unique_domains,
        AVG(email_count) AS avg_emails_per_company
    FROM {{ ref('gold_company_segments') }}
    GROUP BY segment, city, industry, company_size, priority_tier
),

city_metrics AS (
    SELECT
        city,
        COUNT(*) AS total_companies,
        COUNT(DISTINCT industry) AS industry_diversity,
        AVG(rating) AS city_avg_rating,
        SUM(total_reviews) AS city_total_reviews,
        COUNT(CASE WHEN priority_tier IN ('A', 'B') THEN 1 END) AS high_value_companies,
        ROUND(
            COUNT(CASE WHEN priority_tier IN ('A', 'B') THEN 1 END)::NUMERIC / 
            COUNT(*)::NUMERIC * 100, 
            2
        ) AS high_value_percentage
    FROM {{ ref('gold_company_segments') }}
    WHERE city IS NOT NULL
    GROUP BY city
),

industry_metrics AS (
    SELECT
        industry,
        COUNT(*) AS total_companies,
        COUNT(DISTINCT city) AS city_coverage,
        AVG(rating) AS industry_avg_rating,
        AVG(engagement_score) AS industry_avg_engagement,
        COUNT(CASE WHEN company_size IN ('Large', 'Medium') THEN 1 END) AS large_medium_companies,
        COUNT(CASE WHEN website_domain IS NOT NULL THEN 1 END) AS companies_with_website,
        ROUND(
            COUNT(CASE WHEN website_domain IS NOT NULL THEN 1 END)::NUMERIC / 
            COUNT(*)::NUMERIC * 100,
            2
        ) AS website_coverage_percentage
    FROM {{ ref('gold_company_segments') }}
    GROUP BY industry
)

SELECT
    'segment' AS metric_type,
    segment AS metric_name,
    company_count AS count,
    ROUND(avg_rating::NUMERIC, 2) AS avg_rating,
    total_reviews,
    ROUND(avg_engagement_score::NUMERIC, 2) AS avg_engagement,
    unique_domains,
    ROUND(avg_emails_per_company::NUMERIC, 2) AS avg_emails,
    NULL AS percentage,
    priority_tier,
    CURRENT_TIMESTAMP AS calculated_at
FROM segment_metrics

UNION ALL

SELECT
    'city' AS metric_type,
    city AS metric_name,
    total_companies AS count,
    ROUND(city_avg_rating::NUMERIC, 2) AS avg_rating,
    city_total_reviews AS total_reviews,
    NULL AS avg_engagement,
    NULL AS unique_domains,
    NULL AS avg_emails,
    high_value_percentage AS percentage,
    CASE
        WHEN high_value_percentage > 30 THEN 'A'
        WHEN high_value_percentage > 20 THEN 'B'
        WHEN high_value_percentage > 10 THEN 'C'
        ELSE 'D'
    END AS priority_tier,
    CURRENT_TIMESTAMP AS calculated_at
FROM city_metrics

UNION ALL

SELECT
    'industry' AS metric_type,
    industry AS metric_name,
    total_companies AS count,
    ROUND(industry_avg_rating::NUMERIC, 2) AS avg_rating,
    NULL AS total_reviews,
    ROUND(industry_avg_engagement::NUMERIC, 2) AS avg_engagement,
    companies_with_website AS unique_domains,
    NULL AS avg_emails,
    website_coverage_percentage AS percentage,
    CASE
        WHEN large_medium_companies > 10 AND website_coverage_percentage > 50 THEN 'A'
        WHEN large_medium_companies > 5 AND website_coverage_percentage > 30 THEN 'B'
        WHEN website_coverage_percentage > 20 THEN 'C'
        ELSE 'D'
    END AS priority_tier,
    CURRENT_TIMESTAMP AS calculated_at
FROM industry_metrics

ORDER BY metric_type, priority_tier, count DESC