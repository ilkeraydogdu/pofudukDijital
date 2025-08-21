{{ config(
    materialized='table',
    indexes=[
        {'columns': ['segment'], 'type': 'btree'},
        {'columns': ['city'], 'type': 'btree'},
        {'columns': ['company_size'], 'type': 'btree'}
    ]
) }}

WITH company_data AS (
    SELECT
        company_id,
        legal_name,
        city,
        company_type,
        website_domain,
        rating,
        total_reviews,
        emails,
        keywords,
        data_sources,
        last_updated
    FROM {{ ref('silver_unified_companies') }}
),

-- Classify companies by industry based on keywords
industry_classification AS (
    SELECT
        company_id,
        CASE
            WHEN keywords @> ARRAY['yazılım', 'software', 'bilişim', 'teknoloji', 'tech']
                THEN 'Technology'
            WHEN keywords @> ARRAY['e-ticaret', 'ecommerce', 'online', 'satış', 'mağaza']
                THEN 'E-Commerce'
            WHEN keywords @> ARRAY['danışmanlık', 'consulting', 'consultancy', 'müşavirlik']
                THEN 'Consulting'
            WHEN keywords @> ARRAY['üretim', 'imalat', 'fabrika', 'manufacturing', 'production']
                THEN 'Manufacturing'
            WHEN keywords @> ARRAY['lojistik', 'nakliye', 'kargo', 'taşımacılık', 'logistics']
                THEN 'Logistics'
            WHEN keywords @> ARRAY['turizm', 'otel', 'tourism', 'travel', 'seyahat']
                THEN 'Tourism'
            WHEN keywords @> ARRAY['eğitim', 'education', 'okul', 'kurs', 'training']
                THEN 'Education'
            WHEN keywords @> ARRAY['sağlık', 'health', 'medical', 'hastane', 'klinik']
                THEN 'Healthcare'
            WHEN keywords @> ARRAY['inşaat', 'construction', 'yapı', 'müteahhit']
                THEN 'Construction'
            WHEN keywords @> ARRAY['gıda', 'food', 'restoran', 'yemek', 'restaurant']
                THEN 'Food & Beverage'
            ELSE 'Other'
        END AS industry
    FROM company_data
),

-- Estimate company size based on various signals
size_estimation AS (
    SELECT
        company_id,
        CASE
            WHEN company_type IN ('Anonim Şirket') THEN 'Large'
            WHEN total_reviews > 1000 THEN 'Large'
            WHEN total_reviews BETWEEN 100 AND 1000 THEN 'Medium'
            WHEN total_reviews BETWEEN 10 AND 100 THEN 'Small'
            WHEN company_type IN ('Limited Şirket') AND total_reviews < 10 THEN 'Small'
            WHEN company_type IN ('Şahıs Şirketi') THEN 'Micro'
            ELSE 'Unknown'
        END AS company_size
    FROM company_data
),

-- Calculate engagement score
engagement_score AS (
    SELECT
        company_id,
        CASE
            WHEN rating >= 4.5 AND total_reviews > 50 THEN 'High'
            WHEN rating >= 4.0 AND total_reviews > 20 THEN 'Medium'
            WHEN rating >= 3.5 THEN 'Low'
            ELSE 'Very Low'
        END AS engagement_level,
        COALESCE(rating * LOG(total_reviews + 1), 0) AS engagement_score
    FROM company_data
),

-- Create marketing segments
segmented_companies AS (
    SELECT
        c.company_id,
        c.legal_name,
        c.city,
        c.company_type,
        c.website_domain,
        i.industry,
        s.company_size,
        e.engagement_level,
        e.engagement_score,
        c.rating,
        c.total_reviews,
        ARRAY_LENGTH(c.emails, 1) AS email_count,
        ARRAY_LENGTH(c.data_sources, 1) AS data_source_count,
        -- Create segment label
        CONCAT(
            COALESCE(c.city, 'Unknown'),
            ' - ',
            i.industry,
            ' - ',
            s.company_size,
            ' - ',
            e.engagement_level,
            ' Engagement'
        ) AS segment,
        -- Marketing priority score
        CASE
            WHEN s.company_size IN ('Large', 'Medium') 
                AND e.engagement_level IN ('High', 'Medium')
                AND c.website_domain IS NOT NULL
                THEN 'A'
            WHEN s.company_size IN ('Medium', 'Small')
                AND e.engagement_level IN ('Medium', 'Low')
                AND c.website_domain IS NOT NULL
                THEN 'B'
            WHEN s.company_size = 'Small'
                AND e.engagement_level = 'Low'
                THEN 'C'
            ELSE 'D'
        END AS priority_tier,
        c.last_updated
    FROM company_data c
    LEFT JOIN industry_classification i ON c.company_id = i.company_id
    LEFT JOIN size_estimation s ON c.company_id = s.company_id
    LEFT JOIN engagement_score e ON c.company_id = e.company_id
)

SELECT
    company_id,
    legal_name,
    city,
    company_type,
    website_domain,
    industry,
    company_size,
    engagement_level,
    ROUND(engagement_score::NUMERIC, 2) AS engagement_score,
    rating,
    total_reviews,
    email_count,
    data_source_count,
    segment,
    priority_tier,
    -- Add actionable insights
    CASE
        WHEN priority_tier = 'A' THEN 'High-value target for direct outreach'
        WHEN priority_tier = 'B' THEN 'Good candidate for email marketing'
        WHEN priority_tier = 'C' THEN 'Nurture with content marketing'
        WHEN priority_tier = 'D' THEN 'Low priority - monitor only'
    END AS marketing_recommendation,
    last_updated,
    CURRENT_TIMESTAMP AS created_at
FROM segmented_companies
ORDER BY priority_tier, engagement_score DESC