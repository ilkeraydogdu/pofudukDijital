{{ config(
    materialized='table',
    unique_key='company_id',
    indexes=[
        {'columns': ['legal_name'], 'type': 'btree'},
        {'columns': ['city'], 'type': 'btree'},
        {'columns': ['website_domain'], 'type': 'btree'},
        {'columns': ['created_at'], 'type': 'btree'}
    ]
) }}

WITH google_search_data AS (
    SELECT
        company_name AS legal_name,
        city,
        company_type,
        link AS source_url,
        'google_search' AS source_type,
        processed_at
    FROM {{ ref('bronze_google_search') }}
    WHERE company_name IS NOT NULL
),

google_places_data AS (
    SELECT
        business_name AS legal_name,
        SPLIT_PART(address, ',', -2) AS city,
        NULL AS company_type,
        website,
        phone,
        address,
        rating,
        reviews_count,
        'google_places' AS source_type,
        processed_at
    FROM {{ ref('bronze_google_places') }}
    WHERE business_name IS NOT NULL
),

website_data AS (
    SELECT
        company_name AS legal_name,
        website_url,
        emails,
        phones,
        address,
        social_links,
        keywords,
        'website' AS source_type,
        processed_at
    FROM {{ ref('bronze_websites') }}
    WHERE company_name IS NOT NULL
),

-- Combine all sources
combined_data AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY legal_name, city) AS row_num,
        legal_name,
        city,
        company_type,
        source_url,
        NULL AS website,
        NULL AS phone,
        NULL AS address,
        NULL AS rating,
        NULL AS reviews_count,
        NULL AS emails,
        NULL AS phones,
        NULL AS social_links,
        NULL AS keywords,
        source_type,
        processed_at
    FROM google_search_data
    
    UNION ALL
    
    SELECT
        ROW_NUMBER() OVER (ORDER BY legal_name, city) AS row_num,
        legal_name,
        city,
        company_type,
        NULL AS source_url,
        website,
        phone,
        address,
        rating,
        reviews_count,
        NULL AS emails,
        NULL AS phones,
        NULL AS social_links,
        NULL AS keywords,
        source_type,
        processed_at
    FROM google_places_data
    
    UNION ALL
    
    SELECT
        ROW_NUMBER() OVER (ORDER BY legal_name) AS row_num,
        legal_name,
        NULL AS city,
        NULL AS company_type,
        NULL AS source_url,
        website_url AS website,
        NULL AS phone,
        address,
        NULL AS rating,
        NULL AS reviews_count,
        emails,
        phones,
        social_links,
        keywords,
        source_type,
        processed_at
    FROM website_data
),

-- Deduplicate and merge
deduplicated AS (
    SELECT
        MD5(CONCAT(
            COALESCE(UPPER(TRIM(legal_name)), ''),
            COALESCE(UPPER(TRIM(city)), ''),
            COALESCE(REGEXP_REPLACE(website, '^https?://(www\.)?', ''), '')
        )) AS company_id,
        UPPER(TRIM(legal_name)) AS legal_name,
        INITCAP(TRIM(city)) AS city,
        company_type,
        -- Aggregate websites
        STRING_AGG(DISTINCT website, ', ') AS websites,
        REGEXP_REPLACE(
            STRING_AGG(DISTINCT website, ', '),
            '^https?://(www\.)?([^/]+).*',
            '\2'
        ) AS website_domain,
        -- Aggregate phones
        STRING_AGG(DISTINCT phone, ', ') AS phones,
        -- Get first non-null address
        MAX(address) AS address,
        -- Aggregate ratings
        AVG(rating) AS avg_rating,
        SUM(reviews_count) AS total_reviews,
        -- Aggregate emails as array
        ARRAY_AGG(DISTINCT email) FILTER (WHERE email IS NOT NULL) AS emails,
        -- Aggregate social links
        JSONB_AGG(DISTINCT social_links) FILTER (WHERE social_links IS NOT NULL) AS social_links,
        -- Aggregate keywords
        ARRAY_AGG(DISTINCT keyword) FILTER (WHERE keyword IS NOT NULL) AS keywords,
        -- Track sources
        ARRAY_AGG(DISTINCT source_type) AS data_sources,
        MIN(processed_at) AS first_seen,
        MAX(processed_at) AS last_updated
    FROM combined_data
    LEFT JOIN LATERAL (
        SELECT jsonb_array_elements_text(emails) AS email
    ) e ON emails IS NOT NULL
    LEFT JOIN LATERAL (
        SELECT jsonb_array_elements_text(keywords) AS keyword
    ) k ON keywords IS NOT NULL
    WHERE legal_name IS NOT NULL
    GROUP BY
        UPPER(TRIM(legal_name)),
        INITCAP(TRIM(city)),
        company_type
)

SELECT
    company_id,
    legal_name,
    city,
    company_type,
    websites,
    website_domain,
    phones,
    address,
    ROUND(avg_rating, 1) AS rating,
    total_reviews,
    emails,
    social_links,
    keywords[1:20] AS top_keywords,  -- Limit to top 20 keywords
    data_sources,
    first_seen,
    last_updated,
    CURRENT_TIMESTAMP AS created_at
FROM deduplicated