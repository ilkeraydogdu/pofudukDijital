{{ config(
    materialized='incremental',
    unique_key='website_url',
    on_schema_change='fail'
) }}

WITH raw_data AS (
    SELECT
        website_url,
        source_type,
        fetch_ts,
        data,
        status_code,
        parser_version,
        created_at
    FROM {{ source('raw', 'websites_raw') }}
    {% if is_incremental() %}
        WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
    {% endif %}
)

SELECT
    website_url,
    source_type,
    fetch_ts,
    status_code,
    -- Extract key fields from JSON
    data->>'company_name' AS company_name,
    data->>'title' AS page_title,
    data->>'meta_description' AS meta_description,
    data->'emails' AS emails,
    data->'phones' AS phones,
    data->>'address' AS address,
    data->'social_links' AS social_links,
    data->'keywords' AS keywords,
    data AS full_data,
    parser_version,
    created_at,
    CURRENT_TIMESTAMP AS processed_at
FROM raw_data