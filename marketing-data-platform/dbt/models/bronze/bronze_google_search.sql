{{ config(
    materialized='incremental',
    unique_key='source_url',
    on_schema_change='fail'
) }}

WITH raw_data AS (
    SELECT
        source_url,
        source_type,
        fetch_ts,
        data,
        parser_version,
        created_at
    FROM {{ source('raw', 'google_search_raw') }}
    {% if is_incremental() %}
        WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
    {% endif %}
)

SELECT
    source_url,
    source_type,
    fetch_ts,
    -- Extract key fields from JSON
    data->>'title' AS title,
    data->>'snippet' AS snippet,
    data->>'link' AS link,
    data->>'company_name' AS company_name,
    data->>'city' AS city,
    data->>'company_type' AS company_type,
    data AS full_data,
    parser_version,
    created_at,
    CURRENT_TIMESTAMP AS processed_at
FROM raw_data