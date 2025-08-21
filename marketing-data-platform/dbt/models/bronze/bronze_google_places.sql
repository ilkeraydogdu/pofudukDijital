{{ config(
    materialized='incremental',
    unique_key='place_id',
    on_schema_change='fail'
) }}

WITH raw_data AS (
    SELECT
        place_id,
        source_type,
        fetch_ts,
        data,
        parser_version,
        created_at
    FROM {{ source('raw', 'google_places_raw') }}
    {% if is_incremental() %}
        WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
    {% endif %}
)

SELECT
    place_id,
    source_type,
    fetch_ts,
    -- Extract key fields from JSON
    data->>'name' AS business_name,
    data->>'formatted_address' AS address,
    data->>'formatted_phone_number' AS phone,
    data->>'website' AS website,
    (data->>'rating')::DECIMAL(2,1) AS rating,
    (data->>'user_ratings_total')::INTEGER AS reviews_count,
    data->'types' AS business_types,
    data->>'business_status' AS business_status,
    data AS full_data,
    parser_version,
    created_at,
    CURRENT_TIMESTAMP AS processed_at
FROM raw_data