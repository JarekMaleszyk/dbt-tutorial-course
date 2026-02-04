{{
    config(
        materialized='incremental',
        unique_key='event_id',
        on_schema_change='sync_all_columns'
    )
}}

WITH source AS (
    SELECT *
    FROM {{ source('thelook_ecommerce', 'events') }}
    {% if is_incremental() %}
      -- Filtrowanie, aby dbt nie przeliczał wszystkiego od nowa
      WHERE created_at > (SELECT max(created_at) FROM {{ this }})
    {% endif %}
)

SELECT
    id AS event_id,
    user_id,
    sequence_number,
    session_id,
    created_at,
    ip_address,
    city,
    state,
    postal_code,
    browser,
    traffic_source,
    uri AS web_link,
    event_type,
    -- Jeśli funkcja get_brand_name nie jest zarejestrowana w DuckDB, 
    -- to tutaj też może wystąpić błąd. Na czas testu możesz to zakomentować.
    uri AS brand_name 

FROM source