{{
    config(
        materialized='incremental',  
        unique_key='source_id',
        incremental_strategy='delete+insert',
    )
}}
{% if does_table_exist('bronze', 'routy') %}
WITH routy AS (
    SELECT * FROM {{ source('bronze', 'routy') }}
),
final AS (
    SELECT 
        COALESCE(date, CAST(REPLACE(filename, '.csv', '') AS TIMESTAMP)) AS event_time
        , COALESCE(CAST(date AS DATE), CAST(REPLACE(filename, '.csv', '') AS DATE)) AS event_date
        , marketing_source
        , operator
        , COALESCE(country_code, country) AS country_code
        , COALESCE(raw_earnings, 0) AS raw_earnings
        , 0 AS total_earnings
        , COALESCE(visits, 0) AS visits
        , COALESCE(signups, 0) AS signups
        , source
        , filename
        , ingestion_timestamp
        , event_id
        , source_id
    FROM 
        routy
    ORDER BY 
        ingestion_timestamp DESC
)

{% else %}
-- If the source does not exist or is empty, return a single row with NULL values
WITH final AS (
    SELECT 
        NULL AS event_time,
        NULL AS event_date,
        NULL AS marketing_source,
        NULL AS operator,
        NULL AS country_code,
        NULL AS raw_earnings,
        NULL AS total_earnings,
        NULL AS visits,
        NULL AS signups,
        NULL AS source,
        NULL AS filename,
        NULL AS ingestion_timestamp,
        NULL AS event_id,
        NULL AS source_id
)
{% endif %}

-- Final output
SELECT * FROM final WHERE event_time IS NOT NULL
