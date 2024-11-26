{{
    config(
        materialized='incremental',  
        unique_key='source_id',
        incremental_strategy='delete+insert',
    )
}}


WITH routy AS (
    SELECT * FROM {{ source('bronze', 'routy') }}
),
final AS (
    SELECT 
        COALESCE(date, CAST(REPLACE(filename, '.csv', '') AS TIMESTAMP)) AS event_time,
        marketing_source,
        operator,
        COALESCE(country_code, country) AS country_code,
        COALESCE(raw_earnings, 0) AS raw_earnings,
        COALESCE(visits, 0) AS visits,
        COALESCE(signups, 0) AS signups,
        source,
        filename,
        ingestion_timestamp,
        event_id,
        source_id
    FROM 
        routy
    ORDER BY 
        ingestion_timestamp DESC
)

SELECT * FROM final