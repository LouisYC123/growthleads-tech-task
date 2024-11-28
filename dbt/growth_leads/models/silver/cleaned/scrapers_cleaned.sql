{{
    config(
        materialized='incremental',  
        unique_key='source_id',
        incremental_strategy='delete+insert',
    )
}}

WITH scrapers AS (
    SELECT 
        *
    FROM (
        SELECT *, MAX(ingestion_timestamp) OVER () AS max_ingestion_timestamp
        FROM {{ source('bronze', 'scrapers') }}
    ) subquery
    WHERE ingestion_timestamp = max_ingestion_timestamp
),
final AS (
SELECT 
    COALESCE(date, CAST(REPLACE(filename, '.csv', '') AS TIMESTAMP)) AS event_time
    , COALESCE(CAST(date AS DATE), CAST(REPLACE(filename, '.csv', '') AS DATE)) AS event_date
    , marketing_source
    , operator
    , country AS country_code
    , 0 as raw_earnings
    , COALESCE(total_earnings, 0) as total_earnings
    , COALESCE(visits, 0) AS visits
    , 0 as signups
    , source 
    , filename
    , ingestion_timestamp 
    , event_id
    , source_id
FROM 
    scrapers
ORDER BY 
    ingestion_timestamp DESC
)
SELECT * FROM final