{{
    config(
        materialized='incremental',  
        unique_key='source_id',
        incremental_strategy='delete+insert',
    )
}}

WITH voluum AS (
    SELECT 
        *
    FROM (
        SELECT *, MAX(ingestion_timestamp) OVER () AS max_ingestion_timestamp
        FROM {{ source('bronze', 'voluum') }}
    ) subquery
    WHERE ingestion_timestamp = max_ingestion_timestamp
),
final AS (
SELECT 
    COALESCE(date, CAST(REPLACE(filename, '.csv', '') AS TIMESTAMP)) AS event_time
    , COALESCE(CAST(date AS DATE), CAST(REPLACE(filename, '.csv', '') AS DATE)) AS event_date
    , voluum_brand
    , COALESCE(clicks, 0) as clicks
    , source 
    , filename
    , ingestion_timestamp 
    , event_id
    , source_id
FROM 
    voluum
ORDER BY 
    ingestion_timestamp DESC
)
SELECT * FROM final