{{
    config(
        materialized='incremental',  
        unique_key='source_id',
        incremental_strategy='delete+insert',
    )
}}


WITH voluum AS (
    SELECT * FROM {{ source('bronze', 'voluum' )}}
),
final AS (
SELECT 
    COALESCE(date, CAST(REPLACE(filename, '.csv', '') AS TIMESTAMP)) AS event_time
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