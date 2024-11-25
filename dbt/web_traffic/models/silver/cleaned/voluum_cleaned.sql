WITH voluum AS (
    SELECT * FROM {{ source('bronze', 'voluum' )}}
)
SELECT 
    COALESCE(date, CAST(REPLACE(filename, '.csv', '') AS TIMESTAMP)) AS event_time
    , voluum_brand
    , COALESCE(clicks, 0) as clicks
    , source 
    , filename
    , ingestion_timestamp 
    , source_id
FROM 
    voluum