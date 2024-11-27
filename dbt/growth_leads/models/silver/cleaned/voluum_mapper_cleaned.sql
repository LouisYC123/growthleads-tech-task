WITH voluum_mapper AS (
    SELECT * FROM {{ source('bronze', 'voluum_mapper' )}}
)
SELECT 
    voluum_brand 
    , marketing_source
    , source 
    , filename
    , ingestion_timestamp 
    , source_id
FROM 
    voluum_mapper