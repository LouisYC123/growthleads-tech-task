WITH cental_mapping AS (
    SELECT * FROM {{ source('bronze', 'central_mapping' )}}
)
SELECT 
    variants 
    , REPLACE(std_operator, 'LOL', ' ') AS std_operator
    , country as country_code
    , source
    , filename
    , ingestion_timestamp 
    , source_id
FROM 
    cental_mapping