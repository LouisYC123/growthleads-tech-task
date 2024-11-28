{{
    config(
        materialized='incremental',
        unique_key='source_id',
        incremental_strategy='delete+insert',
    )
}}
WITH cental_mapping AS (
    SELECT * FROM {{ source('bronze', 'central_mapping' )}}
)
SELECT 
    variants 
    , REPLACE(std_operator, 'LOL', ' ') AS operator
    , country as country_code
    , source
    , filename
    , ingestion_timestamp 
    , source_id
FROM 
    cental_mapping