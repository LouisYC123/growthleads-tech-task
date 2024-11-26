{{
    config(
        materialized='incremental',  
        unique_key='source_id'  ,
        incremental_strategy='delete+insert',   
    )
}}

WITH manual AS (
    SELECT * FROM {{ ref('manual_cleaned') }}
),
voluum_mapper AS (
    SELECT * FROM {{ ref('voluum_mapper_cleaned') }}
),
voluum AS (
    SELECT * FROM {{ ref('voluum_cleaned' )}}
),
final as (
SELECT
    m.*
    , v.voluum_brand
    , COALESCE(v.clicks, 0) as clicks
FROM 
    manual AS m
    LEFT JOIN voluum_mapper vr
        ON m.marketing_source = vr.marketing_source
    LEFT JOIN voluum v 
        ON vr.voluum_brand = v.voluum_brand
        AND m.filename = v.filename
)

SELECT 
    *
FROM 
    final
{% if is_incremental() %}
WHERE source_id NOT IN (
    SELECT source_id
    FROM {{ this }}
)
{% endif %}
