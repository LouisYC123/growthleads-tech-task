{{ config(
    materialized='incremental',
    unique_key='source_id',
    incremental_strategy='delete+insert',
) }}


WITH operators AS (
    SELECT * FROM {{ ref('central_mapping_cleaned') }}
),
country AS (
    SELECT * FROM {{ ref('country') }}
),
operators_distinct AS (
    SELECT DISTINCT 
        operator
        , variants
        , country_code
        , source_id
    FROM operators
)

SELECT
    MD5(operator) AS operator_id
    , od.operator
    , od.variants
    , c.country_id
    , od.source_id
    , CURRENT_TIMESTAMP load_timestamp
FROM 
    operators_distinct od
    LEFT JOIN country c ON od.country_code = c.country_code
