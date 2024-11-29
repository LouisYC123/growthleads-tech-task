{{ config(
    materialized='incremental',
    unique_key='country_code',
    incremental_strategy='delete+insert',
) }}

WITH routy_voluum AS (
    SELECT * FROM {{ ref('union_enriched') }}
),
countries AS (
    SELECT DISTINCT 
        country_code
    FROM routy_voluum
    WHERE country_code IS NOT NULL
)

SELECT
    MD5(country_code) AS country_id
    , country_code
    , CURRENT_TIMESTAMP AS load_timestamp
FROM 
    countries
