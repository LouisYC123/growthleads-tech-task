{{ config(materialized='incremental') }}

WITH routy_voluum AS (
    SELECT * FROM {{ ref('union_enriched') }}
),
countries AS (
    SELECT DISTINCT 
        country_code
    FROM routy_voluum
)

SELECT
    ROW_NUMBER() OVER (ORDER BY country_code) AS country_id -- PRIMARY KEY
    , country_code
    , CURRENT_TIMESTAMP AS load_timestamp
FROM 
    countries
{% if is_incremental() %}
WHERE country_code NOT IN (SELECT country_code FROM {{ this }})
{% endif %}