{{ config(
    materialized='incremental',
    unique_key='marketing_source_id'
) }}

WITH traffic AS (
    SELECT DISTINCT marketing_source FROM {{ ref('union_enriched') }}
),
deals_cleaned AS (
    SELECT * FROM {{ ref('deals_cleaned') }}
),
final AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY d.marketing_source) AS marketing_source_id
        , ms.marketing_source
        , COALESCE(d.deal_type, 'FIXED 0') AS deal_type
        , commision_formula
        , add_amount
        , has_plus_clicks
        , clicks_multiplier
        , CURRENT_TIMESTAMP AS load_timestamp
    FROM 
        traffic AS ms
        LEFT JOIN deals_cleaned AS d
        ON ms.marketing_source = d.marketing_source
)

SELECT * FROM final
