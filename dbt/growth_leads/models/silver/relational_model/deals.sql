{{ config(materialized='incremental') }}

WITH deals_cleaned AS (
    SELECT * FROM {{ ref('deals_cleaned') }}
),
deals_distinct AS (
    SELECT DISTINCT 
        deal_type
        , commision_formula
        , add_amount
        , has_plus_clicks
        , clicks_multiplier
    FROM deals_cleaned
)

SELECT
    ROW_NUMBER() OVER (ORDER BY deal_type) AS deal_id -- PRIMARY KEY
    , deal_type
    , commision_formula
    , add_amount
    , has_plus_clicks
    , clicks_multiplier
    , CURRENT_TIMESTAMP AS load_timestamp
FROM 
    deals_distinct

{% if is_incremental() %}
WHERE deal_type NOT IN (SELECT deal_type FROM {{ this }})
{% endif %}
