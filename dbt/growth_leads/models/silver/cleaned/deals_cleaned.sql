WITH deals AS (
    SELECT * FROM {{ source('bronze', 'deals' )}}
)

SELECT
    marketing_source
    , deal AS deal_type
    , comments AS commision_formula
    , ({{ extract_addition_number('comments') }}) AS add_amount
    , {{ contains_plus_clicks('comments') }} AS has_plus_clicks
    , {{ extract_clicks_multiplier('comments') }} AS clicks_multiplier
    , source 
    , filename
    , ingestion_timestamp 
    , source_id
FROM deals
