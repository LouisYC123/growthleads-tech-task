WITH deals AS (
    SELECT * FROM {{ source('bronze', 'deals' )}}
)

SELECT
    *
    , ({{ extract_addition_number('comments') }}) AS add_amount
    , {{ contains_plus_clicks('comments') }} AS has_plus_clicks
    , {{ extract_clicks_multiplier('comments') }} AS clicks_multiplier
FROM deals
