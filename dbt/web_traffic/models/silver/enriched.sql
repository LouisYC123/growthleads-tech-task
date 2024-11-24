with temp as(
{{ dbt_utils.union_relations(
    relations=[
        ref('routy_voluum'),
        source('bronze', 'manual')
    ]
) }}
)
SELECT 
    t.* 
    , d.deal
    , d.comments
    , d.add_amount
    , d.has_plus_clicks
    , d.clicks_multiplier
    , CASE 
        WHEN d.deal = 'default' THEN t.raw_earnings
        WHEN d.has_plus_clicks THEN t.raw_earnings + d.add_amount + (t.clicks * d.clicks_multiplier)
        ELSE t.raw_earnings + d.add_amount
    END AS total_commission
FROM 
    temp t JOIN silver.deals d 
    ON t.marketing_source = d.marketing_source
