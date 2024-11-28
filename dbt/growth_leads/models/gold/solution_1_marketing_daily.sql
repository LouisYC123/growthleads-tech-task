{{ config(
    materialized='view',
) }}


WITH traffic_events AS (
    SELECT 
        * 
    FROM 
        {{ ref('traffic_events') }}
),
calc_commision AS (
    SELECT 
        te.event_date
        , ms.marketing_source
        , te.source
        , te.clicks
        , te.raw_earnings
        , te.total_earnings
        , CASE 
            WHEN ms.deal_type = 'default' THEN te.raw_earnings
            WHEN ms.has_plus_clicks THEN 
                COALESCE(te.raw_earnings, 0) 
                + COALESCE(ms.add_amount, 0) 
                + (COALESCE(te.clicks, 0) * COALESCE(ms.clicks_multiplier, 0))
            ELSE 
                te.raw_earnings + ms.add_amount
        END AS total_commission
    FROM 
        traffic_events te
        JOIN silver.marketing_sources ms 
            ON te.marketing_source_id = ms.marketing_source_id
    WHERE 
        te.source <> 'scrapers'
)
SELECT 
    marketing_source
    , event_date
    , SUM(total_commission)
FROM 
    calc_commision
GROUP BY
    1, 2