{{ config(
    materialized='incremental',
    unique_key='traffic_event_id' 
) }}

WITH traffic_events AS (
    SELECT * FROM {{ ref('traffic_events') }}
),
marketing_sources AS (
    SELECT * FROM {{ ref('marketing_sources') }}
),
deals AS (
    SELECT * FROM {{ ref('deals') }}
),
operators AS (
    SELECT * FROM {{ ref('operators') }}
),
final AS (
SELECT
    te.traffic_event_id
    , te.event_time
    , ms.marketing_source
    , o.operator
    , d.deal_type
    , d.commision_formula
    , te.raw_earnings   
    , te.clicks
    , d.add_amount
    , d.has_plus_clicks
    , d.clicks_multiplier
    , CASE 
        WHEN 
            d.deal_type = 'default' THEN te.raw_earnings
        WHEN 
            d.has_plus_clicks THEN te.raw_earnings + d.add_amount + (te.clicks * d.clicks_multiplier)
        ELSE 
            te.raw_earnings + d.add_amount
    END AS total_commission
FROM 
    traffic_events te 
    JOIN marketing_sources ms ON te.marketing_source_id = ms.marketing_source_id
    JOIN deals d ON ms.deal_id = d.deal_id
    JOIN operators o ON te.operator_id = o.operator_id
)
SELECT * FROM final