{{ config(
    materialized='incremental',
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
calendar AS (
    SELECT calendar_date AS event_date
    FROM {{ ref('dim_calendar') }}
),
daily_totals AS (
    SELECT
        event_date,
        marketing_source_id,
        source,
        SUM(clicks) AS clicks,
        SUM(raw_earnings) AS raw_earnings,
        SUM(total_earnings) AS total_earnings
    FROM 
        traffic_events
    GROUP BY 
        event_date, marketing_source_id, source
),
date_filled AS (
    SELECT 
        c.event_date,
        m.marketing_source_id,
        d.source,
        COALESCE(d.clicks, 0) AS clicks,
        COALESCE(d.raw_earnings, 0) AS raw_earnings,
        COALESCE(d.total_earnings, 0) AS total_earnings
    FROM 
        calendar c
    CROSS JOIN (SELECT DISTINCT marketing_source_id FROM daily_totals) m
    LEFT JOIN daily_totals d
        ON c.event_date = d.event_date
        AND m.marketing_source_id = d.marketing_source_id
),
scrapers_totals AS (
    SELECT 
        event_date,
        marketing_source_id,
        source,
        clicks,
        raw_earnings,
        total_earnings,
        COALESCE(
            total_earnings - LAG(total_earnings, 1, 0) OVER (
                PARTITION BY marketing_source_id 
                ORDER BY event_date DESC
            ),
            0
        ) AS total_commission,
        CAST(NULL AS NUMERIC) AS add_amount,        
        CAST(NULL AS BOOLEAN) AS has_plus_clicks,   
        CAST(NULL AS NUMERIC) AS clicks_multiplier
    FROM 
        date_filled
    WHERE 
        source = 'scrapers'
),
non_scrapers_totals AS (
    SELECT 
        dt.event_date,
        dt.marketing_source_id,
        dt.source,
        dt.clicks,
        dt.raw_earnings,
        dt.total_earnings,
        CASE 
            WHEN d.deal_type = 'default' THEN dt.raw_earnings
            WHEN d.has_plus_clicks THEN 
                COALESCE(dt.raw_earnings, 0) 
                + COALESCE(d.add_amount, 0) 
                + (COALESCE(dt.clicks, 0) * COALESCE(d.clicks_multiplier, 0))
            ELSE 
                dt.raw_earnings + d.add_amount
        END AS total_commission,
        d.add_amount::NUMERIC AS add_amount,         
        d.has_plus_clicks::BOOLEAN AS has_plus_clicks,
        d.clicks_multiplier::NUMERIC AS clicks_multiplier
    FROM 
        date_filled dt
        JOIN marketing_sources ms 
            ON dt.marketing_source_id = ms.marketing_source_id
        JOIN deals d 
            ON ms.deal_id = d.deal_id
    WHERE 
        dt.source <> 'scrapers'
)
SELECT 
    event_date,
    marketing_source_id,
    source,
    clicks,
    raw_earnings,
    total_earnings,
    CASE 
        WHEN total_commission < 0 THEN 0
        ELSE total_commission
    END AS total_commission,
    add_amount,
    has_plus_clicks,
    clicks_multiplier
FROM 
    scrapers_totals
UNION ALL
SELECT 
    event_date,
    marketing_source_id,
    source,
    clicks,
    raw_earnings,
    total_earnings,
    CASE 
        WHEN total_commission < 0 THEN 0
        ELSE total_commission
    END AS total_commission,
    add_amount,
    has_plus_clicks,
    clicks_multiplier
FROM 
    non_scrapers_totals
