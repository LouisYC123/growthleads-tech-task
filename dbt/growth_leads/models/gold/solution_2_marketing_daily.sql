{{ config(materialized='view') }}


WITH traffic_events AS (
    SELECT 
        * 
    FROM 
        {{ ref('traffic_events') }}
    WHERE 
        source = 'scrapers'
),
calendar AS (
    SELECT 
        calendar_date
    FROM 
         {{ ref('dim_calendar') }}
    WHERE 
        calendar_date BETWEEN (
            SELECT MIN(event_date) FROM silver.traffic_events
        ) AND (
            SELECT MAX(event_date) FROM silver.traffic_events
        )
),
daily_aggs AS (
    SELECT
        cal.calendar_date,
        cal.calendar_date - INTERVAL '1 day' AS prev_date,
        te.marketing_source_id,
        SUM(te.clicks) AS clicks,
        SUM(te.raw_earnings) AS raw_earnings,
        SUM(te.total_earnings) AS total_earnings
    FROM 
        calendar cal
        LEFT JOIN traffic_events te
            ON cal.calendar_date = te.event_date
    GROUP BY
        cal.calendar_date, te.marketing_source_id
),
daily_aggs_with_prev_earnings AS (
    SELECT
        da.calendar_date,
        da.prev_date,
        da.marketing_source_id,
        da.clicks,
        da.raw_earnings,
        da.total_earnings,
        prev_da.raw_earnings as prev_day_earnings
    FROM 
        daily_aggs da
        LEFT JOIN daily_aggs prev_da
            ON da.marketing_source_id = prev_da.marketing_source_id
            AND da.prev_date = prev_da.calendar_date
), 
final as (
SELECT 
    da.calendar_date as date
    , ms.marketing_source
    , total_earnings
    , prev_day_earnings
    , CASE 
        WHEN prev_day_earnings IS NULL THEN 0
        ELSE COALESCE(total_earnings - prev_day_earnings, 0)
    end as total_commission
FROM 
    daily_aggs_with_prev_earnings da
    join silver.marketing_sources ms
    on da.marketing_source_id = ms.marketing_source_id
ORDER BY 
    ms.marketing_source, calendar_date
)
SELECT 
    date
    , marketing_source
    , total_commission
FROM 
    final