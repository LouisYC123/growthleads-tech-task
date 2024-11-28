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
        calendar_date,
        month
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
        cal.month,
        cal.calendar_date - INTERVAL '1 day' AS prev_date,
        te.operator_id,
        SUM(te.clicks) AS clicks,
        SUM(te.raw_earnings) AS raw_earnings,
        SUM(te.total_earnings) AS total_earnings
    FROM 
        calendar cal
        LEFT JOIN traffic_events te
            ON cal.calendar_date = te.event_date
    GROUP BY
        cal.calendar_date, cal.month, te.operator_id
),
daily_aggs_with_prev_earnings AS (
    SELECT
        da.calendar_date,
        da.month,
        da.prev_date,
        da.operator_id,
        da.clicks,
        da.raw_earnings,
        da.total_earnings,
        prev_da.raw_earnings as prev_day_earnings
    FROM 
        daily_aggs da
        LEFT JOIN daily_aggs prev_da
            ON da.operator_id = prev_da.operator_id
            AND da.prev_date = prev_da.calendar_date
), 
final AS (
    SELECT 
        da.calendar_date AS date,
        da.month,
        o.operator,
        da.total_earnings,
        da.prev_day_earnings,
        CASE 
            WHEN prev_day_earnings IS NULL THEN 0
            ELSE COALESCE(da.total_earnings - da.prev_day_earnings, 0)
        END AS total_commission
    FROM 
        daily_aggs_with_prev_earnings da
        JOIN silver.operators o
            ON da.operator_id = o.operator_id
)
SELECT 
    month,
    operator,
    SUM(total_commission) AS total_commission
FROM 
    final
GROUP BY 
    month, operator
ORDER BY 
    month, operator