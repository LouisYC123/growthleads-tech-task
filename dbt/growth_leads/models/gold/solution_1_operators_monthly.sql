WITH traffic_events AS (
    SELECT 
        * 
    FROM 
        {{ ref('traffic_events') }}
),
operators AS (
    SELECT 
        operator_id
        , operator
    FROM 
        {{ ref('operators') }}
),
calendar AS (
    SELECT 
        calendar_date,
        month
    FROM 
        {{ ref('dim_calendar') }}
),
calc_commission AS (
    SELECT 
        te.event_date
        , ms.marketing_source
        , o.operator
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
        JOIN operators o
            ON te.operator_id = o.operator_id
    WHERE 
        te.source <> 'scrapers'
),
calc_commission_with_month AS (
    SELECT 
        cc.operator
        , cal.month
        , cc.total_commission
    FROM 
        calc_commission cc
        JOIN calendar cal
            ON cc.event_date = cal.calendar_date
)
SELECT 
    operator
    , month
    , SUM(total_commission) AS total_commission
FROM 
    calc_commission_with_month
GROUP BY 
    1,2
ORDER BY 
    1,2
