WITH commision_calculated AS (
    SELECT * FROM {{ ref('solution_1') }}
),
calendar AS (
    SELECT * FROM {{ ref('dim_calendar') }}
)
SELECT 
    cc.operator
    ,c.month
    , SUM(cc.total_commission) AS total_commission
FROM 
    commision_calculated cc
    JOIN calendar c ON DATE(cc.event_time) = c.calendar_date
GROUP BY
    cc.operator
    , c.month
ORDER BY
    cc.operator
    , c.month