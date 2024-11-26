{{ config(
    materialized='incremental',
    unique_key='traffic_event_id' 
) }}

WITH commision_calculated AS (
    SELECT * FROM {{ ref('solution_1') }}
),
calendar AS (
    SELECT * FROM {{ ref('dim_calendar') }}
)
SELECT 
    cc.marketing_source
    , c.calendar_date AS date
    , SUM(cc.total_commission) AS total_commission
FROM 
    commision_calculated cc
    JOIN calendar c ON DATE(cc.event_time) = c.calendar_date
GROUP BY
    cc.marketing_source
    , c.calendar_date
ORDER BY
    cc.marketing_source
    , date