-- models/gold/total_commission_by_source_month.sql

-- TODO - check then remove this and assign centrally
-- {{ config(materialized='view') }}

WITH data AS (
    SELECT * FROM {{ ref('enriched') }}
)

SELECT
    DATE_TRUNC('month', data.date) AS month,
    data.marketing_source,
    SUM(data.total_commission) AS total_commission
FROM 
    data
GROUP BY
    month,
    data.marketing_source
ORDER BY
    month,
    data.marketing_source
