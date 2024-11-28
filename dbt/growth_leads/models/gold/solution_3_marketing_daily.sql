
WITH non_scrapers_daily AS (
    SELECT 
        marketing_source
        , date
        , total_commission
    FROM 
        {{ ref('solution_1_marketing_daily') }}
),
scrapers_daily AS (
    SELECT 
        marketing_source
        , date
        , total_commission
    FROM 
        {{ ref('solution_2_marketing_daily') }}
),
combined AS (
SELECT * from non_scrapers_daily
UNION
SELECT * from scrapers_daily
) 
SELECT
    marketing_source
    , date
    , SUM(total_commission) as total_commission
FROM 
    combined
GROUP BY 
    1,2