
WITH non_scrapers_monthly AS (
    SELECT 
        operator
        , month
        , total_commission
    FROM 
        gold.solution_1_operators_monthly
),
scrapers_monthly AS (
    SELECT 
        operator
        , month
        , total_commission
    FROM 
        gold.solution_2_operators_monthly
),
combined AS (
SELECT * from non_scrapers_monthly
UNION
SELECT * from scrapers_monthly
) 
SELECT
    operator
    , month
    , SUM(total_commission) as total_commission
FROM 
    combined
GROUP BY 
    1,2

