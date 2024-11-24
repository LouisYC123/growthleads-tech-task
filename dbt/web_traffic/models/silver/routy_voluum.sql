WITH routy AS (
    SELECT * FROM {{ source('bronze', 'routy' )}}
),
voluum_mapper AS (
    SELECT * FROM {{ source('bronze', 'voluum_mapper' )}}
),
voluum AS (
    SELECT * FROM {{ source('bronze', 'voluum' )}}
),
manual AS (
    SELECT * FROM {{ source('bronze', 'manual' )}}
)

SELECT
    r.*
    , v.voluum_brand
    , v.clicks
FROM 
    routy AS r
    JOIN voluum_mapper vr
        ON r.marketing_source = vr.marketing_source
    JOIN voluum v 
        ON vr.voluum_brand = v.voluum_brand
        AND r.date = v.date

