WITH manual AS (
    SELECT * FROM {{ ref('manual_cleaned') }}
),
voluum_mapper AS (
    SELECT * FROM {{ ref('voluum_mapper_cleaned') }}
),
voluum AS (
    SELECT * FROM {{ ref('voluum_cleaned' )}}
),
final as (
SELECT
    m.*,
    v.voluum_brand,
    v.clicks
FROM 
    manual AS m
    JOIN voluum_mapper vr
        ON m.marketing_source = vr.marketing_source
    JOIN voluum v 
        ON vr.voluum_brand = v.voluum_brand
        -- Note sure about this
        -- AND r.event_date = v.event_date
)
SELECT * FROM final