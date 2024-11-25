WITH manual AS (
    SELECT * FROM {{ source('bronze', 'manual' )}}
),
central_mapping AS (
    SELECT * FROM {{ ref('central_mapping_cleaned' )}}
)
SELECT 
    COALESCE(m.date, CAST(REPLACE(m.filename, '.csv', '') AS TIMESTAMP)) AS event_time
    , m.marketing_source
    , m.operator
    , cm.country_code
    , COALESCE(m.raw_earnings, 0) as raw_earnings
    , COALESCE(m.visits, 0) as visits
    , COALESCE(m.signups, 0) as signups
    , m.source 
    , m.filename
    , m.ingestion_timestamp 
    , m.source_id
    
FROM 
    manual m
    JOIN central_mapping cm ON m.operator = cm.variants 
        OR m.operator = cm.std_operator 

