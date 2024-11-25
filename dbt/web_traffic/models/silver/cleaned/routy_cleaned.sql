WITH routy AS (
    SELECT * FROM {{ source('bronze', 'routy' )}}
)
SELECT 
    COALESCE(date, CAST(REPLACE(filename, '.csv', '') AS TIMESTAMP)) AS event_time
    , marketing_source
    , operator
    , COALESCE(country_code, country) as country_code
    , COALESCE(raw_earnings, 0) as raw_earnings
    , COALESCE(visits, 0) as visits
    , COALESCE(signups, 0) as signups
    , source 
    , filename
    , ingestion_timestamp 
    , source_id
FROM 
    routy
WHERE
    marketing_source IN (
        'TOP-SPO1'
        , 'LOW-SPO1'
        , 'LOW-SPO2'
        , 'LOW-SPO21'
        , 'TOP-FR'
        , 'LOW-FR12'
        , 'GRAUTO'
        , 'FRAUTO'
    )
