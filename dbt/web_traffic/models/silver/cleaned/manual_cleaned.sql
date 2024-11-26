{{
    config(
        materialized='incremental',  
        unique_key='source_id',
        incremental_strategy='delete+insert',
    )
}}


WITH manual AS (
    SELECT * FROM {{ source('bronze', 'manual') }}
),
central_mapping AS (
    SELECT * FROM {{ ref('central_mapping_cleaned') }}
),
final AS (
    SELECT 
        COALESCE(m.date, CAST(REPLACE(m.filename, '.csv', '') AS TIMESTAMP)) AS event_time,
        m.marketing_source,
        m.operator,
        cm.country_code,
        COALESCE(m.raw_earnings, 0) AS raw_earnings,
        COALESCE(m.visits, 0) AS visits,
        COALESCE(m.signups, 0) AS signups,
        m.source,
        m.filename,
        m.ingestion_timestamp,
        m.event_id,
        m.source_id
    FROM 
        manual m
    JOIN central_mapping cm 
        ON m.operator = cm.variants 
        OR m.operator = cm.operator
)

SELECT * FROM final