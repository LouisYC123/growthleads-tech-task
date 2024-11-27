{{
    config(
        materialized='incremental',
        unique_key='source_id',
        incremental_strategy='delete+insert',
    )
}}

{% if does_table_exist('bronze', 'manual') %}
WITH manual AS (
    SELECT * FROM {{ source('bronze', 'manual') }}
),
central_mapping AS (
    SELECT * FROM {{ ref('central_mapping_cleaned') }}
),
final AS (
    SELECT 
        COALESCE(m.date, CAST(REPLACE(m.filename, '.csv', '') AS TIMESTAMP)) AS event_time,
        COALESCE(CAST(m.date AS DATE), CAST(REPLACE(m.filename, '.csv', '') AS DATE)) AS event_date,
        m.marketing_source,
        m.operator,
        cm.country_code,
        COALESCE(m.raw_earnings, 0) AS raw_earnings,
        0 AS total_earnings,
        COALESCE(m.visits, 0) AS visits,
        COALESCE(m.signups, 0) AS signups,
        m.source,
        m.filename,
        m.ingestion_timestamp,
        m.event_id,
        m.source_id
    FROM 
        manual m
    LEFT JOIN central_mapping cm 
        ON m.operator = cm.variants 
        OR m.operator = cm.operator
)
{% else %}
-- If the source does not exist or is empty, return a single row with NULL values
WITH final AS (
    SELECT 
        NULL AS event_time,
        NULL AS event_date,
        NULL AS marketing_source,
        NULL AS operator,
        NULL AS country_code,
        NULL AS raw_earnings,
        NULL AS total_earnings,
        NULL AS visits,
        NULL AS signups,
        NULL AS source,
        NULL AS filename,
        NULL AS ingestion_timestamp,
        NULL AS event_id,
        NULL AS source_id
)
{% endif %}

-- Final output
SELECT * FROM final WHERE event_time IS NOT NULL
