{{ config(
    materialized='incremental',
    unique_key='source_id',
    incremental_strategy='delete+insert',
) }}

WITH traffic AS (
    SELECT * FROM {{ ref('union_enriched') }}
),
marketing_sources AS (
    SELECT * FROM {{ ref('marketing_sources') }}
),
operators AS (
    SELECT * FROM {{ ref('operators') }}
),
country AS (
    SELECT * FROM {{ ref('country') }}
),
traffic_mapped AS (
    -- Map dimensions to their respective IDs
    SELECT
        t.event_time
        , t.event_date
        , ms.marketing_source_id
        , op.operator_id
        , c.country_id
        , t.clicks
        , t.visits
        , t.signups
        , t.raw_earnings
        , t.total_earnings
        , t.source
        , t.filename
        , t.source_id
        , CURRENT_TIMESTAMP AS load_timestamp
    FROM traffic t
    LEFT JOIN marketing_sources AS ms
        ON t.marketing_source = ms.marketing_source
    LEFT JOIN operators AS op
        -- not typically advisable, but for this small dataset should be ok
        ON t.operator = op.variants OR t.operator = op.operator
    LEFT JOIN country AS c
        ON t.country_code = c.country_code
),

traffic_deduplicated AS (
    SELECT DISTINCT *
    FROM traffic_mapped
),
final as (
SELECT
    ROW_NUMBER() OVER (
        ORDER BY d.event_time, d.load_timestamp
        ) AS traffic_event_id 
    , d.event_time
    , d.event_date
    , d.marketing_source_id
    , d.operator_id
    , d.country_id
    , d.clicks
    , d.visits
    , d.signups
    , d.raw_earnings
    , d.total_earnings
    , d.source_id
    , d.source
    , d.filename
    , CURRENT_TIMESTAMP AS load_timestamp
FROM traffic_deduplicated AS d
)
SELECT 
    *
FROM 
    final