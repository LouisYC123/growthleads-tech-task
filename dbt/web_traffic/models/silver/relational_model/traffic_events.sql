{{ config(
    materialized='incremental',
    unique_key='traffic_event_id' 
) }}

WITH traffic AS (
    SELECT * FROM {{ ref('routy_manual_enriched') }}
),

traffic_mapped AS (
    -- Map dimensions to their respective IDs
    SELECT
        t.event_time
        , ms.marketing_source_id
        , op.operator_id
        , c.country_id
        , t.clicks
        , t.visits
        , t.signups
        , t.raw_earnings
        , t.source
        , t.filename
        , t.source_id
        , CURRENT_TIMESTAMP AS load_timestamp
    FROM traffic t
    -- Map marketing_source to marketing_source_id
    LEFT JOIN {{ ref('marketing_sources') }} AS ms
        ON t.marketing_source = ms.marketing_source
    -- Map operator to operator_id
    LEFT JOIN {{ ref('operators') }} AS op
            -- not typically advisable, but for this small dataset should be ok
        ON t.operator = op.variants OR t.operator = op.std_operator
    -- Map voluum_brand to brand_id
    LEFT JOIN {{ ref('country') }} AS c
        ON t.country_code = c.country_code
),

traffic_deduplicated AS (
    -- Remove duplicates to ensure one row per unique event
    SELECT DISTINCT *
    FROM traffic_mapped
)

SELECT
    ROW_NUMBER() OVER (
        ORDER BY d.event_time, d.load_timestamp
        ) AS traffic_event_id -- Generate primary key
    , d.event_time
    , d.marketing_source_id
    , d.operator_id
    , d.country_id
    , d.clicks
    , d.visits
    , d.signups
    , d.raw_earnings
    , d.source_id
    , d.source
    , d.filename
    , CURRENT_TIMESTAMP AS load_timestamp
FROM traffic_deduplicated AS d

{% if is_incremental() %}
WHERE d.event_time NOT IN (
    SELECT event_time FROM {{ this }}
)
{% endif %}