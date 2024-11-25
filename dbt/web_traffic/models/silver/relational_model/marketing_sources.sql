{{ config(
    materialized='custom_table_with_pk',
    unique_key='marketing_source_id'
) }}

{{ config(materialized='incremental') }}


WITH traffic as (
    SELECT * FROM {{ ref('routy_manual_enriched' )}}
),
deals_cleaned as (
    SELECT * FROM {{ ref('deals_cleaned' )}}
),
deals as (
    SELECT * FROM {{ ref('deals' )}}
),
marketing_sources_distinct AS (
    SELECT DISTINCT
        marketing_source
    FROM 
        traffic
),
marketing_deals_distinct AS (
    SELECT DISTINCT
        marketing_source
        , deal_type
    FROM 
        deals_cleaned
),
final as (
    SELECT 
        ms.marketing_source
        , COALESCE(d.deal_type, 'FIXED 0') as deal_type
        , CURRENT_TIMESTAMP AS load_timestamp
    FROM 
        marketing_sources_distinct AS ms
        LEFT JOIN marketing_deals_distinct AS d
        ON ms.marketing_source = d.marketing_source
)
SELECT
    ROW_NUMBER() OVER (ORDER BY f.marketing_source) AS marketing_source_id -- Generate primary key
    , f.marketing_source
    , d.deal_id
    , CURRENT_TIMESTAMP AS load_timestamp
FROM 
    final AS f
    LEFT JOIN deals d ON f.deal_type = d.deal_type

{% if is_incremental() %}
WHERE marketing_source NOT IN (SELECT marketing_source FROM {{ this }})
{% endif %}