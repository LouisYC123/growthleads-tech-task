{{
    config(
        materialized='incremental',  
        unique_key='source_id'  ,
        incremental_strategy='delete+insert',   
    )
}}

WITH final AS (
    {{ dbt_utils.union_relations(
        relations=[
            ref('routy_enriched'),
            ref('manual_enriched')
        ]
    ) }}
)

SELECT 
    *
FROM 
    final
{% if is_incremental() %}
WHERE source_id NOT IN (
    SELECT source_id
    FROM {{ this }}
)
{% endif %}
ORDER BY 
    ingestion_timestamp DESC