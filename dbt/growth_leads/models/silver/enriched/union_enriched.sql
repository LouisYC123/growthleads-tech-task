{{
    config(
        materialized='incremental',
        unique_key='source_id',
        incremental_strategy='delete+insert',
    )
}}

-- Explicitly declare dependencies to ensure dbt understands them
-- depends_on: {{ ref('routy_cleaned') }}
-- depends_on: {{ ref('voluum_cleaned') }}
-- depends_on: {{ ref('manual_cleaned') }}
-- depends_on: {{ ref('scrapers_cleaned') }}

{% 
    set relations = [] 
%}

{% if adapter.get_relation(database=this.database, schema=this.schema, identifier='routy_cleaned') %}
    {% do relations.append(ref('routy_cleaned')) %}
{% endif %}
{% if adapter.get_relation(database=this.database, schema=this.schema, identifier='manual_cleaned') %}
    {% do relations.append(ref('manual_cleaned')) %}
{% endif %}
{% if adapter.get_relation(database=this.database, schema=this.schema, identifier='scrapers_cleaned') %}
    {% do relations.append(ref('scrapers_cleaned')) %}
{% endif %}

WITH unioned AS (
    {{ dbt_utils.union_relations(
        relations=relations
    ) }}
),
voluum_mapper AS (
    SELECT * FROM {{ ref('voluum_mapper_cleaned') }}
),
voluum AS (
    SELECT * FROM {{ ref('voluum_cleaned') }}
),
final as (
SELECT
    m.*
    , v.voluum_brand
    , COALESCE(v.clicks, 0) as clicks
FROM 
    unioned AS m
    LEFT JOIN voluum_mapper vr
        ON m.marketing_source = vr.marketing_source
    LEFT JOIN voluum v 
        ON vr.voluum_brand = v.voluum_brand
        AND m.event_time = v.event_time
)

SELECT 
    *
FROM 
    final
ORDER BY 
    ingestion_timestamp DESC
