/*
macro to dynamically map raw data fields to dimension tables.
Accepts parameters for:
    The source table and column (e.g., raw.marketing_source).
    The dimension table and column (e.g., silver_marketing_sources.marketing_source).
    Dynamically generates the LEFT JOIN.
*/
{% macro map_to_dimension(raw_table, raw_column, dimension_table, dimension_key, dimension_column) %}
LEFT JOIN {{ ref(dimension_table) }} AS dim_{{ raw_column }}
ON {{ raw_table }}.{{ raw_column }} = dim_{{ raw_column }}.{{ dimension_column }}
{% endmacro %}
