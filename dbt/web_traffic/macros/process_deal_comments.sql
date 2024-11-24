{% macro extract_addition_number(column) %}
    COALESCE((regexp_match({{ column }}, '\+ *(\d+)'))[1]::integer, 0)
{% endmacro %}

{% macro contains_plus_clicks(column) %}
    ({{ column }} ~ '\+ *clicks')
{% endmacro %}

{% macro extract_clicks_multiplier(column) %}
    COALESCE(((regexp_match({{ column }}, 'clicks\*(\d+)'))[1])::integer, 0)
{% endmacro %}
