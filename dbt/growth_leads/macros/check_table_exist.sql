{% macro does_table_exist(schema, table) %}
    {% set relation = adapter.get_relation(database=target.database, schema=schema, identifier=table) %}
    {{ return(relation is not none) }}
{% endmacro %}
