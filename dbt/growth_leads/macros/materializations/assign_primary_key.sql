{% materialization custom_table_with_pk, default -%}
{%- set unique_key = config.get('unique_key', none) -%}

-- Drop table if it exists (only for full refresh)
{% if not is_incremental() %}
DROP TABLE IF EXISTS {{ this }};
{% endif %}

-- Create the table
CREATE TABLE {{ this }} AS
{% if is_incremental() %}
-- Incremental logic: Append new rows only
SELECT * FROM ({{
    sql
}}) as new_rows
WHERE {{ unique_key }} NOT IN (SELECT {{ unique_key }} FROM {{ this }})
{% else %}
-- Full refresh: Load all rows
{{ sql }}
{% endif %}

-- Add the Primary Key constraint
{% if unique_key %}
ALTER TABLE {{ this }} ADD CONSTRAINT {{ this.name }}_pk PRIMARY KEY ({{ unique_key }});
{% endif %}
{%- endmaterialization %}
