WITH operators AS (
    SELECT * FROM {{ ref('central_mapping_cleaned') }}
),
country AS (
    SELECT * FROM {{ ref('country') }}
),
operators_distinct AS (
    SELECT DISTINCT 
        operator
        , variants
        , country_code
    FROM operators
)

SELECT
    ROW_NUMBER() OVER (ORDER BY od.operator) AS operator_id -- PRIMARY KEY
    , od.operator
    , od.variants
    , c.country_id
    , CURRENT_TIMESTAMP load_timestamp
FROM 
    operators_distinct od
    LEFT JOIN country c ON od.country_code = c.country_code

{% if is_incremental() %}
WHERE operator NOT IN (SELECT operator FROM {{ this }})
{% endif %}
