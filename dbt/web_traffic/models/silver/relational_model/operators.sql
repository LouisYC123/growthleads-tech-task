WITH operators AS (
    SELECT * FROM {{ ref('central_mapping_cleaned') }}
),
country AS (
    SELECT * FROM {{ ref('country') }}
),
operators_distinct AS (
    SELECT DISTINCT 
        std_operator
        , variants
        , country_code
    FROM operators
)

SELECT
    ROW_NUMBER() OVER (ORDER BY od.std_operator) AS operator_id -- PRIMARY KEY
    , od.std_operator
    , od.variants
    , c.country_id
    , CURRENT_TIMESTAMP load_timestamp
FROM 
    operators_distinct od
    LEFT JOIN country c ON od.country_code = c.country_code

{% if is_incremental() %}
WHERE std_operator NOT IN (SELECT std_operator FROM {{ this }})
{% endif %}
