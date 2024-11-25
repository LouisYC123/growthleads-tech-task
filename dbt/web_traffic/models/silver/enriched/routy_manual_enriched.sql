WITH temp AS (
    {{ dbt_utils.union_relations(
        relations=[
            ref('routy_enriched'),
            ref('manual_enriched')
        ]
    ) }}
)
SELECT * FROM temp
