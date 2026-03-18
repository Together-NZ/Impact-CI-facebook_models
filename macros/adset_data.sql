{% macro adset_data(source_name, table_name) %}
adset_data AS (
    select distinct json_value(data,'$.id') as adset_id,
    json_value(data,'$.name') as adset_name,
    ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(data,'$.id') ORDER BY JSON_VALUE(data,'$.updated_time') DESC) AS row_num
    from {{ source(source_name, table_name) }}
),
deduplicate_adset_data AS (
    SELECT * FROM adset_data where row_num = 1
)
{% endmacro %}