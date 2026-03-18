{% macro ad_data(source_name, table_name) %}

ad_data AS (
    SELECT JSON_VALUE(data,'$.id') AS ad_id,
    JSON_VALUE(data,'$.name') AS ad_name,
    ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(data,'$.id') ORDER BY JSON_VALUE(data,'$.updated_time') DESC) AS row_num
    FROM {{ source(source_name, table_name) }}
),
deduplicate_ad_data AS (
    SELECT * FROM ad_data where row_num = 1
)
{% endmacro %}