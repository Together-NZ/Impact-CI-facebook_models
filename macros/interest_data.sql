{% macro interest_data(source_name, table_name) %}
interest_data AS (
    SELECT
        JSON_VALUE(data, '$.id') AS ad_id,
        IFNULL(JSON_EXTRACT_ARRAY(JSON_EXTRACT(data, '$.targeting.flexible_spec')), []) AS flexible_spec_array,
        JSON_EXTRACT_ARRAY(data, '$.device_platforms') AS device
    FROM {{ source(source_name, table_name) }}
),
filtered_interest_data AS (
    SELECT
        ad_id,
        STRING_AGG(DISTINCT JSON_VALUE(interest_item, '$.name'), ', ') AS interest_names
    FROM interest_data,
    UNNEST(flexible_spec_array) AS flexible_spec_item,
    UNNEST(IFNULL(JSON_EXTRACT_ARRAY(flexible_spec_item, '$.interests'), [])) AS interest_item
    GROUP BY ad_id
)
{% endmacro %}