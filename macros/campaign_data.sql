{% macro campaign_data(source_name, table_name) %}
campaign_data AS (
    SELECT DISTINCT
        JSON_VALUE(data, '$.id') AS campaign_id,
        JSON_VALUE(data,'$.name') AS campaign_name,
        JSON_VALUE(data, '$.status') AS campaign_status,
        JSON_VALUE(data, '$.objective') AS campaign_objective,
        JSON_VALUE(data, '$.start_time') AS start_time,
        JSON_VALUE(data, '$.stop_time') AS stop_time,
        JSON_VALUE(data,'$.updated_time') AS updated_time
        
    FROM {{ source(source_name, table_name) }}
),
deduplicated_campaign_data AS (
    SELECT
        campaign_id,
        campaign_status,
        campaign_objective,
        start_time,
        updated_time,
        campaign_name,
        stop_time,
        ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY updated_time DESC) AS row_num
    FROM campaign_data
),
filtered_campaign_data AS (
    SELECT
        campaign_id,
        campaign_status,
        campaign_objective,
        start_time,
        campaign_name,
        stop_time
    FROM deduplicated_campaign_data
    WHERE row_num = 1
)
{% endmacro %}