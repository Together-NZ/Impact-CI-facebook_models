{% macro conversion_goal(source_name, table_name) %}
latest_ad_sets_goal AS (
  SELECT adset_name,
  adset_id,adset_tracking_goal
        FROM ( SELECT
          JSON_VALUE(data,'$.name') AS adset_name,
          JSON_VALUE(data,'$.id') AS adset_id,
          JSON_EXTRACT(data,'$.promoted_object') AS adset_tracking_goal,
        ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(data,'$.id') ORDER BY JSON_VALUE(data,'$.updated_time') DESC) AS row_num
        FROM {{ source(source_name, table_name) }}
        )
        WHERE row_num=1
),
adset_id_with_custom_object AS (
  SELECT adset_id,adset_tracking_goal,JSON_VALUE(adset_tracking_goal,'$.pixel_rule') AS custom_mapping_rule FROM latest_ad_sets_goal WHERE JSON_VALUE(adset_tracking_goal,'$.pixel_rule') IS NOT NULL
)
{% endmacro %}