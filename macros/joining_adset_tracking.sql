{% macro joining(source_name, table_name) %}
adset_id_with_default_tracking AS (
  SELECT adset_id FROM latest_ad_sets_goal WHERE adset_id NOT IN (SELECT adset_id from adset_id_with_other_offsite_event_type) AND adset_id NOT IN (
    SELECT adset_id FROM (
      SELECT adset_id FROM adset_id_with_custom_object
    )
  )
),
default_adset_tracking_goal AS (
  SELECT DISTINCT LOWER(JSON_VALUE(data,'$.optimization_goal')) AS conversion_tag,JSON_VALUE(data,'$.adset_id') AS adset_id FROM {{ source(source_name, table_name) }} WHERE JSON_VALUE(data,'$.adset_id') IN (SELECT adset_id FROM adset_id_with_default_tracking)
)
{% endmacro %}