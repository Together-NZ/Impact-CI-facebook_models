{% macro joining(source_name, table_name) %}
adset_id_with_default_tracking AS (
  SELECT adset_id
  FROM latest_ad_sets_goal
  WHERE adset_id NOT IN (SELECT adset_id FROM adset_id_with_other_offsite_event_type)
    AND adset_id NOT IN (SELECT adset_id FROM custom_conversion_id_joining)
),
default_adset_tracking_goal AS (
  SELECT adset_id, optimization_goal AS conversion_tag
  FROM latest_ad_sets_goal
  WHERE adset_id IN (SELECT adset_id FROM adset_id_with_default_tracking)
    AND optimization_goal IS NOT NULL
)
{% endmacro %}
