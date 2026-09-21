{% macro joining(source_name, table_name) %}
default_adset_tracking_goal AS (
  SELECT adset_id, optimization_goal AS conversion_tag
  FROM latest_ad_sets_goal
  WHERE optimization_goal IS NOT NULL
)
{% endmacro %}
