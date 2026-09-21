{% macro joining(source_name, table_name) %}
default_adset_tracking_goal AS (
  SELECT CAST(NULL AS STRING) AS adset_id, CAST(NULL AS STRING) AS conversion_tag
  FROM latest_ad_sets_goal
  WHERE FALSE
)
{% endmacro %}
