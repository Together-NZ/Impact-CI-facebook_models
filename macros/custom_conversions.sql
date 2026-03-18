{% macro custom_conversions(source_name, table_name) %}
custom_conversion_stats AS (
  SELECT DISTINCT JSON_VALUE(data,'$.id') AS conversion_id,JSON_VALUE(data,'$.name') AS conversion_name,JSON_VALUE(data,'$.rule') AS custom_mapping_rule FROM 
  {{ source(source_name, table_name) }}
),
custom_conversion_id_joining AS (
  SELECT conversion_id AS conversion_tag,adset_id FROM adset_id_with_custom_object AS ad LEFT JOIN custom_conversion_stats AS cs ON cs.custom_mapping_rule=ad.custom_mapping_rule
),
adset_id_with_other_offsite_event_type AS (
  SELECT  LOWER(JSON_VALUE(adset_tracking_goal,'$.custom_event_type')) AS conversion_tag,adset_id FROM latest_ad_sets_goal WHERE JSON_VALUE(adset_tracking_goal,'$.custom_event_type') !='OTHER' AND adset_id NOT IN (
    SELECT adset_id FROM custom_conversion_id_joining)
)
{% endmacro %}