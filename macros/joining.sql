{% macro joining(source_name, table_name) %}
adset_id_with_default_tracking AS (
  SELECT adset_id FROM latest_ad_sets_goal WHERE adset_id NOT IN (SELECT adset_id from adset_id_with_other_offsite_event_type) AND adset_id NOT IN (
    SELECT adset_id FROM (
      SELECT adset_id FROM adset_id_with_custom_object
    )
  )
),
default_adset_tracking_goal AS (
  SELECT DISTINCT LOWER(JSON_VALUE(data,'$.optimization_goal')) AS conversion_tag,JSON_VALUE(data,'$.adset_id') AS adset_id FROM `amp-main.facebook_raw.ads_insights` WHERE JSON_VALUE(data,'$.adset_id') IN (SELECT adset_id FROM adset_id_with_default_tracking)
),
ad_to_adset AS (
    SELECT JSON_VALUE(data,'$.id') AS ad_id,JSON_VALUE(data,'$.adset_id') AS adset_id ,
    ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(data,'$.id') ORDER BY JSON_VALUE(data,'$.updated_time') DESC) AS row_num
    FROM {{ source(source_name, table_name) }}
),
deduplicate_ad_to_adset AS (
    SELECT * FROM ad_to_adset where row_num = 1
),
centralized_adset_conversion_tag AS (
  SELECT adset_id,conversion_tag FROM default_adset_tracking_goal UNION ALL 
  SELECT adset_id,conversion_tag FROM adset_id_with_other_offsite_event_type UNION ALL
  SELECT adset_id,conversion_tag FROM custom_conversion_id_joining
),
centralized_ad_conversion_tag AS (
  SELECT ad_id,conversion_tag FROM deduplicate_ad_to_adset
  LEFT JOIN centralized_adset_conversion_tag AS cnt ON deduplicate_ad_to_adset.adset_id = cnt.adset_id
)
{% endmacro %}