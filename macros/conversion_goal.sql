{% macro conversion_goal(source_name, table_name, insights_table_name='ads_insights') %}
latest_ad_sets_raw AS (
  SELECT
    adset_name,
    adset_id,
    adset_tracking_goal,
    optimization_goal
  FROM (
    SELECT
      JSON_VALUE(data, '$.name') AS adset_name,
      JSON_VALUE(data, '$.id') AS adset_id,
      JSON_EXTRACT(data, '$.promoted_object') AS adset_tracking_goal,
      LOWER(JSON_VALUE(data, '$.optimization_goal')) AS optimization_goal,
      ROW_NUMBER() OVER (
        PARTITION BY JSON_VALUE(data, '$.id')
        ORDER BY JSON_VALUE(data, '$.updated_time') DESC
      ) AS row_num
    FROM {{ source(source_name, table_name) }}
  )
  WHERE row_num = 1
),
latest_insights_goal AS (
  SELECT
    adset_id,
    optimization_goal
  FROM (
    SELECT
      JSON_VALUE(data, '$.adset_id') AS adset_id,
      LOWER(JSON_VALUE(data, '$.optimization_goal')) AS optimization_goal,
      ROW_NUMBER() OVER (
        PARTITION BY JSON_VALUE(data, '$.adset_id')
        ORDER BY JSON_VALUE(data, '$.date_start') DESC, _sdc_extracted_at DESC
      ) AS row_num
    FROM {{ source(source_name, insights_table_name) }}
    WHERE JSON_VALUE(data, '$.optimization_goal') IS NOT NULL
  )
  WHERE row_num = 1
),
latest_ad_sets_goal AS (
  SELECT
    COALESCE(ad.adset_name, ins.adset_id) AS adset_name,
    COALESCE(ad.adset_id, ins.adset_id) AS adset_id,
    ad.adset_tracking_goal,
    COALESCE(ad.optimization_goal, ins.optimization_goal) AS optimization_goal
  FROM latest_ad_sets_raw AS ad
  FULL OUTER JOIN latest_insights_goal AS ins
    ON ad.adset_id = ins.adset_id
),
adset_id_with_custom_object AS (
  SELECT
    adset_id,
    adset_tracking_goal,
    JSON_VALUE(adset_tracking_goal, '$.pixel_rule') AS custom_mapping_rule
  FROM latest_ad_sets_goal
  WHERE JSON_VALUE(adset_tracking_goal, '$.pixel_rule') IS NOT NULL
)
{% endmacro %}
