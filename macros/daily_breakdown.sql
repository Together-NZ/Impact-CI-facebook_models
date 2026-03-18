{% macro daily_breakdown(source_name, table_name) %}
ranked_data AS (
    SELECT
        JSON_VALUE(data, '$.account_currency') AS account_currency,
        JSON_VALUE(data, '$.account_id') AS account_id,
        JSON_VALUE(data, '$.account_name') AS account_name,
        JSON_VALUE(data, '$.ad_id') AS ad_id,
        --JSON_VALUE(data, '$.ad_name') AS ad_name,
        JSON_VALUE(data, '$.adset_id') AS adset_id,
        --JSON_VALUE(data, '$.adset_name') AS adset_name,
        JSON_VALUE(data, '$.campaign_id') AS campaign_id,
        JSON_EXTRACT_ARRAY(data, '$.conversions') AS conversion_array,
        SAFE_CAST(JSON_VALUE(data, '$.clicks') AS INT64) AS clicks, 
        SAFE_CAST(JSON_VALUE(data, '$.impressions') AS INT64) AS impressions, 
        JSON_VALUE(data, '$.ctr') AS ctr,
        SAFE_CAST(JSON_VALUE(data,'$.frequency') AS FLOAT64) AS frequency,
        SAFE_CAST(JSON_VALUE(data, '$.spend') AS FLOAT64) AS spend,
        SAFE_CAST(JSON_VALUE(data,'$.reach') AS INT64) AS reach,
        JSON_VALUE(data, '$.date_start') AS date_start,
        JSON_VALUE(data, '$.date_stop') AS date_stop,
        JSON_EXTRACT(data, '$.video_p25_watched_actions') AS video_p25_actions,
        JSON_EXTRACT(data, '$.video_p50_watched_actions') AS video_p50_actions,
        JSON_EXTRACT(data, '$.video_p75_watched_actions') AS video_p75_actions,
        JSON_EXTRACT(data, '$.video_p100_watched_actions') AS video_p100_actions,
        JSON_EXTRACT(data, '$.video_15_sec_watched_actions') AS video_15_sec_watched_actions,
        JSON_EXTRACT(data, '$.video_play_actions') AS video_play_actions_array,
        JSON_EXTRACT(data, '$.actions') AS video_view_actions_array,
        JSON_EXTRACT(data, '$.actions') AS actions,
        ROW_NUMBER() OVER (
            PARTITION BY 
                JSON_VALUE(data, '$.account_id'),
                JSON_VALUE(data, '$.ad_id'),
                JSON_VALUE(data, '$.date_start')
            ORDER BY 
                _sdc_extracted_at DESC
        ) AS row_number
    FROM {{ source(source_name, table_name) }}
),
deduplicated_data AS (
    SELECT *
    FROM ranked_data
    WHERE row_number = 1
),
conversion_array AS (
    SELECT conversion_array, ad_id FROM deduplicated_data
),
flattened_video_actions AS (
    SELECT
        date_start,
        ad_id,
        --ad_name,
        adset_id,
        --adset_name,
        clicks,
        spend,
        campaign_id,
        reach,
        frequency,
        impressions,
        actions,
        JSON_EXTRACT_ARRAY(video_play_actions_array) AS video_play_array,
        JSON_EXTRACT_ARRAY(video_p25_actions) AS video_p25_array,
        JSON_EXTRACT_ARRAY(video_p50_actions) AS video_p50_array,
        JSON_EXTRACT_ARRAY(video_p75_actions) AS video_p75_array,
        JSON_EXTRACT_ARRAY(video_15_sec_watched_actions) AS video_15_sec_watched_actions,
        JSON_EXTRACT_ARRAY(deduplicated_data.video_p100_actions) AS video_p100_array
    FROM deduplicated_data
)
{% endmacro %}