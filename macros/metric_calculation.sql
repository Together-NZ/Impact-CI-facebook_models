{% macro metric_calculation() %}
parsed_video_actions AS (
 SELECT
    date_start,
    va.ad_id,
    va.adset_id,
    spend,
    clicks,
    campaign_id,
    impressions,
    conversion_tag,
    CASE WHEN conversion_tag ='reach'
        THEN NULL
        WHEN conversion_tag = 'impressions'
        THEN impressions
        WHEN conversion_tag = 'thruplay'
        THEN 
            SAFE_CAST((
                SELECT SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(v, '$.value') AS FLOAT64))
                FROM UNNEST(video_15_sec_watched_actions) AS v
            ) AS INT64)
    ELSE
        SAFE_CAST((
        SELECT SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(entry, '$.value') AS FLOAT64))
        FROM UNNEST(JSON_EXTRACT_ARRAY(actions)) AS entry
        WHERE (
            REGEXP_CONTAINS(conversion_tag, r'\d')
            AND JSON_VALUE(entry, '$.action_type') LIKE CONCAT('%', conversion_tag, '%')
        )
        OR (
            NOT REGEXP_CONTAINS(conversion_tag, r'\d')
            AND conversion_tag LIKE CONCAT('%', JSON_VALUE(entry, '$.action_type'), '%')
        )
        ) AS INT64) 
    END AS conversion,
    -- ACTIONS (sum all matching entries)
    SAFE_CAST((
        SELECT SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(entry, '$.value') AS FLOAT64))
        FROM UNNEST(JSON_EXTRACT_ARRAY(actions)) AS entry
        WHERE JSON_VALUE(entry, '$.action_type') = 'post'
    ) AS INT64) AS post_share,
    SAFE_CAST((
        SELECT SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(entry, '$.value') AS FLOAT64))
        FROM UNNEST(JSON_EXTRACT_ARRAY(actions)) AS entry
        WHERE LOWER(JSON_VALUE(entry, '$.action_type')) = 'lead'
    ) AS INT64) AS lead,

    SAFE_CAST((
        SELECT SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(entry, '$.value') AS FLOAT64))
        FROM UNNEST(JSON_EXTRACT_ARRAY(actions)) AS entry
        WHERE JSON_VALUE(entry, '$.action_type') = 'post_reaction'
    ) AS INT64) AS post_reaction_value,

    SAFE_CAST((
        SELECT SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(entry, '$.value') AS FLOAT64))
        FROM UNNEST(JSON_EXTRACT_ARRAY(actions)) AS entry
        WHERE JSON_VALUE(entry, '$.action_type') = 'comment'
    ) AS INT64) AS comments,

    SAFE_CAST((
        SELECT SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(entry, '$.value') AS FLOAT64))
        FROM UNNEST(JSON_EXTRACT_ARRAY(actions)) AS entry
        WHERE JSON_VALUE(entry, '$.action_type') = 'like'
    ) AS INT64) AS likes,

    SAFE_CAST((
        SELECT SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(entry, '$.value') AS FLOAT64))
        FROM UNNEST(JSON_EXTRACT_ARRAY(actions)) AS entry
        WHERE JSON_VALUE(entry, '$.action_type') = 'post_engagement'
    ) AS INT64) AS post_reaction_engagement,

    SAFE_CAST((
            SELECT SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(entry, '$.value') AS FLOAT64))
        FROM UNNEST(JSON_EXTRACT_ARRAY(actions)) AS entry
        WHERE JSON_VALUE(entry, '$.action_type') = 'link_click'
    ) AS INT64) AS page_engagement,

    -- VIDEO ACTIONS (sum entire arrays)
    SAFE_CAST((
        SELECT SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(v, '$.value') AS FLOAT64))
        FROM UNNEST(video_play_array) AS v
    ) AS INT64) AS last_video_played,

    SAFE_CAST((
        SELECT SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(v, '$.value') AS FLOAT64))
        FROM UNNEST(video_p25_array) AS v
    ) AS INT64) AS last_video_p25,

    SAFE_CAST((
        SELECT SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(v, '$.value') AS FLOAT64))
        FROM UNNEST(video_p50_array) AS v
    ) AS INT64) AS last_video_p50,

    SAFE_CAST((
        SELECT SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(v, '$.value') AS FLOAT64))
        FROM UNNEST(video_p75_array) AS v
    ) AS INT64) AS last_video_p75,

    SAFE_CAST((
        SELECT SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(v, '$.value') AS FLOAT64))
        FROM UNNEST(video_p100_array) AS v
    ) AS INT64) AS last_video_p100

FROM flattened_video_actions AS va LEFT JOIN centralized_ad_conversion_tag AS cnt ON va.ad_id = cnt.ad_id
    
),



summed_data AS (
    SELECT
        date_start,
        ad_id,
        --ad_name,
        adset_id,
        --adset_name,
        campaign_id,
        conversion_tag,
        SUM(conversion) AS conversions,
        SUM(SAFE_CAST(post_share AS INT64)) AS shares,
        SUM(SAFE_CAST(likes AS INT64)) AS likes,
        SUM(SAFE_CAST(lead AS INT64)) AS lead,
        SUM(SAFE_CAST(comments AS INT64)) AS comments,
        SUM(SAFE_CAST(clicks AS INT64)) AS clicks,
        SUM(SAFE_CAST(impressions AS INT64)) AS impressions,
        SUM(SAFE_CAST(post_reaction_value AS INT64)) AS post,
        SUM(SAFE_CAST(page_engagement AS INT64)) AS page_engagement,
        SUM(SAFE_CAST(post_reaction_engagement AS INT64)) AS engagement,
        SUM(spend) AS total_spend,
 
        SUM(last_video_played) AS total_video_played,
        SUM(last_video_p25) AS total_video_p25,
        SUM(last_video_p50) AS total_video_p50,
        SUM(last_video_p75) AS total_video_p75,
        SUM(last_video_p100) AS total_video_p100
    FROM parsed_video_actions
    GROUP BY date_start, ad_id,  campaign_id, adset_id, conversion_tag
)
{% endmacro %}