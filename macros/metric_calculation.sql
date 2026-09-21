{% macro metric_calculation() %}
parsed_metrics AS (
    SELECT    date_start,
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
    FROM flattened_video_actions
),
basic_metrics AS (
    SELECT     SUM(SAFE_CAST(post_share AS INT64)) AS shares,
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
        SUM(last_video_p100) AS total_video_p100,
        date_start,
        ad_id,
        --ad_name,
        adset_id,
        --adset_name,
        campaign_id
        FROM parsed_metrics GROUP BY date_start,campaign_id,ad_id,adset_id
),
reported_conversion_tags AS (
    SELECT
        va.date_start,
        va.ad_id,
        CASE
            WHEN LOWER(JSON_VALUE(entry, '$.action_type')) IN (
                'lead', 'purchase', 'add_to_cart', 'initiate_checkout',
                'complete_registration', 'subscribe', 'schedule', 'contact'
            ) THEN LOWER(JSON_VALUE(entry, '$.action_type'))
            WHEN REGEXP_CONTAINS(LOWER(IFNULL(JSON_VALUE(entry, '$.action_type'), '')), r'offsite_conversion\.custom\.\d+')
                THEN REGEXP_EXTRACT(LOWER(JSON_VALUE(entry, '$.action_type')), r'(\d+)$')
            ELSE NULL
        END AS conversion_tag
    FROM flattened_video_actions AS va,
    UNNEST(JSON_EXTRACT_ARRAY(va.actions)) AS entry
),
all_conversion_tags AS (
    SELECT ad_id, CAST(NULL AS STRING) AS date_start, conversion_tag
    FROM centralized_ad_conversion_tag
    WHERE conversion_tag IS NOT NULL
    UNION DISTINCT
    SELECT ad_id, date_start, optimization_goal
    FROM flattened_video_actions
    WHERE optimization_goal IS NOT NULL
    UNION DISTINCT
    SELECT ad_id, date_start, conversion_tag
    FROM reported_conversion_tags
    WHERE conversion_tag IS NOT NULL
),
parsed_conversion_actions AS (
 SELECT
    va.date_start,
    va.ad_id,
    va.adset_id,
    va.campaign_id,
    t.conversion_tag,
    CASE
        WHEN t.conversion_tag IN ('lead', 'lead_generation') THEN 'lead'
        WHEN t.conversion_tag IN ('landing_page_views', 'landing_page_view') THEN 'landing_page_view'
        WHEN t.conversion_tag IN ('link_clicks', 'link_click') THEN 'link_click'
        ELSE t.conversion_tag
    END AS conversion_key,
    CASE WHEN t.conversion_tag ='reach'
        THEN NULL
        WHEN t.conversion_tag = 'impressions'
        THEN impressions
        WHEN t.conversion_tag = 'thruplay'
        THEN
            SAFE_CAST((
                SELECT SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(v, '$.value') AS FLOAT64))
                FROM UNNEST(video_15_sec_watched_actions) AS v
            ) AS INT64)
        WHEN t.conversion_tag IN ('lead', 'lead_generation')
        THEN
            SAFE_CAST((
                SELECT SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(entry, '$.value') AS FLOAT64))
                FROM UNNEST(JSON_EXTRACT_ARRAY(actions)) AS entry
                WHERE LOWER(JSON_VALUE(entry, '$.action_type')) = 'lead'
            ) AS INT64)
        WHEN t.conversion_tag IN ('landing_page_views', 'landing_page_view')
        THEN
            SAFE_CAST((
                SELECT SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(entry, '$.value') AS FLOAT64))
                FROM UNNEST(JSON_EXTRACT_ARRAY(actions)) AS entry
                WHERE LOWER(JSON_VALUE(entry, '$.action_type')) IN ('landing_page_view', 'omni_landing_page_view')
            ) AS INT64)
        WHEN t.conversion_tag IN ('link_clicks', 'link_click')
        THEN
            SAFE_CAST((
                SELECT SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(entry, '$.value') AS FLOAT64))
                FROM UNNEST(JSON_EXTRACT_ARRAY(actions)) AS entry
                WHERE LOWER(JSON_VALUE(entry, '$.action_type')) = 'link_click'
            ) AS INT64)
        WHEN t.conversion_tag = 'offsite_conversions'
        THEN
            SAFE_CAST((
                SELECT SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(entry, '$.value') AS FLOAT64))
                FROM UNNEST(JSON_EXTRACT_ARRAY(actions)) AS entry
                WHERE LOWER(JSON_VALUE(entry, '$.action_type')) IN (
                    'lead', 'purchase', 'add_to_cart', 'initiate_checkout',
                    'complete_registration', 'subscribe', 'schedule', 'contact'
                )
            ) AS INT64)
    ELSE
        SAFE_CAST((
        SELECT SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(entry, '$.value') AS FLOAT64))
        FROM UNNEST(JSON_EXTRACT_ARRAY(actions)) AS entry
        WHERE (
            REGEXP_CONTAINS(t.conversion_tag, r'\d')
            AND JSON_VALUE(entry, '$.action_type') LIKE CONCAT('%', t.conversion_tag, '%')
        )
        OR (
            NOT REGEXP_CONTAINS(t.conversion_tag, r'\d')
            AND (
                t.conversion_tag = LOWER(JSON_VALUE(entry, '$.action_type'))
                OR t.conversion_tag LIKE CONCAT('%', JSON_VALUE(entry, '$.action_type'), '%')
            )
        )
        ) AS INT64)
    END AS conversion
FROM flattened_video_actions AS va
INNER JOIN all_conversion_tags AS t
    ON va.ad_id = t.ad_id
    AND (t.date_start IS NULL OR t.date_start = va.date_start)
),
conversion_by_key AS (
    SELECT
        date_start,
        ad_id,
        adset_id,
        campaign_id,
        conversion_key,
        MAX(conversion) AS conversion
    FROM parsed_conversion_actions
    GROUP BY date_start, ad_id, adset_id, campaign_id, conversion_key
),
conversion_by_key_deduped AS (
    SELECT *
    FROM conversion_by_key AS k
    WHERE conversion_key != 'offsite_conversions'
       OR NOT EXISTS (
            SELECT 1
            FROM conversion_by_key AS other
            WHERE other.ad_id = k.ad_id
              AND other.date_start = k.date_start
              AND other.conversion_key NOT IN ('offsite_conversions', 'reach', 'impressions')
       )
),
sum_conversion AS (
    SELECT
        SUM(k.conversion) AS conversions,
        t.conversion_tag,
        k.date_start,
        k.ad_id,
        k.adset_id,
        k.campaign_id
    FROM conversion_by_key_deduped AS k
    INNER JOIN (
        SELECT
            date_start,
            ad_id,
            adset_id,
            campaign_id,
            STRING_AGG(DISTINCT conversion_tag, ', ' ORDER BY conversion_tag) AS conversion_tag
        FROM parsed_conversion_actions
        WHERE conversion_tag IS NOT NULL
        GROUP BY date_start, ad_id, adset_id, campaign_id
    ) AS t
        ON k.date_start = t.date_start
        AND k.ad_id = t.ad_id
        AND k.adset_id = t.adset_id
        AND k.campaign_id = t.campaign_id
    GROUP BY k.date_start, k.ad_id, k.adset_id, k.campaign_id, t.conversion_tag
),
summed_data AS (
    SELECT
   bm.date_start,bm.ad_id,bm.campaign_id,
   bm.adset_id,bm.shares,
        bm.likes,
        bm.lead,
        bm.comments,
        bm.clicks,
        bm.impressions,
        bm.post,
        bm.page_engagement,
        bm.engagement,
        bm.total_spend,
 
        bm.total_video_played,
        bm.total_video_p25,
        bm.total_video_p50,
        bm.total_video_p75,
        bm.total_video_p100,
        c.conversions,
        c.conversion_tag
        FROM basic_metrics AS bm LEFT JOIN sum_conversion AS c on bm.ad_id=c.ad_id AND bm.date_start=c.date_start
)
{% endmacro %}
