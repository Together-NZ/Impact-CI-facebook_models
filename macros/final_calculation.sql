{% macro final_calculation() %}
deplicate_data AS (
SELECT 
    sd.date_start as date,
    sd.ad_id,
    ad.ad_name,
    i.interest_names,
    sd.adset_id AS media_buy_external_id,
    adset.adset_name AS media_buy_external_name,
    sd.campaign_id,
    fcd.campaign_name,
    sd.shares,
    sd.likes,
    sd.lead,
    sd.comments,
    sd.conversion_tag,
    fcd.campaign_status,
    fcd.campaign_objective,
    fcd.start_time,
    fcd.stop_time,
    sd.conversions as conversions,
    sd.total_video_p25 as video_25_completion,
    sd.total_video_p50 as video_50_completion,
    sd.total_video_p75 as video_75_completion, 
    sd.total_video_p100 as video_completion,
    sd.total_video_played as video_played,
    sd.page_engagement AS clicks,
    sd.engagement AS social_post_engagement,
    sd.impressions,
    sd.post AS delivery_social_post_like,
    sd.total_spend as media_cost,
    row_number() OVER (PARTITION BY sd.date_start, sd.ad_id ,fcd.campaign_id, sd.conversion_tag ORDER BY sd.date_start) AS row_number
FROM summed_data sd
LEFT JOIN filtered_campaign_data fcd
    ON fcd.campaign_id = sd.campaign_id
LEFT JOIN filtered_interest_data i
    ON i.ad_id = sd.ad_id
LEFT JOIN deduplicate_ad_data as ad
    ON ad.ad_id = sd.ad_id
LEFT JOIN deduplicate_adset_data as adset
    ON adset.adset_id = sd.adset_id

ORDER BY sd.date_start
)
SELECT * EXCEPT(ad_name), ad_name as creative_name, 
'Meta' AS publisher,
CASE 
WHEN ARRAY_LENGTH(SPLIT(campaign_name,'_'))>=4 AND SPLIT(campaign_name,'_')[OFFSET(3)] LIKE '%SOCIAL%' AND (lower(campaign_name) like '%vid%' or lower(ad_name) like '%vid%') THEN 'Social Video'
WHEN ARRAY_LENGTH(SPLIT(campaign_name,'_'))>=4 AND SPLIT(campaign_name,'_')[OFFSET(3)] LIKE '%SOCIAL%' AND (lower(campaign_name) not like '%vid%' and lower(ad_name) not like '%vid%')THEN 'Social Display'
else 'Other'
END AS media_format,
    CASE WHEN ARRAY_LENGTH(SPLIT(media_buy_external_name, '_')) >= 8 THEN SPLIT(media_buy_external_name, '_')[SAFE_OFFSET(7)] 
         ELSE 'Other' END AS audience_name,
    CASE WHEN ARRAY_LENGTH(SPLIT(ad_name, '_')) >= 8 THEN SPLIT(ad_name, '_')[SAFE_OFFSET(7)] 
         ELSE 'Other' END AS creative_descr,
    CASE WHEN ARRAY_LENGTH(SPLIT(ad_name, '_')) >= 8 THEN SPLIT(ad_name, '_')[SAFE_OFFSET(5)] 
         ELSE 'Other' END AS ad_format_detail,
    CASE WHEN ARRAY_LENGTH(SPLIT(ad_name, '_')) >= 8 THEN SPLIT(ad_name, '_')[SAFE_OFFSET(6)] 
         ELSE 'Other' END AS ad_format,
    CASE WHEN ARRAY_LENGTH(SPLIT(campaign_name,'_')) <=1 THEN 'Other'
        ELSE SPLIT(campaign_name,'_')[SAFE_OFFSET(1)] END AS campaign_descr
FROM deplicate_data WHERE row_number = 1
{% endmacro %}