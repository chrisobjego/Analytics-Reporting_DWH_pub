WITH source_data AS (
  SELECT * 
  FROM 
   {{ source('google_ads', 'google_ads__ad_group_report') }}
),
    renamed as (
        SELECT
        CAST(date_day AS DATE) AS DATE,
        CAST(campaign_name AS STRING) AS campaign_name,      
        CAST(clicks AS INT64) AS clicks,
        CAST(impressions AS INT64) AS impressions,
        CAST(spend AS FLOAT64) AS spend
        from source_data
    )
    SELECT  
    *
    FROM
    renamed




