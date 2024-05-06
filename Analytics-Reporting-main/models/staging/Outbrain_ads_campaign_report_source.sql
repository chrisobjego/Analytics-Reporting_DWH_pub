WITH source_data AS (
  SELECT * 
  FROM 
    {{ source('outbrain_ads', 'campaign_report') }}
),
    renamed as (
        SELECT
        CAST(day AS DATE) AS DATE,        
        CAST(clicks AS INT64) AS clicks,
        CAST(impressions AS INT64) AS impressions,
        ROUND(CAST(spend AS FLOAT64), 2) AS spend
        from source_data
    )
    SELECT  
    *
    FROM
    renamed




