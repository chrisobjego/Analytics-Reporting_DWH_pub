WITH source_data AS (
  SELECT * 
  FROM 
    {{ source('meta_ads', 'meta_ads_cost') }}
),
    renamed as (
        SELECT
        CAST(date AS DATE) AS DATE,
        campaign_name AS campaign_name,        
        CAST(clicks AS INT64) AS clicks,
        CAST(impressions AS INT64) AS impressions,
        CAST(spend AS FLOAT64) AS spend,
        CAST(cpc AS FLOAT64) AS cpc
        from source_data
    )
    SELECT  
    *
    FROM
    renamed




