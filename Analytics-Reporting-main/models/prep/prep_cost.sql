WITH AllCosts AS (
  SELECT 
    DATE,
    'google_ads' AS source,
    ROUND(spend, 2) AS daily_cost
  FROM {{ ref ('Google_ads_ad_group_report_source')}}
    
  UNION ALL
  SELECT 
    DATE ,
    'facebook_ads' AS source,
    ROUND(spend, 2) AS daily_cost
  FROM {{ ref ('Meta_ads_group_report_source')}}


  UNION ALL
  SELECT 
    DATE,
    'outbrain_ads' AS source,
    ROUND(spend, 2) AS daily_cost
  FROM {{ ref ('Outbrain_ads_campaign_report_source')}}


  UNION ALL
  SELECT 
    DATE,
    'demand_cost' AS source,
    ROUND(spend, 2) AS daily_cost
  FROM {{ ref ('Google_sheet_demand_ads_source')}}
 

  UNION ALL
  SELECT 
    DATE(Date) AS DATE,
    'adcell_cost' AS source,
    ROUND(cost, 2) AS daily_cost
  FROM {{ ref ('Affiliates_ads_adcell_cost_source')}}
    

  UNION ALL
  SELECT 
    DATE,
    'microsoft_ads' AS source,
    ROUND(spend, 2) AS daily_cost
  FROM {{ ref ('Microsoft_Bing_ads_ad_group_report_source')}}


  UNION ALL
  SELECT 
    DATE,
    'coop_cost' AS source,
    ROUND(spend, 2) AS daily_cost
  FROM {{ ref ('Google_sheet_Coop_ads_source')}}
)

SELECT 
  DATE,
  CAST(SUM(CASE WHEN source = 'google_ads' THEN daily_cost ELSE 0 END) AS FLOAT64) AS google_ads,
  CAST(SUM(CASE WHEN source = 'facebook_ads' THEN daily_cost ELSE 0 END) AS FLOAT64) AS facebook_ads,
  CAST(SUM(CASE WHEN source = 'outbrain_ads' THEN daily_cost ELSE 0 END) AS FLOAT64) AS outbrain_ads,
  CAST(SUM(CASE WHEN source = 'demand_cost' THEN daily_cost ELSE 0 END) AS FLOAT64) AS demand_cost,
  CAST(SUM(CASE WHEN source = 'adcell_cost' THEN daily_cost ELSE 0 END) AS FLOAT64) AS adcell_cost,
  CAST(SUM(CASE WHEN source = 'microsoft_ads' THEN daily_cost ELSE 0 END) AS FLOAT64) AS microsoft_ads,
  CAST(SUM(CASE WHEN source = 'coop_cost' THEN daily_cost ELSE 0 END) AS FLOAT64) AS coop_cost,
  ROUND(CAST(SUM(daily_cost) AS FLOAT64), 2) AS total_daily_cost
FROM 
  AllCosts
GROUP BY 
  DATE
ORDER BY 
  DATE
