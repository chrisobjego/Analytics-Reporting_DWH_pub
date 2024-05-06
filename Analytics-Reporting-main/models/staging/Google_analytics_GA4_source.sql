WITH source_data as (
  SELECT *
  FROM 
  {{ source('google_analytics_4', 'ga_4_sourcemediumcampaign') }}
),

    renamed as (
        SELECT
            CAST(date AS DATE) AS event_date,
            CAST(custom_event_user_id AS STRING) AS user_id,
            CAST(campaign_name AS STRING) AS event_campaign,
            CAST(medium AS STRING) AS event_medium,
            CAST(source AS STRING) AS event_source,
            CAST(event_name AS STRING) AS event_name,
            CAST(is_conversion_event AS BOOLEAN) AS event_conversions,
            CAST(conversions AS FLOAT64) AS conversions_credit
        FROM source_data
    )

    SELECT  
    *
    FROM
    renamed