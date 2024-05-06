WITH source_data AS (
  SELECT * 
  FROM 
   {{ source('affiliates', 'Adcell_Data') }}
),
    renamed as (
        SELECT
            DATE(TIMESTAMP(createTime)) AS create_date,
            DATE(TIMESTAMP(changeTime)) AS change_date,        
            CAST(reference AS STRING) AS user_id,
            CAST(referer AS STRING) AS adcell_referer_link,
            CAST(eventId AS STRING) AS adcell_event_id,
            CAST(promotionId AS STRING) AS adcell_promotion_Id,
            CAST(eventName AS STRING) AS adcell_event_name,
            CAST(affiliateId AS STRING) AS adcell_affiliate_Id,
            CAST(affiliateCompany AS STRING) AS adcell_affiliate_Company,
            CAST(commissionAffiliate AS STRING) AS adcell_commission_Affiliate,
            CAST(commissionNetwork AS STRING) AS adcell_commission_Network,
            CAST(commissionStatus AS STRING) AS adcell_commission_Status,
            CAST(eventType AS STRING) AS adcell_event_Type
        FROM source_data
    )
    SELECT  
    *
    FROM
    renamed




