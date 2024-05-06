WITH source_data AS (
    SELECT *
    FROM 
       {{ source('hubspot', 'email_event') }}
),
email_event AS (
    SELECT
        CAST(email_campaign_id AS STRING) AS campaign_id,
        CAST(created AS DATE) AS created_date,
        CAST(sent_by_created AS DATE) AS sent_date,      
        CAST(type AS STRING) AS type,
        CAST(recipient AS STRING) AS email
    FROM source_data
)
SELECT  
    *
FROM
    email_event



    






