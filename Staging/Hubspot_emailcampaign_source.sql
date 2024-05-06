WITH source_data AS (
    SELECT *
    FROM 
       {{ source('hubspot', 'email_campaign') }}
),
email_campaign AS (
    SELECT
        CAST(id AS STRING) AS campaign_id,
        CAST(content_id AS STRING) AS content_id,
        CAST(name AS STRING) AS email_campaign_name,
        CAST(num_included AS INT64) AS num_included,
        CAST(subject AS STRING) AS email_subject
    FROM source_data
)
SELECT  
    *
FROM
    email_campaign



    






