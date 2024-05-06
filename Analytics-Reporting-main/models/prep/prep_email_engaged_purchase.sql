WITH source_data AS (
    SELECT 
        prep_email_contact.sent_date,
        prep_subscriber.First_invoice_date,
        DATE_DIFF(prep_subscriber.First_invoice_date, prep_email_contact.sent_date, DAY) AS date_difference,
        prep_email_contact.user_id,
        Hubspot_emailcampaign_source.email_campaign_name,
        Hubspot_emailcampaign_source.email_subject,
        ROW_NUMBER() OVER (PARTITION BY prep_email_contact.user_id ORDER BY DATE_DIFF(prep_subscriber.First_invoice_date, prep_email_contact.sent_date, DAY)) AS rank
    FROM 
        {{ ref('prep_email_contact')}}  AS prep_email_contact   
    LEFT JOIN 
        {{ ref('Hubspot_emailcampaign_source') }} AS Hubspot_emailcampaign_source 
        ON  prep_email_contact.campaign_id = Hubspot_emailcampaign_source.campaign_id 
    LEFT JOIN 
        {{ ref('prep_subscriber') }} AS prep_subscriber 
        ON  prep_subscriber.user_id = prep_email_contact.user_id
    WHERE 
        DATE_DIFF(prep_subscriber.First_invoice_date, prep_email_contact.sent_date, DAY) BETWEEN 1 AND 7
)

SELECT 
    --sent_date,
    --First_invoice_date,
    --date_difference,
    user_id,
    email_campaign_name,
    email_subject
FROM 
    source_data
WHERE 
    rank = 1

    















