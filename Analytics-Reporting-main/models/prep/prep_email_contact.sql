
    SELECT 
    Hubspot_email_event_source.campaign_id,
    Hubspot_email_event_source.created_date,
    Hubspot_email_event_source.sent_date,
    Hubspot_email_event_source.type,
    Hubspot_email_event_source.email,
    Hubspot_contact_source.user_id
    FROM 
      {{ ref ('Hubspot_email_event_source')}}  AS Hubspot_email_event_source 
    
    LEFT JOIN 
      {{ ref('Hubspot_contact_source') }} AS Hubspot_contact_source 
    ON  Hubspot_contact_source.email = Hubspot_email_event_source.email 
    WHERE Hubspot_email_event_source.type = 'OPEN' AND Hubspot_contact_source.property_user_status = 'customer' 















