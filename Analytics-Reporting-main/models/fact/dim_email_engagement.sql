SELECT 
    COALESCE(prep_email_engaged_activation.user_id, prep_email_engaged_purchase.user_id) AS user_id,
    COALESCE(prep_email_engaged_activation.email_campaign_name, 'did_not_open_in7days') AS Last_email_b4_activation,
    COALESCE(prep_email_engaged_activation.email_subject, 'did_not_open_in7days') AS Last_email_subject_b4_activation,
    COALESCE(prep_email_engaged_purchase.email_campaign_name, 'did_not_open_in7days') AS Last_email_b4_purchase,
    COALESCE(prep_email_engaged_purchase.email_subject, 'did_not_open_in7days') AS Last_email_subject_b4_purchase       
FROM 
    {{ ref('prep_email_engaged_activation')}}  AS prep_email_engaged_activation   
FULL OUTER JOIN
    {{ ref('prep_email_engaged_purchase') }} AS prep_email_engaged_purchase 
ON
    prep_email_engaged_activation.user_id = prep_email_engaged_purchase.user_id




    















