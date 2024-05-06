    select  
    user_id,
    registration_date,
    activation_date,
    time_to_convert_hours,
    time_to_convert_days
    FROM {{ ref ('prep_activation')}}  
 
















