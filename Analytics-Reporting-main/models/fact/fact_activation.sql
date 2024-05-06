    select  
    --dim_activated_key,
    user_id,
    activation_date ,
    activation_count AS rental_contract_count,
    time_to_convert_days
    FROM {{ ref ('prep_activation')}}  














