
    select  
        DB_entry_id,
        user_id,
        ocb_creation_date,
        billing_period_id,
        cost_summary
    FROM 
      {{ ref ('prep_ocb')}}  














