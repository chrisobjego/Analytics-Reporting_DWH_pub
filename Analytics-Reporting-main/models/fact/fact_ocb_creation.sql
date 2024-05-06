
    select  
        DB_entry_id,
        user_id,
        ocb_creation_date,

    FROM 
      {{ ref ('prep_ocb')}}  














