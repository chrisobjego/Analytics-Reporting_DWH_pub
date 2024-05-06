
    select 
        object_id,
        user_id,
        object_creation_date

    FROM 
      {{ ref ('prep_rental_object_creation')}}  














