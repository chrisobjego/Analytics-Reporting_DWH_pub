WITH source_data AS (
    SELECT *
    FROM 
      {{ ref ('Postgres_rental_object_source')}}  
),
    rental_object as (
        SELECT
        object_id,
        creation_date AS object_creation_date,
        user_id,
        object_type,
        object_area,
        object_postal_address_id
        
        from source_data

    )
    SELECT  
    *
    FROM
    rental_object













