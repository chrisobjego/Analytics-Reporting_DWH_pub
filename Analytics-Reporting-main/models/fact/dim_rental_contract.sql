SELECT 
    --rental_contract_key,
    rental_object_type,
    rental_contract_type,
    is_automatic_vacancy
FROM {{ ref ('prep_rental_contract')}}  










