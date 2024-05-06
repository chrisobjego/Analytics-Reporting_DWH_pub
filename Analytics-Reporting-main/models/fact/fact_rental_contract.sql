SELECT 
    --rental_contract_key,
    contract_id,
    user_id,
    object_id,
    contract_creation_date
FROM {{ ref ('prep_rental_contract')}}  










