WITH source_data AS (
    SELECT *
    FROM 
      {{ ref ('Postgres_rental_contract_source')}}  
),
    rental_contract as (
        SELECT
        --{{ dbt_utils.generate_surrogate_key(['DB_entry_id'])}} as rental_contract_key,
        DB_entry_id AS contract_id,
        user_id,
        object_id,
        creation_date AS contract_creation_date,
        modification_date AS contract_modification_date,
        start_date AS contract_start_date,
        end_date AS contract_end_date,
        rental_due_date AS contract_rental_due_date,
        object_type AS rental_object_type,
        contract_type AS rental_contract_type,
        automatic_vacancy AS is_automatic_vacancy
        from source_data
        where automatic_vacancy = false
    )
    SELECT  
    *
    FROM
    rental_contract













