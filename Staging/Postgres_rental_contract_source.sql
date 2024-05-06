WITH source_data AS (
    SELECT *
    FROM 
      {{ source('postgres', 'DB_Rental_Contract') }}
),
    renamed as (
        SELECT
        CAST(landlord_user_id AS STRING) AS user_id,
        CAST(creation_date AS DATE) AS creation_date,
        CAST(modification_date AS DATE) AS modification_date,
        CAST(start_date AS DATE) AS start_date,
        CAST(end_date AS DATE) AS end_date,
        CAST(id AS STRING) AS DB_entry_id,
        CAST(rental_object_id AS STRING) AS object_id,
        CAST(rental_object_type AS STRING) AS object_type,
        CAST(deposit_id AS STRING) AS deposit_id,
        CAST(rental_due_date AS FLOAT64) AS rental_due_date,
        CAST(rental_contract_type AS STRING) AS contract_type,
        CAST(is_automatic_vacancy AS boolean) AS automatic_vacancy,
        from source_data
    )
    SELECT  
    *
    FROM
    renamed