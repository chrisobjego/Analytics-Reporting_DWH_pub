WITH source_data AS (
    SELECT *
    FROM 
      {{ source('postgres', 'DB_Rental_Objects') }}
),
    renamed as (
        SELECT
        CAST(lessor_id AS STRING) AS user_id,
        CAST(id AS STRING) AS object_id,
        CAST(postal_address_id AS STRING) AS object_postal_address_id,
        CAST(name AS STRING) AS object_named_by_user,
        CAST(creation_date AS DATE) AS creation_date,
        CAST(modification_date AS DATE) AS modification_date,
        CAST(type AS STRING) AS object_type,
        CAST(area AS FLOAT64) AS object_area
        from source_data
    )
    SELECT  
    *
    FROM
    renamed