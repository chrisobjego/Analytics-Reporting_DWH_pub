WITH source_data AS (
    SELECT *
    FROM 
      {{ source('postgres', 'DB_Objego_user') }}
),
    renamed as (
        SELECT
        CAST(user_id AS STRING) AS user_id,
        CAST(user_properties_id AS STRING) AS object_id,
        CAST(email_address AS STRING) AS email,
        CAST(user_status AS STRING) AS user_status,
        CAST(registration_date AS DATE) AS registration_date,
        CAST(login_date AS DATE) AS login_date,
        CAST(login_count AS INT64) AS login_count                                      
        from source_data
    )
    SELECT  
    *
    FROM
    renamed