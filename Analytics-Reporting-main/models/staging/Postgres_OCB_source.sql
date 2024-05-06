WITH source_data AS (
    SELECT *
    FROM 
      {{ source('postgres', 'DB_OCB') }}
),
    renamed as (
        SELECT
            CAST(id AS STRING) AS DB_entry_id,
            creation_date,
            --CAST(creation_date AS DATE) AS creation_date,
            --CAST((modification_date AS INT64) AS DATE) AS modification_date,
            --DATE(TIMESTAMP(CAST(creation_date AS INT64))) AS creation_date,
            --CAST((creation_date AS INT64) AS DATE) AS creation_date,
            --CAST(FORMAT_DATETIME('%Y-%m-%d',TIMESTAMP_ADD(TIMESTAMP(CAST(creation_date AS INT64)), INTERVAL 2 HOUR)) AS DATE),
            --CAST(modification_date AS DATE) AS modification_date,
            --DATE(TIMESTAMP(CAST(CAST(creation_date AS STRING) AS INT64))) AS creation_date,
            --DATE(TIMESTAMP_MILLIS(CAST(CAST(creation_date AS STRING) AS INT64) / 1000)) AS creation_date,
            --DATE(TIMESTAMP_MILLIS(CAST(creation_date AS INT64))) AS creation_date,
            CAST(landlord_user_id AS STRING) AS user_id,
            CAST(rental_object_id AS STRING) AS object_id,
            CAST(rental_object_type AS STRING) AS object_type,
            CAST(billing_period_id AS STRING) AS billing_period_id,
            CAST(customization_id AS STRING) AS customization_id,
            CAST(cover_letter_id AS STRING) AS cover_letter_id,
            ROUND(CAST(total_cost_summary AS FLOAT64), 2) AS cost_summary,
            ROUND(CAST(total_pre_paid_summary AS FLOAT64), 2) AS pre_paid_summary,
            ROUND(CAST(total_saldo_summary AS FLOAT64), 2) AS saldo_summary,
            ROUND(CAST(total_cost_net_summary AS FLOAT64), 2) AS cost_net_summary,
            ROUND(CAST(total_prepayment_gross AS FLOAT64), 2) AS prepayment_gross,
            ROUND(CAST(total_prepayment_net AS FLOAT64), 2) AS prepayment_net,
            ROUND(CAST(total_balance_gross AS FLOAT64), 2) AS balance_gross,
            ROUND(CAST(total_balance_net AS FLOAT64), 2) AS balance_net
        FROM source_data
    )
    SELECT  
    *
    FROM
    renamed