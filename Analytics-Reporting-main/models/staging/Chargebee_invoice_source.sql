WITH source_data AS (
    SELECT *
    FROM 
       {{ source('chargebee', 'invoice')}}
),
    renamed as (
        SELECT
        CAST(id AS STRING) AS invoice_id,
        CAST(subscription_id AS STRING) AS subscription_id,
        CAST(customer_id AS STRING) AS user_id,
        CAST(status AS STRING) AS invoice_status,
        CAST(first_invoice AS BOOL) AS first_invoice,
        CAST(dunning_status AS STRING) AS dunning_status,
        CAST(FORMAT_DATETIME('%Y-%m-%d',TIMESTAMP_ADD(TIMESTAMP_SECONDS(CAST(date AS INT64)), INTERVAL 2 HOUR)) AS DATE) AS invoice_date,
        CAST(FORMAT_DATETIME('%Y-%m-%d',TIMESTAMP_ADD(TIMESTAMP_SECONDS(CAST(due_date AS INT64)), INTERVAL 2 HOUR)) AS DATE) AS invoice_due_date,
        CAST(FORMAT_DATETIME('%Y-%m-%d',TIMESTAMP_ADD(TIMESTAMP_SECONDS(CAST(generated_at AS INT64)), INTERVAL 2 HOUR)) AS DATE) AS invoice_generated_date,       
        CAST(FORMAT_DATETIME('%Y-%m-%d',TIMESTAMP_ADD(TIMESTAMP_SECONDS(CAST(paid_at AS INT64)), INTERVAL 2 HOUR)) AS DATE) AS invoice_paid_date,       
        CAST(FORMAT_DATETIME('%Y-%m-%d',TIMESTAMP_ADD(TIMESTAMP_SECONDS(CAST(updated_at AS INT64)), INTERVAL 2 HOUR)) AS DATE) AS invoice_updated_date,
        CAST(FORMAT_DATETIME('%Y-%m-%d',TIMESTAMP_ADD(TIMESTAMP_SECONDS(CAST(voided_at AS INT64)), INTERVAL 2 HOUR)) AS DATE) AS invoice_voided_date,  
        CAST(FORMAT_DATETIME('%Y-%m-%d',TIMESTAMP_ADD(TIMESTAMP_SECONDS(CAST(next_retry_at AS INT64)), INTERVAL 2 HOUR)) AS DATE) AS invoice_next_retry_date,           
        CAST(amount_adjusted / 100 AS FLOAT64) AS amount_adjusted,
        CAST(amount_due / 100 AS FLOAT64) AS amount_due,
        CAST(amount_paid / 100 AS FLOAT64) AS amount_paid,
        CAST(amount_to_collect / 100 AS FLOAT64) AS amount_to_collect,
        CAST(credits_applied / 100 AS FLOAT64) AS credits_applied,        
        CAST(round_off_amount / 100 AS FLOAT64) AS round_off_amount,
        CAST(sub_total / 100 AS FLOAT64) AS sub_total, 
        CAST(tax / 100 AS FLOAT64) AS tax,   
        CAST(total/ 100 AS FLOAT64) AS total, 
        CAST(write_off_amount / 100 AS FLOAT64) AS write_off_amount          
        from source_data
    )
    SELECT  
    *
    FROM
    renamed