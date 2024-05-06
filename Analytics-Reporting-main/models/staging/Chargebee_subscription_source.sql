WITH source_data AS (
    SELECT *
    FROM 
       {{ source('chargebee', 'subscription') }}
),
    renamed as (
        SELECT
        CAST(id AS STRING) AS subscription_id,
        CAST(customer_id AS STRING) AS user_id,
        CAST(FORMAT_DATETIME('%Y-%m-%d',TIMESTAMP_ADD(TIMESTAMP_SECONDS(CAST(created_at AS INT64)), INTERVAL 2 HOUR)) AS DATE) AS subscription_created_date,
        CAST(FORMAT_DATETIME('%Y-%m-%d',TIMESTAMP_ADD(TIMESTAMP_SECONDS(CAST(activated_at AS INT64)), INTERVAL 2 HOUR)) AS DATE) AS subscription_activated_date,
        CAST(FORMAT_DATETIME('%Y-%m-%d',TIMESTAMP_ADD(TIMESTAMP_SECONDS(CAST(current_term_start AS INT64)), INTERVAL 2 HOUR)) AS DATE) AS subscription_current_term_start_date,       
        CAST(FORMAT_DATETIME('%Y-%m-%d',TIMESTAMP_ADD(TIMESTAMP_SECONDS(CAST(current_term_end AS INT64)), INTERVAL 2 HOUR)) AS DATE) AS subscription_current_term_end_date,       
        CAST(FORMAT_DATETIME('%Y-%m-%d',TIMESTAMP_ADD(TIMESTAMP_SECONDS(CAST(updated_at AS INT64)), INTERVAL 2 HOUR)) AS DATE) AS subscription_updated_date,
        CAST(FORMAT_DATETIME('%Y-%m-%d',TIMESTAMP_ADD(TIMESTAMP_SECONDS(CAST(cancel_schedule_created_at AS INT64)), INTERVAL 2 HOUR)) AS DATE) AS subscription_cancel_schedule_created_date,
        CAST(FORMAT_DATETIME('%Y-%m-%d',TIMESTAMP_ADD(TIMESTAMP_SECONDS(CAST(cancelled_at AS INT64)), INTERVAL 2 HOUR)) AS DATE) AS subscription_cancelled_date,
        CAST(FORMAT_DATETIME('%Y-%m-%d',TIMESTAMP_ADD(TIMESTAMP_SECONDS(CAST(due_since AS INT64)), INTERVAL 2 HOUR)) AS DATE) AS subscription_due_since_date,
        CAST(FORMAT_DATETIME('%Y-%m-%d',TIMESTAMP_ADD(TIMESTAMP_SECONDS(CAST(next_billing_at AS INT64)), INTERVAL 2 HOUR)) AS DATE) AS subscription_next_billing_date, 
        CAST(FORMAT_DATETIME('%Y-%m-%d',TIMESTAMP_ADD(TIMESTAMP_SECONDS(CAST(started_at AS INT64)), INTERVAL 2 HOUR)) AS DATE) AS subscription_started_date,        
        CAST(billing_period AS INT64) AS billing_period,   
        CAST(billing_period_unit AS STRING)  AS billing_period_unit,    
        CAST(due_invoices_count AS INT64) AS due_invoices_count,
        CAST(monthly_reccuring_revenue/ 100 AS FLOAT64) AS mrr, 
        CAST(status AS STRING) AS status, 
        CAST(total_dues/ 100 AS FLOAT64) AS total_dues, 
        CAST(coupon AS STRING) AS coupon
        from source_data
    )
    SELECT  
    *
    FROM
    renamed