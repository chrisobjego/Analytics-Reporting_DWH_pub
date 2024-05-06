WITH source_data AS (
    SELECT *
    FROM 
    {{ source('chargebee', 'invoice_discount') }}
),
    renamed as (
        SELECT
        CAST(invoice_id AS STRING) AS invoice_id,
        CAST(entity_id AS STRING) AS coupon_type,  
        CAST(discount_type AS STRING) AS discount_type,      
        CAST(amount/ 100 AS FLOAT64) AS amount_discounted         
        from source_data
    )
    SELECT  
    *
    FROM
    renamed