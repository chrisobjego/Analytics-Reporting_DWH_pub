WITH source_data AS (
    SELECT *
    FROM 
    analytics2poc.chargebee_product_catalog_2.invoice_line_item
),
    renamed as (
        SELECT
        CAST(subscription_id AS STRING) AS subscription_id,
        CAST(invoice_id AS STRING) AS invoice_id,
        CAST(customer_id AS STRING) AS user_id,
        CAST(description AS STRING)  AS description,    
        CAST(entity_description AS STRING) AS entity_description,
        CAST(entity_id AS STRING) AS entity_id,
        CAST(line_item_id AS STRING) AS line_item_id,        
        CAST(FORMAT_DATETIME('%Y-%m-%d',TIMESTAMP_ADD(TIMESTAMP_SECONDS(CAST(date_from AS INT64)), INTERVAL 2 HOUR)) AS DATE) AS date_from,
        CAST(FORMAT_DATETIME('%Y-%m-%d',TIMESTAMP_ADD(TIMESTAMP_SECONDS(CAST(date_to AS INT64)), INTERVAL 2 HOUR)) AS DATE) AS date_to,
        CAST(quantity AS INT64) AS quantity,   
        CAST(amount/ 100 AS FLOAT64) AS amount, 
        CAST(discount_amount/ 100 AS FLOAT64) AS discount_amount, 
        CAST(item_level_discount_amount/ 100 AS FLOAT64) AS item_level_discount_amount, 
        CAST(tax_amount/ 100 AS FLOAT64) AS tax_amount, 
        CAST(unit_amount/ 100 AS FLOAT64) AS unit_amount
        from source_data
    )
    SELECT  
    *
    FROM
    renamed