WITH source_data AS (
    SELECT *
    FROM 
     {{ ref ('Chargebee_invoice_line_item_source')}}
),
    source_data2 AS (
        SELECT 
            entity_id AS Package,
            --description, --AS Plan_test,
            --entity_description, --AS duration_test,
            quantity,
            invoice_id, 
            amount,
            discount_amount,

            ROUND(CAST((amount - (amount * 0.1596)) AS FLOAT64), 2) AS amount_excl_vat,

            CASE 
                WHEN LOWER(entity_id) LIKE '%yearly%' THEN ROUND(CAST((amount - (amount * 0.1596)) / 12 AS FLOAT64), 2)
                ELSE ROUND(CAST((amount - (amount * 0.1596)) / 3 AS FLOAT64), 2)
            END AS calculated_full_mrr,

            CASE 
                WHEN LOWER(description) LIKE '%basic%' THEN 'Basic'
                ELSE 'Premium'
            END AS Plan,

            CASE 
                WHEN LOWER(entity_id) LIKE '%months%' THEN 'Quarterly'
                ELSE 'Yearly'
            END AS duration,           


            CASE 
                WHEN LOWER(entity_id) LIKE '%yearly%' THEN 
                    CASE 
                        WHEN discount_amount <> 0 THEN ROUND(CAST(((amount - (amount * 0.1596)) - (discount_amount - (discount_amount * 0.1596)))  / 12 AS FLOAT64), 2)
                        ELSE ROUND(CAST((amount - (amount * 0.1596)) / 12 AS FLOAT64), 2)
                    END
                ELSE 
                    CASE 
                        WHEN discount_amount <> 0 THEN ROUND(CAST(((amount - (amount * 0.1596)) - (discount_amount - (discount_amount * 0.1596))) / 3 AS FLOAT64), 2)
                        ELSE ROUND(CAST((amount - (amount * 0.1596))  / 3 AS FLOAT64), 2)
                    END
            END AS discounted_mrr
        FROM source_data
        
    )

    SELECT  
    *
    FROM
    source_data2