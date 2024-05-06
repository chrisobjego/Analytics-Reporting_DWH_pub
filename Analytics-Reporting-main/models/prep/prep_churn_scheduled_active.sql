WITH source_data AS (
    SELECT
        Chargebee_invoice_source.invoice_id AS invoice_id,
        Chargebee_subscription_source.status AS subscription_status,
        Chargebee_subscription_source.subscription_current_term_end_date AS subscription_current_term_end_date,
        Chargebee_invoice_source.invoice_status AS invoice_status,
        Chargebee_invoice_source.subscription_id AS subscription_id,
        Chargebee_invoice_source.invoice_date AS invoice_date,
        Chargebee_invoice_source.user_id AS user_id,
        Chargebee_invoice_source.first_invoice AS first_invoice
    FROM 
        {{ ref('Chargebee_invoice_source') }} AS Chargebee_invoice_source
    LEFT JOIN 
        {{ ref('Chargebee_subscription_source') }} AS Chargebee_subscription_source 
        ON Chargebee_subscription_source.subscription_id = Chargebee_invoice_source.subscription_id
    --WHERE
         --Chargebee_subscription_source.status IN ('cancelled', 'non_renewing') 
        --AND  Chargebee_invoice_source.invoice_status NOT IN ('voided', 'not_paid') -- Exclude voided invoices 
),

last_invoice_status AS (
    SELECT
        user_id,
        subscription_status,
        invoice_status,
        CASE
            WHEN subscription_status LIKE '%cancelled%' THEN subscription_current_term_end_date
            ELSE NULL
        END AS churn_date,
        CASE
            WHEN subscription_status LIKE '%non_renewing%' THEN subscription_current_term_end_date
            ELSE NULL
        END AS schedule_churn_date        
    FROM
        (
            SELECT
                user_id,
                subscription_status,
                invoice_status,
                subscription_current_term_end_date,
                ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY invoice_date DESC) AS rn
            FROM
                source_data
            WHERE
                subscription_status IN ('cancelled', 'non_renewing','active')     
        )
    WHERE
        rn = 1
)

SELECT  
    user_id,
    subscription_status  AS current_status,
    invoice_status,
    churn_date,
    schedule_churn_date
FROM
    last_invoice_status



