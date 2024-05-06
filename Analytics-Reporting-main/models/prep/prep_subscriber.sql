WITH source_data AS (
    SELECT
        Chargebee_invoice_source.invoice_id AS invoice_id,
        Chargebee_subscription_source.status AS subscription_status,
        Chargebee_subscription_source.subscription_current_term_end_date AS subscription_current_term_end_date,        
        Chargebee_invoice_source.invoice_status AS invoice_status,
        Chargebee_invoice_source.subscription_id AS subscription_id,
        Chargebee_invoice_source.invoice_date AS invoice_date,
        Chargebee_invoice_source.user_id AS user_id,
        Chargebee_invoice_source.first_invoice AS first_invoice,
        prep_dim_invoice_itemdetails.quantity AS quantity,
        prep_dim_invoice_itemdetails.Plan AS plan,
        prep_dim_invoice_itemdetails.duration AS duration,
        prep_dim_invoice_itemdetails.Package AS package,
        Chargebee_subscription_source.mrr AS subscription_mrr,
        prep_dim_invoice_itemdetails.discounted_mrr AS discounted_mrr,
        prep_dim_invoice_itemdetails.calculated_full_mrr AS calculated_full_mrr,
        prep_dim_invoice_itemdetails.discount_amount AS discount_amount,
        prep_dim_invoice_itemdetails.amount_excl_vat AS amount_excl_vat,
        Chargebee_invoice_discount_source.coupon_type AS coupon_type,  
        Chargebee_invoice_discount_source.discount_type AS discount_type
    FROM
        {{ ref('Chargebee_invoice_source') }} AS Chargebee_invoice_source
    LEFT JOIN
        {{ ref('prep_dim_invoice_itemdetails') }} AS prep_dim_invoice_itemdetails
        ON prep_dim_invoice_itemdetails.invoice_id = Chargebee_invoice_source.invoice_id
    LEFT JOIN
        {{ ref('Chargebee_invoice_discount_source') }} AS Chargebee_invoice_discount_source
        ON Chargebee_invoice_discount_source.invoice_id = Chargebee_invoice_source.invoice_id
    LEFT JOIN
        {{ ref('Chargebee_subscription_source') }} AS Chargebee_subscription_source
        ON Chargebee_subscription_source.subscription_id = Chargebee_invoice_source.subscription_id
),
last_invoice_status AS (
    SELECT
        user_id,
        subscription_status,
        subscription_mrr,
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
                subscription_mrr,
                subscription_current_term_end_date,
                ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY invoice_id DESC) AS rn
            FROM
                source_data
            WHERE
                subscription_status IN ('cancelled', 'non_renewing','active')
        )
    WHERE
        rn = 1
),
renewal_counts AS (
    SELECT
        user_id,
        MAX(invoice_date) AS last_invoice_date,
        MIN(invoice_date) AS First_invoice_date,        
        CAST(COUNT(DISTINCT CASE WHEN invoice_status NOT IN ('voided', 'not_paid','payment_due') THEN invoice_id END) - 1 AS INT64) AS number_of_renewals_excl,
        CAST(COUNT(DISTINCT CASE WHEN invoice_status NOT IN ('voided', 'not_paid','payment_due') THEN invoice_id END) AS INT64) AS number_of_subscriptions_cycles,
        CASE
            WHEN (COUNT(DISTINCT CASE WHEN invoice_status NOT IN ('voided', 'not_paid','payment_due') THEN invoice_id END) - 1) > 0 THEN 'yes'
            ELSE 'no'
        END AS renewal_status,
        SUM(CASE WHEN invoice_status NOT IN ('voided', 'not_paid','payment_due') THEN amount_excl_vat ELSE 0 END) AS total_revenue_excl
    FROM
        source_data
    GROUP BY
        user_id
),
last_package AS (
    SELECT
        user_id,
        FIRST_VALUE(package) OVER (PARTITION BY user_id ORDER BY invoice_id DESC) AS last_package,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY invoice_id DESC) AS rn
    FROM
        source_data
   
),
package_counts AS (
    SELECT
        user_id,
        COUNTIF(package = 'basic-EUR-Every-3-months') AS count_Basic_3,
        COUNTIF(package = 'premium-EUR-Every-3-months') AS count_Premium_3,
        COUNTIF(package = 'basic-EUR-Yearly') AS count_Basic_12,
        COUNTIF(package = 'premium-EUR-Yearly') AS count_Premium_12
    FROM
        source_data
     
    GROUP BY
        user_id
),
total_revenue AS (
    SELECT
        user_id,
        SUM(amount_excl_vat) AS total_revenue
    FROM
        source_data
    GROUP BY
        user_id
),
package_counts_excl_voided AS (
    SELECT
        user_id,
        COUNTIF(CASE WHEN invoice_status NOT IN ('voided', 'not_paid','payment_due') THEN package ELSE NULL END = 'basic-EUR-Every-3-months') AS count_Basic_3,
        COUNTIF(CASE WHEN invoice_status NOT IN ('voided', 'not_paid','payment_due') THEN package ELSE NULL END = 'premium-EUR-Every-3-months') AS count_Premium_3,
        COUNTIF(CASE WHEN invoice_status NOT IN ('voided', 'not_paid','payment_due') THEN package ELSE NULL END = 'basic-EUR-Yearly') AS count_Basic_12,
        COUNTIF(CASE WHEN invoice_status NOT IN ('voided', 'not_paid','payment_due') THEN package ELSE NULL END = 'premium-EUR-Yearly') AS count_Premium_12
    FROM
        source_data
    GROUP BY
        user_id
),
voided_not_paid_invoices AS (
    SELECT
        user_id,
        COUNT(*) AS count_voided_not_paid_invoices
    FROM
        source_data
    WHERE
        invoice_status NOT IN ('paid')
    GROUP BY
        user_id
)
SELECT  
    rc.*,
    lis.subscription_status AS current_status,
    lis.subscription_mrr,
    lis.churn_date,  
    lis.schedule_churn_date,  
    lp.last_package,
    pc.count_Basic_3,
    pc.count_Premium_3,
    pc.count_Basic_12,
    pc.count_Premium_12,
    pc_excl_voided.count_Basic_3 AS count_Basic_3_excl,
    pc_excl_voided.count_Premium_3 AS count_Premium_3_excl,
    pc_excl_voided.count_Basic_12 AS count_Basic_12_excl,
    pc_excl_voided.count_Premium_12 AS count_Premium_12_excl,
    tr.total_revenue,
    vnpi.count_voided_not_paid_invoices
FROM
    last_invoice_status lis
LEFT JOIN
    renewal_counts rc ON  lis.user_id = rc.user_id
LEFT JOIN
    last_package lp ON lis.user_id = lp.user_id
LEFT JOIN
    package_counts pc ON lis.user_id = pc.user_id
LEFT JOIN
    total_revenue tr ON lis.user_id = tr.user_id
LEFT JOIN
    package_counts_excl_voided pc_excl_voided ON lis.user_id = pc_excl_voided.user_id
LEFT JOIN
    voided_not_paid_invoices vnpi ON lis.user_id = vnpi.user_id
WHERE
    lp.rn = 1
