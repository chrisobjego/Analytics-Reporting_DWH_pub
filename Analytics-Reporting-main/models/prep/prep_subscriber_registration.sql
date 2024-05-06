WITH source_data AS (
    SELECT
        prep_subscriber.user_id AS user_id,
        prep_registration.registration_date AS registration_date,
        last_invoice_date,
        First_invoice_date,
        number_of_renewals_excl,
        CASE
            WHEN number_of_renewals_excl = -1 THEN 0
            ELSE number_of_renewals_excl
        END AS number_of_renewals,
        number_of_subscriptions_cycles,
        renewal_status,
        current_status,
        churn_date,
        schedule_churn_date,
        last_package,
        count_Basic_3,
        count_Basic_3_excl,
        count_Premium_3,
        count_Premium_3_excl,
        count_Basic_12,
        count_Basic_12_excl,
        count_Premium_12,
        count_Premium_12_excl,
        subscription_mrr,
        total_revenue,
        total_revenue_excl,
        COALESCE(count_voided_not_paid_invoices, 0) AS count_invalid_invoice,

        
        CASE
            WHEN EXTRACT(YEAR FROM prep_registration.registration_date) = EXTRACT(YEAR FROM First_invoice_date) 
                AND EXTRACT(MONTH FROM prep_registration.registration_date) = EXTRACT(MONTH FROM First_invoice_date) 
                THEN 'new'
            ELSE 'exist'
        END AS new_existing,
        TRUE AS pay_user
    FROM 
        {{ ref('prep_subscriber') }} AS prep_subscriber
    LEFT JOIN 
        {{ ref('prep_registration') }} AS prep_registration 
        ON prep_registration.user_id = prep_subscriber.user_id
)
SELECT * FROM source_data
