SELECT  
    prep_subscriber_registration.user_id,
    prep_subscriber_registration.registration_date,
    prep_subscriber_registration.First_invoice_date,
    prep_subscriber_registration.last_invoice_date,
    prep_subscriber_registration.number_of_renewals,
    prep_subscriber_registration.number_of_subscriptions_cycles,
    prep_subscriber_registration.renewal_status,
    prep_subscriber_registration.number_of_renewals_excl,
    prep_subscriber_registration.current_status,
    churn_date,
    schedule_churn_date,
    prep_subscriber_registration.last_package,
    prep_subscriber_registration.count_Basic_3,
    prep_subscriber_registration.count_Basic_3_excl,
    prep_subscriber_registration.count_Premium_3,
    prep_subscriber_registration.count_Premium_3_excl,
    prep_subscriber_registration.count_Basic_12,
    prep_subscriber_registration.count_Basic_12_excl,
    prep_subscriber_registration.count_Premium_12,
    prep_subscriber_registration.count_Premium_12_excl,
    prep_subscriber_registration.subscription_mrr,
    prep_subscriber_registration.total_revenue,
    prep_subscriber_registration.total_revenue_excl,
    prep_subscriber_registration.count_invalid_invoice,
    prep_subscriber_registration.new_existing,
    prep_subscriber_registration.pay_user,      
    ROUND(prep_CPPU.CPPU, 2) AS CPPU
from {{ ref ('prep_subscriber_registration')}}
 LEFT JOIN 
{{ ref('prep_CPPU') }} AS prep_CPPU 
ON prep_subscriber_registration.user_id = prep_CPPU.user_id




     
