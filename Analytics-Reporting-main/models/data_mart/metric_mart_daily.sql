WITH metric_mart AS (
    SELECT
        dim_registration.user_id,
        dim_registration.registration_date,
        dim_registration.last_login_date,
        dim_registration.login_count,
        dim_registration.CPR,
        
        dim_signup_user.campaign,
        dim_signup_user.source_medium,
        dim_signup_user.Signup_date,


        dim_users_rental_object_creation.object_created,
        dim_users_rental_object_creation.first_object_creation_date,
        dim_users_rental_object_creation.object_count,
        dim_users_rental_object_creation.multi_family_house,
        dim_users_rental_object_creation.address_container,
        dim_users_rental_object_creation.one_family_house,
        dim_users_rental_object_creation.other_objects,
        dim_users_rental_object_creation.unit_count,
        dim_users_rental_contract.first_rental_contract_date,
        dim_users_rental_contract.contract_created, 
        dim_users_rental_contract.rental_contracts_count, 

        dim_subscribers.First_invoice_date,
        dim_subscribers.number_of_renewals,  --only paid invoices counted here. 
        dim_subscribers.renewal_status,
        dim_subscribers.current_status,
        dim_subscribers.subscription_mrr, 
        dim_subscribers.total_revenue_excl,   --only paid invoices counted here.     
        dim_subscribers.churn_date,
        dim_subscribers.schedule_churn_date,               
        dim_subscribers.count_Basic_3_excl, --only paid invoices counted here.
        dim_subscribers.count_Premium_3_excl,   --only paid invoices counted here.
        dim_subscribers.count_Basic_12_excl, --only paid invoices counted here.
        dim_subscribers.count_Premium_12_excl, --only paid invoices counted here.
        dim_subscribers.last_package,         
        dim_subscribers.new_existing, 
        dim_subscribers.pay_user,
        dim_subscribers.CPPU,
 

        dim_users_ocb.ocb_count,
        dim_users_ocb.ocb_created,

        dim_email_engagement.Last_email_b4_activation,
        dim_email_engagement.Last_email_subject_b4_activation,
        dim_email_engagement.Last_email_b4_purchase,
        dim_email_engagement.Last_email_subject_b4_purchase,


    FROM
        {{ ref('dim_registration') }} AS dim_registration
    LEFT JOIN
        {{ ref('dim_signup_user') }} AS dim_signup_user ON dim_registration.user_id = dim_signup_user.user_id
    LEFT JOIN
        {{ ref('dim_users_rental_object_creation') }} AS dim_users_rental_object_creation ON dim_registration.user_id = dim_users_rental_object_creation.user_id
    LEFT JOIN
        {{ ref('dim_users_rental_contract') }} AS dim_users_rental_contract ON dim_registration.user_id = dim_users_rental_contract.user_id    
    LEFT JOIN
        {{ ref('dim_subscribers') }} AS dim_subscribers ON dim_registration.user_id = dim_subscribers.user_id       
    LEFT JOIN
        {{ ref('dim_users_ocb') }} AS dim_users_ocb ON dim_registration.user_id = dim_users_ocb.user_id   
    LEFT JOIN
        {{ ref('dim_email_engagement') }} AS dim_email_engagement ON dim_registration.user_id = dim_email_engagement.user_id                      
)
SELECT
    user_id,
    Signup_date as signup_date,
    registration_date,
    last_login_date,
    login_count,    
    COALESCE(campaign, 'not_tracked') AS campaign,
    COALESCE(source_medium, 'not_tracked') AS source_medium,
    COALESCE(object_created,FALSE) AS created_object,   
    first_object_creation_date,
    COALESCE(object_count,0) AS object_count,
    COALESCE(multi_family_house,0) AS multi_family_house,
    COALESCE(address_container,0) AS address_container,
    COALESCE(one_family_house,0) AS one_family_house,
    COALESCE(other_objects,0) AS other_objects,
    COALESCE(unit_count,0) AS unit_count,
    COALESCE(contract_created,FALSE) AS created_contract,
    first_rental_contract_date,
    COALESCE(rental_contracts_count,0) AS rental_contracts_count,
    CASE WHEN DATE_DIFF(first_rental_contract_date, registration_date, DAY) <= 30 THEN TRUE ELSE FALSE END AS activated,
    CASE WHEN first_rental_contract_date <= registration_date THEN 0 ELSE DATE_DIFF(first_rental_contract_date, registration_date, DAY) END AS days_btw_reg_act,

    COALESCE(Last_email_b4_activation,'did_not_open_in7days') AS Last_email_b4_activated,
    COALESCE(Last_email_subject_b4_activation,'did_not_open_in7days') AS Last_email_subject_b4_activated,
    COALESCE(Last_email_b4_purchase,'did_not_open_in7days') AS Last_email_b4_subscribing,
    COALESCE(Last_email_subject_b4_purchase,'did_not_open_in7days') AS Last_email_subject_b4_subscribing,

    COALESCE(pay_user,FALSE) AS subscriber,
    First_invoice_date AS first_invoice_date,
    COALESCE(current_status,'starters') AS current_status,  
    COALESCE(last_package,'starters') AS current_package,
    COALESCE(number_of_renewals,0) AS renewal_counts,    
    COALESCE(count_Basic_3_excl,0) AS count_basic_3,       
    COALESCE(count_Premium_3_excl,0) AS count_premium_3,
    COALESCE(count_Basic_12_excl,0) AS count_basic_12,
    COALESCE(count_Premium_12_excl,0) AS count_premium_12,
    COALESCE(new_existing,'starters') AS new_existing,
    COALESCE(subscription_mrr,0.0) AS mrr,
    COALESCE(total_revenue_excl,0.0) AS total_revenue,   
     
    churn_date,  
    schedule_churn_date,
    COALESCE(ocb_created,FALSE) AS created_ocb,   
    COALESCE(ocb_count,0) AS ocb_count,
    CPR as cpr ,    
    COALESCE(CPPU,0) AS cppu,
    COUNT(user_id) OVER (PARTITION BY user_id) AS user_count
FROM
    metric_mart

