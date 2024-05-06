select
      {{ dbt_utils.generate_surrogate_key(['Chargebee_subscription_source.subscription_id'])}} as subscription_key,
      Chargebee_subscription_source.user_id as user_id,
      Chargebee_subscription_source.subscription_id as subscription_id,
      Chargebee_subscription_source.status as status,
      Chargebee_subscription_source.mrr as mrr,
      Chargebee_subscription_source.subscription_created_date as subscription_created_date,
      Chargebee_subscription_source.subscription_activated_date as subscription_activated_date,
      Chargebee_subscription_source.subscription_current_term_start_date as subscription_current_term_start_date,
      Chargebee_subscription_source.subscription_current_term_end_date as subscription_current_term_end_date,
    
    
from {{ ref ('Chargebee_subscription_source')}}  
