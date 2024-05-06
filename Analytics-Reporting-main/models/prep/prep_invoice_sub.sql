select
      --{{ dbt_utils.generate_surrogate_key(['Chargebee_invoice_source.invoice_id'])}} as invoice_key,
      Chargebee_invoice_source.invoice_id as invoice_id,
      Chargebee_invoice_source.invoice_status as invoice_status,
      Chargebee_invoice_source.subscription_id as subscription_id_inv,
      prep_subscription.subscription_id as subscription_id_sub,
      Chargebee_invoice_source.invoice_date as invoice_date,
      Chargebee_invoice_source.user_id as user_id,
      Chargebee_invoice_source.first_invoice as first_invoice,
      prep_dim_invoice_itemdetails.quantity as quantity,
      prep_dim_invoice_itemdetails.Plan as plan,
      prep_dim_invoice_itemdetails.duration as duration,
      prep_dim_invoice_itemdetails.Package as package,
      prep_dim_invoice_itemdetails.discounted_mrr as discounted_mrr, 
      prep_dim_invoice_itemdetails.calculated_full_mrr as calculated_full_mrr,
      prep_subscription.mrr as mrr_sub,      
      prep_subscription.status as status,
      prep_subscription.subscription_current_term_start_date as subscription_current_term_start_date,
      prep_subscription.subscription_current_term_end_date as subscription_current_term_end_date,
      prep_dim_invoice_itemdetails.discount_amount as discount_amount,
      Chargebee_invoice_discount_source.coupon_type as coupon_type,  
      Chargebee_invoice_discount_source.discount_type as discount_type,
      prep_registration.registration_date,
      prep_CPPU_sub_id.CPPU,
      prep_cost.total_daily_cost
        
from {{ ref ('Chargebee_invoice_source')}}  
left join {{ ref ('prep_dim_invoice_itemdetails')}} on prep_dim_invoice_itemdetails.invoice_id = Chargebee_invoice_source.invoice_id
left join {{ ref ('Chargebee_invoice_discount_source')}} on Chargebee_invoice_discount_source.invoice_id = Chargebee_invoice_source.invoice_id
left join {{ ref ('prep_subscription')}} on prep_subscription.subscription_id = Chargebee_invoice_source.subscription_id
left join {{ ref ('prep_registration')}} on prep_registration.user_id = Chargebee_invoice_source.user_id
left join {{ ref ('prep_CPPU_sub_id')}} on prep_CPPU_sub_id.subscription_id = Chargebee_invoice_source.subscription_id
left join {{ ref ('prep_cost')}} on prep_cost.DATE = Chargebee_invoice_source.invoice_date







--Chargebee_invoice_discount_source.sql
--Chargebee_invoice_source.sql
--Chargebee_invoice_line_item_source.sql