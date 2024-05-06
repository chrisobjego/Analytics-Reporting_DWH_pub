      WITH source_data AS (
    select
      *             
from {{ ref ('prep_invoice_sub')}} 
)
      SELECT
          subscription_id_sub,
          invoice_id,
          invoice_status,
          subscription_id_inv,
          invoice_date,
          user_id,
          first_invoice,
          quantity,
          plan,
          duration,
          package,
          discounted_mrr,
          calculated_full_mrr,
          mrr_sub,
          status,
          subscription_current_term_start_date,
          subscription_current_term_end_date,
          discount_amount,
          coupon_type,
          discount_type,
          registration_date,
          CPPU,
          total_daily_cost,
          CASE
            WHEN EXTRACT(YEAR FROM registration_date) = EXTRACT(YEAR FROM invoice_date) 
                AND EXTRACT(MONTH FROM registration_date) = EXTRACT(MONTH FROM invoice_date) 
                THEN 'new'
            ELSE 'exist'
          END AS new_existing,
          CASE
            WHEN EXTRACT(YEAR FROM invoice_date) = EXTRACT(YEAR FROM subscription_current_term_end_date) 
                AND EXTRACT(MONTH FROM invoice_date) = EXTRACT(MONTH FROM subscription_current_term_end_date) 
                THEN 'yes'
            ELSE 'no'
          END AS churninmonth,          
          1 AS user_count
      FROM source_data
 



