select
      --{{ dbt_utils.generate_surrogate_key(['prep_invoice.invoice_id'])}} as invoice_key, --Primary_key
      prep_invoice.invoice_id,
      prep_invoice.user_id, 
      prep_invoice.subscription_id,
      prep_invoice.invoice_date as invoice_date, 
      ----prep_subscription.mrr as mrr_from_sub,
      prep_invoice.calculated_full_mrr as mrr_from_invoice,
      prep_invoice.discounted_mrr as discounted_mrr_from_invoice
      from {{ ref ('prep_invoice')}} 
      ---left join {{ ref ('prep_subscription')}} on prep_subscription.subscription_id = prep_invoice.subscription_id       
          --WHERE
        --prep_subscription.status != 'cancelled'

