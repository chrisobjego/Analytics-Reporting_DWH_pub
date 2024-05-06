select
  --invoice_key,
  invoice_id,
  invoice_date,
  first_invoice,
  invoice_status,
  plan,
  duration,
  package,
  discounted_mrr,
  calculated_full_mrr,
  coupon_type,
  discount_type,

from {{ ref ('prep_invoice')}}  






