select
   subscription_id,
   status as subscription_status,
   mrr
from {{ ref ('prep_subscription')}}  


