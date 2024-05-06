
   select 
   registration_key,
   prep_registration.user_id,
   prep_registration.registration_date,
   prep_registration.last_login_date,
   prep_registration.login_count,
   ROUND(prep_CPR.CPR, 2) AS CPR
   from   {{ ref ('prep_registration')}}
   LEFT JOIN
   {{ ref('prep_CPR') }} AS prep_CPR ON prep_registration.user_id = prep_CPR.user_id 



