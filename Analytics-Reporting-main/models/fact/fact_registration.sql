select
  user_id,
  date(registration_date) AS date,
  --registration_key,
  CAST(COUNT(user_id) as INT64) AS registration_count
  FROM {{ ref ('prep_registration')}}
  GROUP BY registration_date,user_id
  ORDER BY registration_date desc



  