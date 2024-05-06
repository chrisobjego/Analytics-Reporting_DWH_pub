select
  user_id,
  date(Signup_date) AS date,
  --dimuser_key,
  CAST(COUNT(user_id) as INT64) AS signup_count
  FROM {{ ref ('prep_dim_user_signup')}}
  GROUP BY Signup_date,user_id
  --ORDER BY Signup_date



  