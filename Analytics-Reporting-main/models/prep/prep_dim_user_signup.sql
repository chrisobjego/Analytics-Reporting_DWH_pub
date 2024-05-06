WITH source_data AS (
  SELECT
    user_id,
    event_date,
    CASE
      WHEN CONCAT(event_source, ' / ', event_medium) LIKE '%/ referral%' THEN 'google / organic'
      ELSE CONCAT(event_source, ' / ', event_medium)
    END AS channel,
    event_name,
    conversions_credit,
    event_campaign,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_name = 'Signup' DESC, conversions_credit DESC) AS row_num
  FROM {{ ref ('Google_analytics_GA4_source')}}
)

SELECT
  event_date AS Signup_date,
  {{ dbt_utils.generate_surrogate_key(['user_id'])}} as dimuser_key,
  user_id,
  event_campaign As campaign,
  channel AS source_medium
FROM
  source_data
WHERE
  row_num = 1
UNION DISTINCT
SELECT
  event_date,
  {{ dbt_utils.generate_surrogate_key(['user_id'])}} as dimuser_key,
  user_id,
  event_campaign,
  channel
FROM
  (WITH CleanedEntries AS (
  SELECT
    event_date,event_campaign,
    user_id,
    CASE 
      WHEN LENGTH(user_id) = 36 THEN event_medium_source
      ELSE NULL
    END AS source_medium,
    ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY event_date) AS row_num
  FROM  {{ ref ('Google_analytics_UA_source')}}
)

SELECT event_date,{{ dbt_utils.generate_surrogate_key(['user_id'])}} as dimuser_key, user_id, event_campaign,
  CASE 
    WHEN source_medium LIKE '%/ referral' THEN 'google / organic'
    ELSE source_medium
  END AS channel
FROM CleanedEntries
WHERE source_medium IS NOT NULL AND row_num = 1) AS query_above
WHERE
  user_id NOT IN (
    SELECT user_id
    FROM source_data
    WHERE row_num = 1
  )