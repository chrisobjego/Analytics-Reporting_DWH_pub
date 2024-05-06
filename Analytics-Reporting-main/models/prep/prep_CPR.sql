WITH DailyRegistrations AS (
  SELECT
    DATE(registration_date) AS registration_date,
    COUNT(DISTINCT user_id) AS daily_registrations
  FROM {{ ref('Postgres_registration_source') }}
  GROUP BY registration_date
),

CostPerRegistration AS (
  SELECT
    DATE,
    total_daily_cost / daily_registrations AS CPR
  FROM {{ ref('prep_cost') }}
  JOIN DailyRegistrations ON DATE = registration_date
)

SELECT
  DATE(d.registration_date) AS registration_date,
  d.user_id,
  IFNULL(cpr.CPR, 0) AS CPR
FROM {{ ref('prep_registration') }} AS d
LEFT JOIN CostPerRegistration AS cpr ON DATE(d.registration_date) = DATE(cpr.DATE)

