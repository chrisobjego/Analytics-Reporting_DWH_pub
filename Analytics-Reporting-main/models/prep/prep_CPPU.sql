WITH DailyNewPurchase AS (
  SELECT
    DATE(invoice_date) AS invoice_date,
    COUNT(DISTINCT user_id) AS daily_NewPurchase
  FROM {{ ref('Chargebee_invoice_source') }}
  WHERE first_invoice = true
  GROUP BY invoice_date
),

CostPerNewPurchase AS (
  SELECT
    DATE,
    total_daily_cost / daily_NewPurchase AS CPPU
  FROM {{ ref('prep_cost') }}
  JOIN DailyNewPurchase ON DATE = invoice_date
)

SELECT
  DATE(d.invoice_date) AS invoice_date,
  d.user_id,
  IFNULL(cppu.CPPU, 0) AS CPPU
FROM (
  SELECT 
    DATE(invoice_date) AS invoice_date,
    user_id,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY invoice_date) AS row_num
  FROM {{ ref('Chargebee_invoice_source') }}
  WHERE first_invoice = true
) AS d
LEFT JOIN CostPerNewPurchase AS cppu ON DATE(d.invoice_date) = DATE(cppu.DATE)
WHERE row_num = 1


