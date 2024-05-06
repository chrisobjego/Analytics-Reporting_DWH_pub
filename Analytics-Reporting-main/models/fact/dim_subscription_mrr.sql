SELECT 
    invoice_date,
    EXTRACT(YEAR FROM invoice_date) AS invoice_year,
    EXTRACT(WEEK FROM invoice_date) AS invoice_week,
    SUM(mrr_sub) AS daily_mrr,
    SUM(SUM(mrr_sub)) OVER (ORDER BY invoice_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumm_daily_mrr
FROM {{ ref ('dim_subscription_id_subscribers')}}
WHERE first_invoice = TRUE AND status <> 'cancelled'
GROUP BY invoice_date
ORDER BY invoice_date DESC
 



