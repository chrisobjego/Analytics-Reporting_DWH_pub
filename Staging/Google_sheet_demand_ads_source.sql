WITH source_data AS (
  SELECT * 
  FROM 
   {{ source('Google_Sheet_coop', 'Demand cost') }}
),
    renamed as (
        SELECT
        CAST(date AS DATE) AS DATE,        
        CAST(cost AS FLOAT64) AS spend
        from source_data
    )
    SELECT  
    *
    FROM
    renamed




