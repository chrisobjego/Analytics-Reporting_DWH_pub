WITH source_data AS (
  SELECT
    user_id,
    COUNT(DISTINCT DB_entry_id) AS ocb_count,
    
  FROM
    {{ ref ('prep_ocb')}}
  GROUP BY
    user_id
)

SELECT
  user_id,
  IF(ocb_count = 0, FALSE, TRUE) AS ocb_created,
  ocb_count

FROM
  source_data














