WITH source_data AS (
  SELECT
    user_id,
    COUNT(DISTINCT contract_id) AS rental_contracts_count,
    MIN(contract_creation_date) AS first_rental_contract_date
  FROM
    {{ ref ('prep_rental_contract')}}
  GROUP BY
    user_id
)

SELECT
  user_id,
  IF(rental_contracts_count = 0, FALSE, TRUE) AS contract_created,
  rental_contracts_count,
  first_rental_contract_date
FROM
  source_data














