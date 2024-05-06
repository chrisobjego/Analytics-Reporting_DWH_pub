WITH source_data AS (
  {{ dbt_date.get_date_dimension('2019-01-01', '2030-01-01' )}}
)

SELECT
  *
FROM
  source_data