WITH source_data AS (
    select 
        prep_registration.user_id AS user_id, 
        prep_registration.registration_date AS registration_date, 
        prep_rental_contract.contract_creation_date AS contract_creation_date,
        prep_rental_contract.contract_id AS contract_id
    FROM {{ ref ('prep_registration')}}  
    inner join {{ ref ('prep_rental_contract')}} on prep_rental_contract.user_id = prep_registration.user_id
),
    activation as (
        SELECT
            --{{ dbt_utils.generate_surrogate_key(['user_id'])}} as dim_activated_key,
            user_id,
            registration_date,
            COUNT(DISTINCT contract_id) AS activation_count,
            MIN(contract_creation_date) AS activation_date,
            TIMESTAMP_DIFF(MIN(contract_creation_date), registration_date, DAY) AS time_to_convert_days,
            TIMESTAMP_DIFF(MIN(contract_creation_date), registration_date, HOUR) AS time_to_convert_hours,
            TIMESTAMP_DIFF(MIN(contract_creation_date), registration_date, MINUTE) AS time_to_convert_minutes
        FROM
            source_data
            
        GROUP BY
            user_id,
            registration_date
    )
    SELECT  
    *
    FROM
    activation














