select
        {{ dbt_utils.generate_surrogate_key(['Postgres_registration_source.user_id'])}} as registration_key,
        Postgres_registration_source.user_id AS user_id,
        Postgres_registration_source.registration_date AS registration_date,
        Postgres_registration_source.login_count AS login_count,
        Postgres_registration_source.login_date AS last_login_date,
        Postgres_registration_source.user_status AS user_status   
from {{ ref ('Postgres_registration_source')}}  







