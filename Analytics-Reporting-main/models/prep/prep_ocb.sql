WITH source_data AS (
    SELECT *
    FROM 
      {{ ref ('Postgres_OCB_source')}}  
),
    ocb as (
        SELECT
        --{{ dbt_utils.generate_surrogate_key(['DB_entry_id'])}} as dim_ocb_key,
        DB_entry_id,
        creation_date AS ocb_creation_date,
        user_id,
        object_id,
        billing_period_id,
        cost_summary,
        cost_net_summary
        
        from source_data

    )
    SELECT  
    *
    FROM
    ocb













