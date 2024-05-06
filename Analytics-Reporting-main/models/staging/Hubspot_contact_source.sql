WITH source_data AS (
    SELECT *
    FROM 
      {{ source('hubspot', 'contact') }}
),
hubspot_contact AS (
    SELECT
        CAST(id AS STRING) AS hubspot_contact_id,
        CAST(property_objego_user_id AS STRING) AS user_id,
        CAST(is_deleted AS BOOLEAN) AS is_deleted,
        CAST(property_cb_customermrr AS INT64) AS property_cb_customermrr,
        CAST(property_cb_dueinvoicescount AS INT64) AS property_cb_dueinvoicescount,
        CAST(property_createdate AS DATE) AS property_createdate,
        CAST(property_email AS STRING) AS email,
        CAST(property_first_conversion_date AS DATE) AS property_first_conversion_date,
        CAST(property_first_conversion_event_name AS STRING) AS property_first_conversion_event_name,
        CAST(property_hs_all_contact_vids AS STRING) AS property_hs_all_contact_vids,
        CAST(property_hs_analytics_average_page_views AS INT64) AS property_hs_analytics_average_page_views,
        CAST(property_hs_analytics_first_referrer AS STRING) AS property_hs_analytics_first_referrer,
        CAST(property_hs_analytics_first_timestamp AS DATE) AS property_hs_analytics_first_timestamp,
        CAST(property_hs_analytics_first_url AS STRING) AS property_hs_analytics_first_url,
        CAST(property_hs_analytics_first_visit_timestamp AS DATE) AS property_hs_analytics_first_visit_timestamp,
        CAST(property_hs_analytics_last_referrer AS STRING) AS property_hs_analytics_last_referrer,
        CAST(property_hs_analytics_last_timestamp AS DATE) AS property_hs_analytics_last_timestamp,
        CAST(property_hs_analytics_last_url AS STRING) AS property_hs_analytics_last_url,
        CAST(property_hs_analytics_last_visit_timestamp AS DATE) AS property_hs_analytics_last_visit_timestamp,
        CAST(property_hs_analytics_num_event_completions AS INT64) AS property_hs_analytics_num_event_completions,
        CAST(property_hs_analytics_num_page_views AS INT64) AS property_hs_analytics_num_page_views,
        CAST(property_hs_analytics_num_visits AS INT64) AS property_hs_analytics_num_visits,
        CAST(property_hs_analytics_revenue AS INT64) AS property_hs_analytics_revenue,
        CAST(property_hs_date_entered_lead AS DATE) AS property_hs_date_entered_lead,
        CAST(property_hs_date_entered_subscriber AS DATE) AS property_hs_date_entered_subscriber,
        CAST(property_hs_email_bad_address AS BOOLEAN) AS property_hs_email_bad_address,
        CAST(property_hs_email_click AS INT64) AS property_hs_email_click,
        CAST(property_hs_email_delivered AS INT64) AS property_hs_email_delivered,
        CAST(property_user_status AS STRING) AS property_user_status


    FROM source_data
)
SELECT  
    *
FROM
    hubspot_contact



    






