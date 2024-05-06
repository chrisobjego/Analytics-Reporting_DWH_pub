WITH source_data AS (
  SELECT * 
  FROM
    {{ source('google_universal_analytics', 'user_id_signup_records') }}
),
    renamed as (
        SELECT
        CAST(DATE AS DATE) AS event_date,
        CAST(objegoUserId_UA_135670334_1___UA__Google_Analytics AS STRING) AS user_id,
        CAST(Campaign___UA__Google_Analytics AS STRING) AS event_campaign,
        CAST(Source__medium___UA__Google_Analytics AS STRING) AS event_medium_source
        from source_data
    )

    SELECT  
    *
    FROM
    renamed