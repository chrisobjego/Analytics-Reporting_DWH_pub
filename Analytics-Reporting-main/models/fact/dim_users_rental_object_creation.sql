WITH source_data AS (
  SELECT 
    user_id,
    MIN(creation_date) AS first_object_creation_date,
    COUNT(IF(ro.object_type IN ('MULTI_FAMILY_HOUSE', 'ADDRESS_CONTAINER', 'ONE_FAMILY_HOUSE','OTHER_RENTAL_OBJECT','PARKING_SPACE'), 1, NULL)) as object_count,
    COUNT(IF(ro.object_type = 'MULTI_FAMILY_HOUSE', 1, NULL)) as multi_family_house,
    COUNT(IF(ro.object_type = 'ADDRESS_CONTAINER', 1, NULL)) as address_container,
    COUNT(IF(ro.object_type = 'ONE_FAMILY_HOUSE', 1, NULL)) as one_family_house,
    COUNT(IF(ro.object_type = 'OTHER_RENTAL_OBJECT', 1, NULL)) as other_objects,
    COUNT(IF(ro.object_type = 'PARKING_SPACE', 1, NULL)) as parking_space,  
    COUNT(IF(ro.object_type NOT IN ('MULTI_FAMILY_HOUSE', 'ADDRESS_CONTAINER'), 1, NULL)) as unit_count,
    CASE 
        WHEN COUNT(IF(ro.object_type IN ('MULTI_FAMILY_HOUSE', 'ADDRESS_CONTAINER', 'ONE_FAMILY_HOUSE','OTHER_RENTAL_OBJECT','PARKING_SPACE'), 1, NULL)) = 0 THEN FALSE
        ELSE TRUE
    END AS object_created    
  FROM
    {{ ref ('Postgres_rental_object_source')}} as ro
  GROUP BY 
    user_id
)

SELECT
  user_id,
  object_created,
  first_object_creation_date,
  object_count,
  multi_family_house,
  address_container,
  one_family_house,
  other_objects,
  parking_space, 
  unit_count
FROM
  source_data














