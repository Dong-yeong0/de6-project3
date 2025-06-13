
SELECT
    DD.DATE_ID,
    DS.STATION_ID,
    DU.USER_ID,
    RAW.RENTAL_TYPE_CODE,
    RAW.USAGE_TIME,
    RAW.USAGE_COUNT,
    RAW.CALORIES,
    RAW.CARBON_EMISSION,
    RAW.DISTANCE,
    RAW.DURATION,
    RAW._LOADED_AT
FROM 
    {{ ref('src_bike_usage') }} RAW
JOIN 
    {{ ref('dim_date') }} DD
    ON DD.DATE_ID = RAW.USAGE_DATE
JOIN 
    {{ ref('dim_station') }} DS
    ON DS.SOURCE_STATION_ID = RAW.STATION_ID 
    AND DS.STATION_TYPE = 'BIKE'
JOIN 
    {{ ref('dim_user') }} DU
    ON DU.GENDER = RAW.GENDER 
    AND DU.AGE_GROUP = RAW.AGE_GROUP
WHERE
    RAW._LOADED_AT IS NOT NULL
{% if is_incremental() %}
    AND RAW._LOADED_AT > (SELECT MAX(_LOADED_AT) FROM {{ this }}) 
{% endif%}
