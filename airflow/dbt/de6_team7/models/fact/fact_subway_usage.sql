SELECT
    DD.DATE_ID,
    DS.STATION_ID,
    RAW.LINE,
    COALESCE(RAW.GT_ON, 0) AS BOARDING_COUNT,
    COALESCE(RAW.GT_OFF, 0) AS GETOFF_COUNT,
    RAW._LOADED_AT
FROM 
    {{ ref('src_subway_usage') }} RAW
JOIN 
    {{ ref('dim_station') }} DS
    ON DS.SOURCE_STATION_ID = RAW.SOURCE_STATION_ID
    AND STATION_TYPE = 'SUBWAY'
JOIN
    {{ ref('dim_date') }} DD
    ON DD.DATE_ID = RAW.USAGE_DATE
WHERE
    RAW._LOADED_AT IS NOT NULL
{% if is_incremental() %}
    AND RAW._LOADED_AT > (SELECT MAX(_LOADED_AT) FROM {{ this }}) 
{% endif%}
