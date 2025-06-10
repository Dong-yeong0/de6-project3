SELECT
    DD.DATE_ID,
    DS.STATION_ID,
    RAW.ROUTE_NUMBER,
    RAW.BOARDING_COUNT,
    RAW.GETOFF_COUNT,
    RAW._LOADED_AT
FROM 
    {{ ref('src_bus_usage') }} RAW
JOIN 
    {{ ref('dim_date') }} DD
    ON DD.DATE_ID = RAW.USAGE_DATE
JOIN 
    {{ ref('dim_station') }} DS
    ON DS.SOURCE_STATION_ID = RAW.NODE_ID 
    AND DS.STATION_TYPE = 'BUS'
WHERE
    RAW._LOADED_AT IS NOT NULL
{% if is_incremental() %}
    AND RAW._LOADED_AT > (SELECT MAX(_LOADED_AT) FROM {{ this }}) 
{% endif%}
