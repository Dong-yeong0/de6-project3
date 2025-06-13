SELECT
    NODE_ID,
    ARS_ID,
    STOP_NAME_KR,
    LATITUDE,
    LONGITUDE,
    STOP_TYPE
FROM
    {{ source('raw', 'bus_location') }}
WHERE
    LATITUDE IS NOT NULL
AND
    LONGITUDE IS NOT NULL