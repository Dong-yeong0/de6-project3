WITH BIKE_LOCATION AS (
    SELECT
        DISTINCT BKU.STATION_ID AS SOURCE_STATION_ID,
        BKU.STATION_NAME,
        'BIKE' AS STATION_TYPE,
        BKL.ADDRESS1 AS ADDRESS,
        '서울' AS CITY,
        BKL.LATITUDE,
        BKL.LONGITUDE
    FROM
        {{ ref('src_bike_usage') }} AS BKU
    JOIN
        {{ ref('src_bike_location') }} AS BKL
        ON BKU.STATION_ID = BKL.STATION_ID
),

BUS_STOP_LOCATION AS (
    SELECT
        DISTINCT BSU.NODE_ID AS SOURCE_STATION_ID,
        BSL.STOP_NAME_KR AS STATION_NAME,
        'BUS' AS STATION_TYPE,
        NULL AS ADDRESS,
        '서울' AS CITY,
        BSL.LATITUDE,
        BSL.LONGITUDE
    FROM
        {{ ref('src_bus_usage') }} AS BSU
    JOIN
        {{ ref('src_bus_stop_location') }} AS BSL
        ON BSU.NODE_ID = BSL.NODE_ID
),

SUBWAY_LOCATION AS (
    SELECT
        CONCAT(SWU.LINE, ' - ', SWU.STATION) AS SOURCE_STATION_ID,
        SWU.STATION AS STATION_NAME,
        'SUBWAY' AS STATION_TYPE,
        NULL AS ADDRESS,
        '서울' AS CITY,
        SWL.LATITUDE,
        SWL.LONGITUDE
    FROM (
        SELECT
            "LINE",
            STATION
        FROM
            {{ ref('src_subway_usage') }}
        GROUP BY
            1, 2
    ) AS SWU
    JOIN
        {{ ref('src_subway_location') }} AS SWL
        ON SWU.LINE = CONCAT(SWL.LINE_NO, '호선')
        AND SWU.STATION = SWL.STATION_NAME
        AND SWU.LINE IN ('1호선', '2호선', '3호선', '4호선', '5호선', '6호선', '7호선', '8호선')
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['SOURCE_STATION_ID', 'STATION_NAME']) }} AS STATION_ID,
    SOURCE_STATION_ID,
    STATION_NAME,
    STATION_TYPE,
    ADDRESS,
    CITY,
    LATITUDE,
    LONGITUDE
FROM 
    BIKE_LOCATION

UNION ALL

SELECT
    {{ dbt_utils.generate_surrogate_key(['SOURCE_STATION_ID', 'STATION_NAME']) }} AS STATION_ID,
    SOURCE_STATION_ID,
    STATION_NAME,
    STATION_TYPE,
    ADDRESS,
    CITY,
    LATITUDE,
    LONGITUDE
FROM 
    BUS_STOP_LOCATION

UNION ALL

SELECT
    {{ dbt_utils.generate_surrogate_key(['SOURCE_STATION_ID', 'STATION_NAME']) }} AS STATION_ID,
    SOURCE_STATION_ID,
    STATION_NAME,
    STATION_TYPE,
    ADDRESS,
    CITY,
    LATITUDE,
    LONGITUDE
FROM 
    SUBWAY_LOCATION