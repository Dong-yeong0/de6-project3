SELECT DISTINCT
    TEMP.USAGE_DATE::DATE AS DATE_ID,
    EXTRACT(YEAR FROM TEMP.USAGE_DATE) AS YEAR,
    EXTRACT(MONTH FROM TEMP.USAGE_DATE) AS MONTH,
    EXTRACT(DAY FROM TEMP.USAGE_DATE) AS DAY_OF_MONTH,
    CASE DAYOFWEEK(TEMP.USAGE_DATE)
        WHEN 0 THEN '일요일'
        WHEN 1 THEN '월요일'
        WHEN 2 THEN '화요일'
        WHEN 3 THEN '수요일'
        WHEN 4 THEN '목요일'
        WHEN 5 THEN '금요일'
        WHEN 6 THEN '토요일'
    END AS DAY_OF_WEEK_KR,
    CASE 
        WHEN DAYOFWEEK(TEMP.USAGE_DATE) IN (0, 6) THEN TRUE
        ELSE FALSE
    END AS IS_WEEKEND,
    EXTRACT(QUARTER FROM TEMP.USAGE_DATE) AS QUARTER,
    TO_CHAR(TEMP.USAGE_DATE, 'YYYY-MM') AS YEAR_MONTH
FROM (
    SELECT 
        USAGE_DATE 
    FROM 
        {{ ref('src_bike_usage') }}

    UNION

    SELECT 
        USAGE_DATE 
    FROM 
        {{ ref('src_bus_usage') }}

    UNION

    SELECT 
        USAGE_DATE
    FROM
        {{ ref('src_subway_usage') }}
) TEMP