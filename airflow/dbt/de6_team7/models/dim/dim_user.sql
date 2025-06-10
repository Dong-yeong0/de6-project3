WITH distanced AS (
    SELECT
        DISTINCT GENDER AS GENDER,
        AGE_GROUP
    FROM
        {{ ref('src_bike_usage') }}
)
SELECT
    {{ 
        dbt_utils.generate_surrogate_key([
            'GENDER', 
            'AGE_GROUP'
		])
    }} AS USER_ID,
    GENDER,
    AGE_GROUP
FROM 
    distanced