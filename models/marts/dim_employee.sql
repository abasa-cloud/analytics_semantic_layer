
WITH base AS (
    SELECT 
        EMPLOYEEID AS employee_id,
        GENDER AS gender,
        MARITALSTATUS AS marital_status,
        OVER18 AS is_adult
    FROM {{ ref('base_semantic_hr_raw') }}
)

SELECT * FROM base