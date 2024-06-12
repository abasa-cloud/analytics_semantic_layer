WITH base AS (
    SELECT 
        EMPLOYEEID AS employee_id,
        EMPLOYEENUMBER AS employee_number,
        GENDER AS gender,
        MARITALSTATUS AS marital_status,
        OVER18 AS over_18
    FROM {{ ref('hr_raw') }}
)

SELECT * FROM base