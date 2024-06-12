WITH base AS (
    SELECT 
        EMPLOYEEID AS employee_id,
        JOBROLE AS role,
        JOBLEVEL AS role_level,
        DEPARTMENT AS department,
        EDUCATION AS education_level,
        EDUCATIONFIELD AS education_field
    FROM {{ ref('hr_raw') }}
)

SELECT * FROM base