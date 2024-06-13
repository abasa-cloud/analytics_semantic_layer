
WITH base AS (
    SELECT distinct
        JOBROLE || ' ' || JOBLEVEL AS role_id,
        Jobrole as job_role,
        department
        --EDUCATION AS education_level,
        --EDUCATIONFIELD AS education_field
    FROM {{ ref('base_semantic_hr_raw') }}
)

SELECT * FROM base
