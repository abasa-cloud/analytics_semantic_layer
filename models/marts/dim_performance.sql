WITH base AS (
    SELECT 
        EMPLOYEEID AS employee_id,
        ENVIRONMENTSATISFACTION AS environment_satisfaction,
        JOBINVOLVEMENT AS job_involvement,
        JOBSATISFACTION AS job_satisfaction,
        PERFORMANCERATING AS performance_rating,
        RELATIONSHIPSATISFACTION AS relationship_satisfaction,
        WORKLIFEBALANCE AS work_life_balance
    FROM {{ ref('hr_raw') }}
)

SELECT * FROM base