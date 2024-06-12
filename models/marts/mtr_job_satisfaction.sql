with mtr_job_satisfaction as (
    select 
        e.employee_id,
        f.attrition,
        p.job_satisfaction,
        p.performance_rating,
        j.role, 
        j.department,
        j.education_field

    from {{ ref('dim_employee') }} e 
    left join {{ ref('fct_employees') }} f 
        on e.employee_id = f.employee_id 
    left join {{ ref('dim_job') }} j 
        on e.employee_id = j.employee_id
    left join {{ ref('dim_performance') }} p 
        on e.employee_id = p.employee_id
)
select * from mtr_job_satisfaction