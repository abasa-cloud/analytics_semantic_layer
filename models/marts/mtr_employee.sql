with mtr_employee as ( 
    select 
        e.employee_id,
        e.gender,
        f.attrition,
        f.termination_date,
        f.hire_date,
        j.role,
        j.department,
    
    from {{ ref('dim_employee') }} e 
    left join {{ ref('dim_job') }} j 
        on e.employee_id = j.employee_id
    left join {{ ref('fct_employees') }} f 
        on e.employee_id = f.employee_id
)
select * from mtr_employee