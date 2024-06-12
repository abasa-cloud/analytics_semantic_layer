WITH base AS (
    SELECT 
        EMPLOYEEID AS employee_id,
        HIREDATE AS hire_date,
        AGE AS age,
        ATTRITION AS attrition,
        TERMINATIONDATE AS termination_date,
        VOLUNTARYATTRITION AS voluntary_attrition,
        EXITINTERVIEWCOMPLETED AS final_interview_completed,
        BUSINESSTRAVEL AS business_travel,
        DISTANCEFROMHOME AS distance_from_home,
        NUMCOMPANIESWORKED AS num_companies_worked,
        STANDARDHOURS AS standard_hours,
        TOTALWORKINGYEARS AS total_working_years,
        TRAININGTIMESLASTYEAR AS training_times_last_year,
        YEARSATCOMPANY AS years_at_company,
        YEARSINCURRENTROLE AS years_in_current_role,
        YEARSSINCELASTPROMOTION AS years_since_last_promotion,
        YEARSWITHCURRMANAGER AS years_with_curr_manager
    FROM {{ ref('hr_raw') }}
)

SELECT * FROM base
