WITH base AS (
    SELECT 
        EMPLOYEEID AS employee_id,
        DAILYRATE AS daily_rate,
        HOURLYRATE AS hourly_rate,
        MONTHLYINCOME AS monthly_income,
        MONTHLYRATE AS monthly_rate,
        PERCENTSALARYHIKE AS percent_salary_hike,
        STOCKOPTIONLEVEL AS stock_option_level
    FROM {{ ref('hr_raw') }}
)

SELECT * FROM base