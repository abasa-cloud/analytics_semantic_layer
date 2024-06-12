with hr_raw as (
    select *
    from {{ source('raw_db', 'hr_raw_') }}
)
select * from hr_raw