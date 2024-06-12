with hr_raw as (
    select *
    from {{ source('semantic', 'hr_raw_') }}
)
select * from hr_raw
