with date_spine as(
    {{dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2010-01-01' as date)",
        end_date="cast('2030-01-01' as date)"
    )}}
),

final as(
    select 
        cast(format_date('%Y%m%d', date_day) as int64) as date_key,
        date_day as full_date,
        extract(year from date_day) as year,
        extract(quarter from date_day) as quarter,
        extract(month from date_day) as month,
        format_date('%B', date_day) as month_name,
        format_date('%A', date_day) as day_of_week,
        extract(dayofweek from date_day) in (1,7) as is_weekend
    from date_spine
) 

select * from final