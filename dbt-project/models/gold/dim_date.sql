with base_dates as (
    select distinct fetch_date as calendar_date
    from {{ source('fitbit_raw', 'raw_steps') }}
    union
    select distinct fetch_date
    from {{ source('fitbit_raw', 'raw_heart_rate') }}
    union
    select distinct fetch_date
    from {{ source('fitbit_raw', 'raw_sleep') }}
)

select
    calendar_date::date                           as date_key,
    calendar_date                                 as date_full,
    extract(year from calendar_date)::int         as year,
    extract(month from calendar_date)::int        as month,
    date_trunc('quarter', calendar_date)::date    as quarter_start,
    extract(quarter from calendar_date)::int      as quarter,
    extract(week from calendar_date)::int         as week_of_year,
    extract(dayofweek from calendar_date)::int    as day_of_week,
    dayname(calendar_date)                         as weekday_name,
    extract(day from calendar_date)::int          as day_of_month,
    extract(dayofyear from calendar_date)::int    as day_of_year,
    -- flag friday/weekend convenience
    case when extract(dayofweek from calendar_date) in (6, 0) then 1 else 0 end as is_weekend
from base_dates
