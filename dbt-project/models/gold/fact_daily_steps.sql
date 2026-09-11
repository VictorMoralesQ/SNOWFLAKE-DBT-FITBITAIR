with joined as (
    select
        d.date_key,
        dev.device_key,
        sum(s.step_count) as total_steps
    from {{ ref('stg_steps') }} s
    left join {{ ref('dim_date') }} d on d.date_full = s.interval_start_time::date
    left join {{ ref('dim_device') }} dev
        on dev.platform = s.platform
       and dev.device_name = s.device_name
       and dev.recording_method = s.recording_method
    group by d.date_key, dev.device_key
)

select * from joined
