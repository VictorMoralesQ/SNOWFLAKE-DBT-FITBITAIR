with flattened as (
    select
        fetch_date,
        loaded_at,
        sp.value:steps.interval.startTime::timestamp_ntz as interval_start_time,
        sp.value:steps.interval.endTime::timestamp_ntz   as interval_end_time,
        sp.value:steps.count::int                         as step_count,
        sp.value:dataSource.platform::varchar             as platform,
        sp.value:dataSource.device.displayName::varchar   as device_name,
        sp.value:dataSource.recordingMethod::varchar      as recording_method
    from {{ source('fitbit_raw', 'raw_steps') }},
    lateral flatten(input => payload:dataPoints) sp
)

select * from flattened
