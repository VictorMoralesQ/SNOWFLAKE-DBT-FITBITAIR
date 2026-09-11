with flattened as (
    select
        fetch_date,
        loaded_at,
        sl.value:sleep.interval.startTime::timestamp_ntz as start_time,
        sl.value:sleep.interval.endTime::timestamp_ntz   as end_time,
        sl.value:sleep.type::varchar                     as sleep_type,
        sl.value:sleep.summary.minutesAsleep::float       as minutes_asleep,
        sl.value:sleep.summary.minutesAwake::float        as minutes_awake,
        sl.value:sleep.summary.minutesInSleepPeriod::float as minutes_in_sleep_period,
        sl.value:sleep.summary.minutesToFallAsleep::float  as minutes_to_fall_asleep,
        sl.value:sleep.summary.minutesAfterWakeUp::float   as minutes_after_wake_up,
        sl.value:dataSource.platform::varchar             as platform,
        sl.value:dataSource.device.displayName::varchar   as device_name,
        sl.value:dataSource.recordingMethod::varchar      as recording_method
    from {{ source('fitbit_raw', 'raw_sleep') }},
    lateral flatten(input => payload:dataPoints) sl
)

select * from flattened
