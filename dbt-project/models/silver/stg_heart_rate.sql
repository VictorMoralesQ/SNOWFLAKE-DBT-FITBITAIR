with flattened as (
    select
        fetch_date,
        loaded_at,
        hr.value:heartRate.sampleTime.physicalTime::timestamp_ntz as sample_time,
        hr.value:heartRate.bpm::int                                as bpm,
        hr.value:dataSource.platform::varchar                      as platform,
        hr.value:dataSource.device.displayName::varchar            as device_name,
        hr.value:dataSource.recordingMethod::varchar               as recording_method
    from {{ source('fitbit_raw', 'raw_heart_rate') }},
    lateral flatten(input => payload:dataPoints) hr
)

select * from flattened
