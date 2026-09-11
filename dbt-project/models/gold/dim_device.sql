with device_sources as (
    select distinct
        platform,
        device_name,
        recording_method
    from {{ ref('stg_steps') }}
    union
    select distinct
        platform,
        device_name,
        recording_method
    from {{ ref('stg_heart_rate') }}
    union
    select distinct
        platform,
        device_name,
        recording_method
    from {{ ref('stg_sleep') }}
)

select
    row_number() over (order by platform, device_name, recording_method) as device_key,
    platform,
    device_name,
    recording_method
from device_sources
