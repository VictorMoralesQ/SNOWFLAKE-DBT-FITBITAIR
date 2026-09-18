select
    start_time::date                     as date_key,
    dev.device_key                       as device_key,
    sl.start_time                        as session_start_time,
    sl.end_time                          as session_end_time,
    sl.sleep_type                        as sleep_type,
    sl.minutes_asleep                    as minutes_asleep,
    sl.minutes_awake                     as minutes_awake,
    sl.minutes_in_sleep_period           as minutes_in_sleep_period,
    sl.minutes_to_fall_asleep            as minutes_to_fall_asleep,
    sl.minutes_after_wake_up             as minutes_after_wake_up
from {{ ref('stg_sleep') }} sl
left join {{ ref('dim_device') }} dev
    on dev.platform = sl.platform
   and dev.device_name = sl.device_name
   and dev.recording_method = sl.recording_method
