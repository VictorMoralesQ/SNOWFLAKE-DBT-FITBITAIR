select
    sample_time::date          as date_key,
    extract(hour from sample_time)::int as hour_of_day,
    dev.device_key             as device_key,
    hr.bpm                     as heart_rate_bpm
from {{ ref('stg_heart_rate') }} hr
left join {{ ref('dim_device') }} dev
    on dev.platform = hr.platform
   and dev.device_name = hr.device_name
   and dev.recording_method = hr.recording_method
