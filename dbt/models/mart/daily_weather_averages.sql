{{ config(
    materialized="incremental",
    unique_key="observation_date"
)}}

select
    date(recorded_local_time) as observation_date,
    round(avg(temperature)::numeric, 2) as avg_temp,
    round(avg(humidity)::numeric, 2) as avg_humidity,
    round(avg(wind_speed)::numeric, 2) as avg_wind_speed,
    round(avg(pressure)::numeric, 2) as avg_pressure,
    round(avg(pm2_5)::numeric, 2) as avg_pm2_5,
    round(avg(pm10)::numeric, 2) as avg_pm10,
    round(avg(co)::numeric, 2) as avg_co,
    round(avg(so2)::numeric, 2) as avg_so2,
    count(*) as record_count

from {{ ref("staging") }}

{% if is_incremental() %}
  where recorded_local_time >= (select max(observation_date) from {{ this }})
{% endif %}

group by 1
order by 1 desc