{{ config(
    materialized="view",
    unique_key="id"
)}}

with source as (
    select *
    from {{ source("raw_weather_source", "nairobi_weather_data") }}
),

de_dup as (
    select
        *,
        row_number() over(partition by time order by recorded_at desc) as rn 
    from source
)

select
    id,
    temperature,
    weather_description,
    wind_speed,
    humidity,
    pressure,
    time as local_weather_time,
    (recorded_at + (utc_offset || ' hours')::interval) as recorded_local_time,
    co,
    so2,
    pm2_5,
    pm10
from de_dup
where rn = 1