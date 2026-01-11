{{ config(
    materialized="table"
) }}

WITH staging_data AS (
    SELECT * FROM {{ ref("staging") }}
),

nairobi_final AS (
    SELECT
        id,
        temperature AS temp_c,
        weather_description,
        wind_speed,
        humidity,
        pressure,
        recorded_local_time,
        co,
        so2,
        pm2_5,
        pm10,
        EXTRACT(HOUR FROM recorded_local_time) AS observation_hour,
        EXTRACT(DOW FROM recorded_local_time) AS day_of_week
    FROM staging_data
)

SELECT * FROM nairobi_final