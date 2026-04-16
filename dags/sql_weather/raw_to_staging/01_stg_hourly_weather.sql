DROP TABLE IF EXISTS staging.stg_hourly_weather CASCADE;
DROP TYPE IF EXISTS staging.stg_hourly_weather CASCADE;

CREATE TABLE staging.stg_hourly_weather AS
SELECT DISTINCT
    city,
    latitude,
    longitude,
    weather_time,
    temperature,
    windspeed,
    humidity,
    pressure,
    cloudcover,
    precipitation,
    inserted_at
FROM weather.hourly_weather
WHERE 
    temperature IS NOT NULL
    AND humidity IS NOT NULL
    AND windspeed IS NOT NULL
    AND weather_time IS NOT NULL;
