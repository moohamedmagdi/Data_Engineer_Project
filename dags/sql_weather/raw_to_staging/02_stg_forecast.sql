DROP TABLE IF EXISTS staging.stg_forecast;

CREATE TABLE staging.stg_forecast AS
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
FROM weather.forecast_5days
WHERE 
    temperature IS NOT NULL
    AND weather_time >= NOW() - INTERVAL '5 days';
