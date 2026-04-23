DROP TABLE IF EXISTS dwh.fact_forecast;

CREATE TABLE dwh.fact_forecast AS
SELECT
    ROW_NUMBER() OVER () AS forecast_id,
    l.location_id,
    f.weather_time AS time,
    f.temperature AS forecast_temperature,
    f.humidity AS forecast_humidity,
    f.windspeed AS forecast_windspeed,
    f.cloudcover AS forecast_clouds,
    f.precipitation AS forecast_rain
FROM staging.stg_forecast f
JOIN staging.stg_location l
    ON f.city = l.location_name   -- ✅ الصح هنا
    AND f.latitude = l.latitude
    AND f.longitude = l.longitude;


