DROP TABLE IF EXISTS dwh.fact_current_weather;

CREATE TABLE dwh.fact_current_weather AS
SELECT
    ROW_NUMBER() OVER () AS current_weather_id,
    l.location_id,
    h.weather_time AS time,
    h.temperature AS current_weather_temperature,
    h.humidity AS current_weather_humidity,
    h.windspeed AS current_weather_windspeed,
    h.cloudcover AS current_weather_clouds,
    h.precipitation AS current_weather_rain
FROM staging.stg_hourly_weather h
JOIN staging.stg_location l
    ON h.city = l.location_name   -- ✅ نفس الفكرة
    AND h.latitude = l.latitude
    AND h.longitude = l.longitude;