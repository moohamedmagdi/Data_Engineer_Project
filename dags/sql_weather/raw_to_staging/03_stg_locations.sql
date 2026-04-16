DROP TABLE IF EXISTS staging.stg_locations;

CREATE TABLE staging.stg_locations AS
SELECT
    ROW_NUMBER() OVER (ORDER BY location_name) AS location_id,
    location_name,
    latitude,
    longitude
FROM (
    SELECT DISTINCT
        city AS location_name,
        latitude,
        longitude
    FROM (
        SELECT city, latitude, longitude FROM staging.stg_hourly_weather
        UNION
        SELECT city, latitude, longitude FROM staging.stg_forecast
    ) t
) x;
