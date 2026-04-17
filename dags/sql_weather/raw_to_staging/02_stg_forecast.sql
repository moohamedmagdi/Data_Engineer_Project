DROP TABLE IF EXISTS staging.stg_forecast;

CREATE TABLE staging.stg_forecast AS
SELECT *
FROM (
    SELECT
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
        inserted_at,
        
        ROW_NUMBER() OVER (
            PARTITION BY city, latitude, longitude, weather_time
            ORDER BY inserted_at DESC   -- الأحدث الأول
        ) AS rn

    FROM weather.forecast_5days

    WHERE 
        temperature IS NOT NULL
        AND weather_time >= DATE_TRUNC('day', NOW())
        AND weather_time < DATE_TRUNC('day', NOW()) + INTERVAL '5 days'
) t
WHERE rn = 1;
