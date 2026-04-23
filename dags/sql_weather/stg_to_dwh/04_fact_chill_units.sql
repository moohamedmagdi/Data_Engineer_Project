DROP TABLE IF EXISTS dwh.fact_chill_units;

CREATE TABLE dwh.fact_chill_units AS
SELECT
    ROW_NUMBER() OVER () AS chill_id,
    l.location_id,
    DATE(h.weather_time) AS date,

    -- تطبيق Positive Daily Logic
    GREATEST(
        SUM(
            CASE
                WHEN h.temperature <= 7.2 THEN 1
                WHEN h.temperature > 7.2 AND h.temperature <= 15.0 THEN 0.5
                WHEN h.temperature > 15.0 AND h.temperature <= 26.6 THEN 0
                WHEN h.temperature > 26.6 AND h.temperature <= 27.8 THEN -0.5
                ELSE -1
            END
        ),
        0
    ) AS total_chill_units

FROM staging.stg_hourly_weather h
JOIN staging.stg_location l
    ON h.city = l.location_name   -- ✅ نفس تصحيح الكود الشغال
    AND h.latitude = l.latitude
    AND h.longitude = l.longitude

GROUP BY
    l.location_id,
    DATE(h.weather_time);