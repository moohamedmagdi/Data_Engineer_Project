DROP TABLE IF EXISTS dwh.fact_chill_units;

CREATE TABLE dwh.fact_chill_units AS
SELECT
    ROW_NUMBER() OVER () AS chill_id,
    l.location_id,
    dd.date_id,

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
    ON h.city = l.location_name
    AND h.latitude = l.latitude
    AND h.longitude = l.longitude

JOIN dwh.dim_date dd
    ON DATE(h.weather_time) = dd.date

GROUP BY
    l.location_id,
    dd.date_id;
