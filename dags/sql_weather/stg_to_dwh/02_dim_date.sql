DROP TABLE IF EXISTS dwh.dim_date;

CREATE TABLE dwh.dim_date AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY date) AS date_id,
    date,
    EXTRACT(DAY FROM date) AS day,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(QUARTER FROM date) AS quarter,
    EXTRACT(WEEK FROM date) AS week_of_year,
    TO_CHAR(date, 'Day') AS day_name
FROM (
    SELECT DATE(weather_time) AS date FROM staging.stg_forecast
    UNION
    SELECT DATE(weather_time) AS date FROM staging.stg_hourly_weather
) d;
