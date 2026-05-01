DROP TABLE IF EXISTS dwh.fact_spray_recommendation;

CREATE TABLE dwh.fact_spray_recommendation AS
SELECT
    ROW_NUMBER() OVER () AS spray_id,

    l.location_id,
    dd.date_id,
    f.weather_time,

    f.temperature,
    f.humidity,
    f.windspeed,
    f.precipitation,

    CASE
        WHEN f.windspeed <= 6 AND f.precipitation = 0
            THEN '✅ رش مثالي'

        WHEN f.windspeed <= 6 AND f.precipitation <= 1
            THEN '⚠️ رش بحذر'

        WHEN f.windspeed <= 6 AND f.precipitation <= 5
            THEN '❌ غير مناسب'

        WHEN f.windspeed <= 6 AND f.precipitation > 5
            THEN '🚫 ممنوع'

        WHEN f.windspeed <= 10 AND f.precipitation = 0
            THEN '⚠️ رش بحذر'

        WHEN f.windspeed <= 10 AND f.precipitation <= 1
            THEN '⚠️ رش بشروط'

        WHEN f.windspeed <= 10 AND f.precipitation <= 5
            THEN '❌ غير مناسب'

        WHEN f.windspeed <= 10 AND f.precipitation > 5
            THEN '🚫 ممنوع'

        WHEN f.windspeed <= 15
            THEN '❌ غير مفضل'

        WHEN f.windspeed > 15
            THEN '🚫 ممنوع تمامًا'

        ELSE 'غير معروف'
    END AS spray_application,

    CASE
        WHEN f.windspeed <= 6 AND f.precipitation = 0
            THEN 'أفضل ظروف رش'

        WHEN f.windspeed > 15
            THEN 'رياح شديدة تسبب فقد كامل'

        WHEN f.precipitation > 5
            THEN 'أمطار غزيرة تغسل المبيد'

        ELSE 'ظروف تحتاج تقييم'
    END AS reason_spray_application

FROM staging.stg_forecast f

JOIN staging.stg_location l
    ON f.city = l.location_name
    AND f.latitude = l.latitude
    AND f.longitude = l.longitude

JOIN dwh.dim_date dd
    ON DATE(f.weather_time) = dd.date;
