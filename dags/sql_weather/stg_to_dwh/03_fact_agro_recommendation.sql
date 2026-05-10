DROP TABLE IF EXISTS dwh.fact_agro_recommendation;

CREATE TABLE dwh.fact_agro_recommendation AS

SELECT
    ROW_NUMBER() OVER () AS recommendation_id,

    l.location_id,
    dd.date_id,
    f.weather_time,

    f.temperature,
    f.humidity,
    f.windspeed,
    f.precipitation,
    f.cloudcover,

    /* =========================================
       SPRAY DECISION
    ========================================= */

    CASE

        WHEN f.windspeed < 6 AND f.precipitation = 0
            THEN '✅ رش مثالي'

        WHEN f.windspeed < 6 AND f.precipitation > 0 AND f.precipitation < 1
            THEN '⚠️ رش بحذر'

        WHEN f.windspeed < 6 AND f.precipitation >= 1 AND f.precipitation < 5
            THEN '❌ غير مناسب'

        WHEN f.windspeed < 6 AND f.precipitation >= 5
            THEN '🚫 ممنوع'

        WHEN f.windspeed >= 6 AND f.windspeed < 10 AND f.precipitation = 0
            THEN '⚠️ رش بحذر'

        WHEN f.windspeed >= 6 AND f.windspeed < 10 AND f.precipitation < 1
            THEN '⚠️ رش بحذر بشروط'

        WHEN f.windspeed >= 6 AND f.windspeed < 10 AND f.precipitation < 5
            THEN '❌ غير مناسب'

        WHEN f.windspeed >= 6 AND f.windspeed < 10 AND f.precipitation >= 5
            THEN '🚫 ممنوع'

        WHEN f.windspeed >= 10 AND f.windspeed < 15
            THEN '❌ غير مفضل'

        WHEN f.windspeed >= 15
            THEN '🚫 ممنوع تمامًا'

        ELSE 'غير معروف'

    END AS spray_decision,

    /* =========================================
       SPRAY REASON
    ========================================= */

    CASE

        WHEN f.windspeed < 6 AND f.precipitation = 0
            THEN 'أفضل الظروف الممكنة للرش بدون انجراف أو غسل للمبيد'

        WHEN f.windspeed < 6 AND f.precipitation < 1
            THEN 'رذاذ خفيف قد يسبب غسل جزئي للمبيد'

        WHEN f.precipitation >= 5
            THEN 'أمطار غزيرة تسبب غسل كامل للمبيد'

        WHEN f.windspeed >= 15
            THEN 'الرياح الشديدة تسبب انجرافًا حادًا وخطرًا بيئيًا'

        ELSE 'ظروف متوسطة تحتاج تقييم قبل الرش'

    END AS spray_reason,

    /* =========================================
       DISEASE ALERT
    ========================================= */

    CASE

        WHEN f.temperature BETWEEN 18 AND 30
         AND f.humidity BETWEEN 40 AND 70
            THEN '⚠️ احتمال البياض الدقيقي'

        WHEN f.temperature BETWEEN 15 AND 27
         AND f.humidity BETWEEN 85 AND 100
            THEN '⚠️ احتمال البياض الزغبي والعفن الرمادي'

        WHEN f.temperature BETWEEN 20 AND 30
         AND f.humidity BETWEEN 70 AND 95
            THEN '⚠️ احتمال الأنثراكنوز وتبقع الأوراق'

        WHEN f.temperature BETWEEN 25 AND 35
         AND f.humidity BETWEEN 65 AND 85
            THEN '⚠️ احتمال تعفن الأسبرجلس'

        ELSE '✅ لا توجد ظروف مناسبة لانتشار الأمراض'

    END AS disease_alert,

    /* =========================================
       PREVENTIVE ADVICE
    ========================================= */

    CASE

        WHEN f.temperature BETWEEN 18 AND 30
         AND f.humidity BETWEEN 40 AND 70
            THEN 'Sulfur - Myclobutanil - Penconazole'

        WHEN f.temperature BETWEEN 15 AND 27
         AND f.humidity BETWEEN 85 AND 100
            THEN 'Mancozeb - Copper hydroxide - Metalaxyl'

        WHEN f.temperature BETWEEN 20 AND 30
         AND f.humidity BETWEEN 70 AND 95
            THEN 'Chlorothalonil - Azoxystrobin'

        WHEN f.temperature BETWEEN 25 AND 35
         AND f.humidity BETWEEN 65 AND 85
            THEN 'Copper compounds - تحسين التهوية'

        ELSE 'لا توجد توصيات وقائية حالياً'

    END AS preventive_advice,

    /* =========================================
       IRRIGATION DECISION
    ========================================= */

    CASE

        WHEN f.temperature < 2
            THEN '✅ ري وقائي عاجل من الصقيع'

        WHEN f.temperature >= 2 AND f.temperature < 7
            THEN '⚠️ احتمال صقيع'

        WHEN f.temperature >= 7 AND f.temperature < 15
            THEN '✅ مناسب للري'

        WHEN f.temperature >= 15 AND f.temperature < 28
            THEN '✅ مثالي للري'

        WHEN f.temperature >= 28 AND f.temperature < 35
            THEN '❌ غير مناسب للري نهاراً'

        WHEN f.temperature >= 35
            THEN '🚫 تجنب الري نهاراً'

        ELSE 'غير معروف'

    END AS irrigation_decision,

    /* =========================================
       IRRIGATION REASON
    ========================================= */

    CASE

        WHEN f.temperature < 2
            THEN 'الري يحمي النبات من أضرار الصقيع'

        WHEN f.temperature >= 28
            THEN 'البخر مرتفع ويُفضل الري فجراً'

        WHEN f.temperature >= 35
            THEN 'إجهاد حراري شديد للنبات'

        ELSE 'ظروف جيدة لامتصاص المياه'

    END AS irrigation_reason

FROM staging.stg_forecast f

JOIN staging.stg_location l
    ON f.city = l.location_name
    AND f.latitude = l.latitude
    AND f.longitude = l.longitude

JOIN dwh.dim_date dd
    ON DATE(f.weather_time) = dd.date;
