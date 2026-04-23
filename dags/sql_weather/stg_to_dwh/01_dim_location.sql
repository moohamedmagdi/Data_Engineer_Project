DROP TABLE IF EXISTS dwh.dim_location;

CREATE TABLE dwh.dim_location AS
SELECT DISTINCT
    location_id,
    location_name,
    latitude,
    longitude
FROM staging.stg_location
WHERE location_id IS NOT NULL;