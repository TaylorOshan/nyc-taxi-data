
CREATE OR REPLACE FUNCTION DateDiff (units VARCHAR(30), start_t TIMESTAMP, end_t TIMESTAMP) 
     RETURNS INT AS $$
   DECLARE
     diff_interval INTERVAL; 
     diff INT = 0;
     years_diff INT = 0;
   BEGIN
     IF units IN ('yy', 'yyyy', 'year', 'mm', 'm', 'month') THEN
       years_diff = DATE_PART('year', end_t) - DATE_PART('year', start_t);
 
       IF units IN ('yy', 'yyyy', 'year') THEN
         -- SQL Server does not count full years passed (only difference between year parts)
         RETURN years_diff;
       ELSE
         -- If end month is less than start month it will subtracted
         RETURN years_diff * 12 + (DATE_PART('month', end_t) - DATE_PART('month', start_t)); 
       END IF;
     END IF;
 
     -- Minus operator returns interval 'DDD days HH:MI:SS'  
     diff_interval = end_t - start_t;
 
     diff = diff + DATE_PART('day', diff_interval);
 
     IF units IN ('wk', 'ww', 'week') THEN
       diff = diff/7;
       RETURN diff;
     END IF;
 
     IF units IN ('dd', 'd', 'day') THEN
       RETURN diff;
     END IF;
 
     diff = diff * 24 + DATE_PART('hour', diff_interval); 
 
     IF units IN ('hh', 'hour') THEN
        RETURN diff;
     END IF;
 
     diff = diff * 60 + DATE_PART('minute', diff_interval);
 
     IF units IN ('mi', 'n', 'minute') THEN
        RETURN diff;
     END IF;
 
     diff = diff * 60 + DATE_PART('second', diff_interval);
 
     RETURN diff;
   END;
   $$ LANGUAGE plpgsql;

CREATE TABLE tmp_points AS
SELECT
  id,
  ST_SetSRID(ST_MakePoint(pickup_longitude, pickup_latitude), 4326) as pickup,
  ST_SetSRID(ST_MakePoint(dropoff_longitude, dropoff_latitude), 4326) as dropoff
FROM yellow_tripdata_staging;  

CREATE INDEX idx_tmp_points_pickup ON tmp_points USING gist (pickup);
CREATE INDEX idx_tmp_points_dropoff ON tmp_points USING gist (dropoff);

CREATE TABLE tmp_pickups_t AS
SELECT t.id, n.gid
FROM tmp_points t, nyct2010 n
WHERE ST_Within(t.pickup, n.geom);

CREATE TABLE tmp_dropoffs_t AS
SELECT t.id, n.gid
FROM tmp_points t, nyct2010 n
WHERE ST_Within(t.dropoff, n.geom);

CREATE TABLE tmp_pickups_bg AS
SELECT t.id, n.gid
FROM tmp_points t, nycbg2010 n
WHERE ST_Within(t.pickup, n.geom);

CREATE TABLE tmp_dropoffs_bg AS
SELECT t.id, n.gid
FROM tmp_points t, nycbg2010 n
WHERE ST_Within(t.dropoff, n.geom);

CREATE TABLE tmp_pickups_b AS
SELECT t.id, n.gid
FROM tmp_points t, nycb2010 n
WHERE ST_Within(t.pickup, n.geom);

CREATE TABLE tmp_dropoffs_b AS
SELECT t.id, n.gid
FROM tmp_points t, nycb2010 n
WHERE ST_Within(t.dropoff, n.geom);

CREATE TABLE tmp_pickups_tz AS
SELECT t.id, n.gid
FROM tmp_points t, taxi_zones n
WHERE ST_Within(t.pickup, n.geom);

CREATE TABLE tmp_dropoffs_tz AS
SELECT t.id, n.gid
FROM tmp_points t, taxi_zones n
WHERE ST_Within(t.dropoff, n.geom);

INSERT INTO trips
(cab_type_id, vendor_id, pickup_datetime, dropoff_datetime, pickup_year, pickup_quarter, pickup_month, pickup_week, pickup_day, pickup_dow, pickup_doy, pickup_hour, dropoff_year, dropoff_quarter, dropoff_month, dropoff_week, dropoff_day, dropoff_dow, dropoff_doy, dropoff_hour, passenger_count, trip_distance, trip_duration, pickup_longitude, pickup_latitude, rate_code_id, store_and_fwd_flag, dropoff_longitude, dropoff_latitude, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, pickup, dropoff, pickup_t_gid, dropoff_t_gid, t_tuple, pickup_bg_gid, dropoff_bg_gid, bg_tuple, pickup_b_gid, dropoff_b_gid, b_tuple, pickup_tz_gid, dropoff_tz_gid, tz_tuple)
SELECT
  cab_types.id,
  vendor_id,
  tpep_pickup_datetime::timestamp,
  tpep_dropoff_datetime::timestamp,
  EXTRACT(YEAR FROM tpep_pickup_datetime::timestamp),
  EXTRACT(QUARTER FROM tpep_pickup_datetime::timestamp),
  EXTRACT(MONTH FROM tpep_pickup_datetime::timestamp),
  EXTRACT(WEEK FROM tpep_pickup_datetime::timestamp),
  EXTRACT(DAY FROM tpep_pickup_datetime::timestamp),
  EXTRACT(DOW FROM tpep_pickup_datetime::timestamp),
  EXTRACT(DOY FROM tpep_pickup_datetime::timestamp),
  EXTRACT(HOUR FROM tpep_pickup_datetime::timestamp),
  EXTRACT(YEAR FROM tpep_dropoff_datetime::timestamp),
  EXTRACT(QUARTER FROM tpep_dropoff_datetime::timestamp),
  EXTRACT(MONTH FROM tpep_dropoff_datetime::timestamp),
  EXTRACT(WEEK FROM tpep_dropoff_datetime::timestamp),
  EXTRACT(DAY FROM tpep_dropoff_datetime::timestamp),
  EXTRACT(DOW FROM tpep_dropoff_datetime::timestamp),
  EXTRACT(DOY FROM tpep_dropoff_datetime::timestamp),
  EXTRACT(HOUR FROM tpep_dropoff_datetime::timestamp),
  passenger_count::integer,
  trip_distance::numeric,
  DATEDIFF('seconds', tpep_pickup_datetime::timestamp, tpep_dropoff_datetime::timestamp),
  CASE WHEN pickup_longitude != 0 THEN pickup_longitude END,
  CASE WHEN pickup_latitude != 0 THEN pickup_latitude END,
  rate_code_id::integer,
  store_and_fwd_flag,
  CASE WHEN dropoff_longitude != 0 THEN dropoff_longitude END,
  CASE WHEN dropoff_latitude != 0 THEN dropoff_latitude END,
  payment_type,
  fare_amount::numeric,
  extra::numeric,
  mta_tax::numeric,
  tip_amount::numeric,
  tolls_amount::numeric,
  improvement_surcharge::numeric,
  total_amount::numeric,
  CASE
    WHEN pickup_longitude != 0 AND pickup_latitude != 0
    THEN ST_SetSRID(ST_MakePoint(pickup_longitude, pickup_latitude), 4326)
  END,
  CASE
    WHEN dropoff_longitude != 0 AND dropoff_latitude != 0
    THEN ST_SetSRID(ST_MakePoint(dropoff_longitude, dropoff_latitude), 4326)
  END,
  tmp_pickups_t.gid,
  tmp_dropoffs_t.gid,
  CASE
    WHEN tmp_pickups_t.gid IS NOT NULL AND tmp_dropoffs_t.gid IS NOT NULL
    THEN (tmp_pickups_t.gid, tmp_dropoffs_t.gid)
  END,
  tmp_pickups_bg.gid,
  tmp_dropoffs_bg.gid,
  CASE
    WHEN tmp_pickups_bg.gid IS NOT NULL AND tmp_dropoffs_bg.gid IS NOT NULL
    THEN (tmp_pickups_bg.gid, tmp_dropoffs_bg.gid)
  END,
  tmp_pickups_b.gid,
  tmp_dropoffs_b.gid,
  CASE
    WHEN tmp_pickups_b.gid IS NOT NULL AND tmp_dropoffs_b.gid IS NOT NULL
    THEN (tmp_pickups_b.gid, tmp_dropoffs_b.gid)
  END,
  tmp_pickups_tz.gid,
  tmp_dropoffs_tz.gid,
  CASE
    WHEN tmp_pickups_tz.gid IS NOT NULL AND tmp_dropoffs_tz.gid IS NOT NULL
    THEN (tmp_pickups_tz.gid, tmp_dropoffs_tz.gid)
  END
FROM
  yellow_tripdata_staging
    INNER JOIN cab_types ON cab_types.type = 'yellow'
    LEFT JOIN tmp_pickups_t ON yellow_tripdata_staging.id = tmp_pickups_t.id
    LEFT JOIN tmp_dropoffs_t ON yellow_tripdata_staging.id = tmp_dropoffs_t.id
    LEFT JOIN tmp_pickups_bg ON yellow_tripdata_staging.id = tmp_pickups_bg.id
    LEFT JOIN tmp_dropoffs_bg ON yellow_tripdata_staging.id = tmp_dropoffs_bg.id
    LEFT JOIN tmp_pickups_b ON yellow_tripdata_staging.id = tmp_pickups_b.id
    LEFT JOIN tmp_dropoffs_b ON yellow_tripdata_staging.id = tmp_dropoffs_b.id
    LEFT JOIN tmp_pickups_tz ON yellow_tripdata_staging.id = tmp_pickups_tz.id
    LEFT JOIN tmp_dropoffs_tz ON yellow_tripdata_staging.id = tmp_dropoffs_tz.id;

TRUNCATE TABLE yellow_tripdata_staging;
DROP TABLE tmp_points;
DROP TABLE tmp_pickups_t;
DROP TABLE tmp_dropoffs_t;
DROP TABLE tmp_pickups_bg;
DROP TABLE tmp_dropoffs_bg;
DROP TABLE tmp_pickups_b;
DROP TABLE tmp_dropoffs_b;
DROP TABLE tmp_pickups_tz;
DROP TABLE tmp_dropoffs_tz;
