CREATE EXTENSION postgis;

CREATE TABLE green_tripdata_staging (
  id serial primary key,
  vendor_id varchar,
  lpep_pickup_datetime varchar,
  lpep_dropoff_datetime varchar,
  store_and_fwd_flag varchar,
  rate_code_id varchar,
  pickup_longitude numeric,
  pickup_latitude numeric,
  dropoff_longitude numeric,
  dropoff_latitude numeric,
  passenger_count varchar,
  trip_distance varchar,
  fare_amount varchar,
  extra varchar,
  mta_tax varchar,
  tip_amount varchar,
  tolls_amount varchar,
  ehail_fee varchar,
  improvement_surcharge varchar,
  total_amount varchar,
  payment_type varchar,
  trip_type varchar,
  junk1 varchar,
  junk2 varchar
);
/*
N.B. junk columns are there because green_tripdata file headers are
inconsistent with the actual data, e.g. header says 20 or 21 columns per row,
but data actually has 22 or 23 columns per row, which COPY doesn't like.
junk1 and junk2 should always be null
*/

CREATE TABLE yellow_tripdata_staging (
  id serial primary key,
  vendor_id varchar,
  tpep_pickup_datetime varchar,
  tpep_dropoff_datetime varchar,
  passenger_count varchar,
  trip_distance varchar,
  pickup_longitude numeric,
  pickup_latitude numeric,
  rate_code_id varchar,
  store_and_fwd_flag varchar,
  dropoff_longitude numeric,
  dropoff_latitude numeric,
  payment_type varchar,
  fare_amount varchar,
  extra varchar,
  mta_tax varchar,
  tip_amount varchar,
  tolls_amount varchar,
  improvement_surcharge varchar,
  total_amount varchar
);



CREATE TABLE taxi_zone_lookups (
  location_id integer primary key,
  borough varchar,
  zone varchar,
  service_zone varchar,
  nyct2010_ntacode varchar
);




CREATE TABLE cab_types (
  id serial primary key,
  type varchar
);

INSERT INTO cab_types (type) SELECT 'yellow';
INSERT INTO cab_types (type) SELECT 'green';

CREATE TABLE trips (
  id serial primary key,
  cab_type_id integer,
  vendor_id varchar,
  pickup_datetime timestamp without time zone,
  dropoff_datetime timestamp without time zone,
  store_and_fwd_flag char(1),
  rate_code_id integer,
  pickup_longitude numeric,
  pickup_latitude numeric,
  dropoff_longitude numeric,
  dropoff_latitude numeric,
  passenger_count integer,
  trip_distance numeric,
  fare_amount numeric,
  extra numeric,
  mta_tax numeric,
  tip_amount numeric,
  tolls_amount numeric,
  ehail_fee numeric,
  improvement_surcharge numeric,
  total_amount numeric,
  payment_type varchar,
  trip_type integer,
  pickup_nyct2010_gid integer,
  dropoff_nyct2010_gid integer
);

SELECT AddGeometryColumn('trips', 'pickup', 4326, 'POINT', 2);
SELECT AddGeometryColumn('trips', 'dropoff', 4326, 'POINT', 2);
