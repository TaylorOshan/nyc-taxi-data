#!/bin/bash

#sudo -u postgres createdb -O toshan nyc_taxi_data

#psql nyc_taxi_data -c "CREATE TABLESPACE taxiDB LOCATION '/media/storage/taxis_tablespace';"
#psql -U nyc_taxi_data -c "ALTER DATABASE nyc_taxi_data SET TABLESPACE taxiDB;"

psql nyc_taxi_data -f create_nyc_taxi_schema.sql

shp2pgsql -s 2263:4326 nyct2010_15b/nyct2010.shp | psql -d nyc_taxi_data
psql nyc_taxi_data -c "CREATE INDEX index_nyct_on_geom ON nyct2010 USING gist (geom);"
psql nyc_taxi_data -c "CREATE INDEX index_nyct_on_ntacode ON nyct2010 (ntacode);"
psql nyc_taxi_data -c "VACUUM ANALYZE nyct2010;"

shp2pgsql -s 2263:4326 nycb2010_16c/nycb2010.shp | psql -d nyc_taxi_data
psql nyc_taxi_data -c "CREATE INDEX index_nycb_on_geom ON nycb2010 USING gist (geom);"
psql nyc_taxi_data -c "CREATE INDEX index_nycb_on_BCTCB ON nycb2010 (BCTCB2010);"
psql nyc_taxi_data -c "VACUUM ANALYZE nycb2010;"

shp2pgsql -s 4269:4326 nycb2010_16c/nycbg2010.shp | psql -d nyc_taxi_data
psql nyc_taxi_data -c "CREATE INDEX index_nycbg_on_geom ON nycbg2010 USING gist (geom);"
psql nyc_taxi_data -c "CREATE INDEX index_nycbg_on_geoid ON nycbg2010 (geo_id);"
psql nyc_taxi_data -c "VACUUM ANALYZE nycbg2010;"

shp2pgsql -s 2263:4326 taxi_zones/taxi_zones.shp | psql -d nyc_taxi_data
psql nyc_taxi_data -c "CREATE INDEX index_taxi_zones_on_geom ON taxi_zones USING gist (geom);"
psql nyc_taxi_data -c "CREATE INDEX index_taxi_zones_on_locationid ON taxi_zones (locationid);"
psql nyc_taxi_data -c "VACUUM ANALYZE taxi_zones;"

