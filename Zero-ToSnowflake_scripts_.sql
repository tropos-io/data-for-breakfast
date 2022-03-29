
-- create a database called CITIBIKE
create or replace database CITIBIKE;

-- Set Worksheet Context database
Use DATABASE CITIBIKE;

-- create a table called TRIPS
create or replace table trips (
    tripduration integer,
    starttime timestamp,
    stoptime timestamp,
    start_station_id integer,
    start_station_name string,
    start_station_latitude float,
    start_station_longitude float,
    end_station_id integer,
    end_station_name string,
    end_station_latitude float,
    end_station_longitude float,
    bikeid integer,
    membership_type string,
    usertype string,
    birth_year integer,
    gender integer
);

--create a stage called citibike_trips in an external aws bucket
create stage citibike_trips
    url = 's3://snowflake-workshop-lab/citibike-trips/';
    --credentials = (aws_secret_key = '<key>' aws_key_id = '<id>');

-- take a look at the contents of the citibike_trips stage
list @citibike_trips;


--create file format
create or replace file format csv type='csv'
  compression = 'auto' field_delimiter = ',' record_delimiter = '\n'
  skip_header = 0 field_optionally_enclosed_by = '\042' trim_space = false
  error_on_column_count_mismatch = false escape = 'none' escape_unenclosed_field = '\134'
  date_format = 'auto' timestamp_format = 'auto' null_if = ('') comment = 'file format for ingesting data for zero to snowflake';
  
-- Verify file format is created
show file formats in database citibike;

-- Load the staged data into the table
copy into trips from @citibike_trips
file_format=CSV;

-- Truncate table Trips
truncate table trips;

--verify table is clear
select * from trips limit 10;

--change warehouse size from small to large (4x)
alter warehouse compute_wh set warehouse_size='large';

-- load data with large warehouse
show warehouses;

-- Truncate table Trips
copy into trips from @citibike_trips
file_format=CSV;

-- Creata Analytics Warehouse
create or replace WAREHOUSE "ANALYTICS_WH" WITH WAREHOUSE_SIZE = 'LARGE' AUTO_SUSPEND = 600 AUTO_RESUME = TRUE MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 1 SCALING_POLICY = 'STANDARD' COMMENT = 'Analytics Warehouse';

-- Set Warehouse context
use warehouse ANALYTICS_WH;

-- See a sample of the trips data
select * from trips limit 20;

-- Basic hourly statistics on Citi Bike usage
select date_trunc('hour', starttime) as "date",
    count(*) as "num trips",
    avg(tripduration)/60 as "avg duration (mins)",
    avg(haversine(start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude)) as "avg distance (km)"
from trips
group by 1 order by 1;

-- Let's see the result cache in action by running the exact same query again
select date_trunc('hour', starttime) as "date",
    count(*) as "num trips",
    avg(tripduration)/60 as "avg duration (mins)",
    avg(haversine(start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude)) as "avg distance (km)"
from trips
group by 1 order by 1;

-- Look at which months that are the busiest & Create a Bar Chart
select
monthname(starttime) as "month",
count(*) as "num trips"
from trips
group by 1 order by 2 desc;

-- Create a development (dev) table clone of the trips table
create table trips_dev clone trips;

-- create a database WEATHER to use for storing the semi-structured JSON data
create database weather;

-- Set Worksheet Context
use role sysadmin;
use warehouse compute_wh;
use database weather;
use schema public;

-- Create a table named JSON_WEATHER_DATA to use for loading the JSON data
create table json_weather_data (v variant);

-- Create a stage that points to the bucket
create stage nyc_weather
url = 's3://snowflake-workshop-lab/weather-nyc';

-- Take a look at the contents of the nyc_weather stage
list @nyc_weather;

-- Load and Verify the Semi-structured Data
copy into json_weather_data
from @nyc_weather
file_format = (type=json);

-- Take a look at the data that was loaded
select * from json_weather_data limit 10;

-- Create a columnar view of the semi-structured JSON weather data
create view json_weather_data_view as
select
v:time::timestamp as observation_time,
v:city.id::int as city_id,
v:city.name::string as city_name,
v:city.country::string as country,
v:city.coord.lat::float as city_lat,
v:city.coord.lon::float as city_lon,
v:clouds.all::int as clouds,
(v:main.temp::float)-273.15 as temp_avg,
(v:main.temp_min::float)-273.15 as temp_min,
(v:main.temp_max::float)-273.15 as temp_max,
v:weather[0].main::string as weather,
v:weather[0].description::string as weather_desc,
v:weather[0].icon::string as weather_icon,
v:wind.deg::float as wind_dir,
v:wind.speed::float as wind_speed
from json_weather_data
where city_id = 5128638;

-- Verify the view with the following query
select * from json_weather_data_view
where date_trunc('month',observation_time) = '2018-01-01'
limit 20;

-- Join WEATHER to TRIPS and count the number of trips associated with certain weather conditions
select weather as conditions
,count(*) as num_trips
from citibike.public.trips
left outer join json_weather_data_view
on date_trunc('hour', observation_time) = date_trunc('hour', starttime)
where conditions is not null
group by 1 order by 2 desc;

-- DROP command to remove the JSON_WEATHER_DATA table
drop table json_weather_data;

-- Run a query on the table
select * from json_weather_data limit 10;

--  Restore the table
undrop table json_weather_data;

--verify table is undropped
select * from json_weather_data_view limit 10;

--Set the Worksheet Context
use role sysadmin;
use warehouse compute_wh;
use database citibike;
use schema public;

-- Replace all of the station names in the table with the word "oops"
update trips set start_station_name = 'oops';

-- Run a query that returns the top 20 stations by number of rides
select
start_station_name as "station",
count(*) as "rides"
from trips
group by 1
order by 2 desc
limit 20;

-- Find the query ID of the last UPDATE command and store it in a variable named $QUERY_ID
set query_id =
(select query_id from table(information_schema.query_history_by_session (result_limit=>5))
where query_text like 'update%' order by start_time limit 1);

-- Time Travel to recreate the table with the correct station names
create or replace table trips as
(select * from trips before (statement => $query_id));

-- Verify that the station names have been restored
select
start_station_name as "station",
count(*) as "rides"
from trips
group by 1
order by 2 desc
limit 20;

-- Change Role to AccountAdmin
use role accountadmin; 

-- (NOTE - enter your unique user name into the second row below)

create or replace role junior_dba;
grant role junior_dba to user <USER_NAME>;--YOUR_USER_NAME_GOES HERE;

-- Change Role to junior_dba

use role junior_dba;

-- -- Change Role to accountadmin and grant usage

use role accountadmin;
grant usage on database citibike to role junior_dba;
grant usage on database weather to role junior_dba;

-- Verify access to the objects for the role junior_dba

use role junior_dba;