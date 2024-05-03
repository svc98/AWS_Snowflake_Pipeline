-- Create a new database
CREATE DATABASE WEATHER;

-- Switch to the newly created database
USE DATABASE WEATHER;

-- Create table to load CSV data
CREATE or replace TABLE weather_data(
    uv            NUMBER(20,0),
    temp          NUMBER(20,0),
    visibility    NUMBER(20,0),
    city          VARCHAR(128),
    humidity      NUMBER(20,0),
    precip_in     NUMBER(20,0),
    wind_speed    NUMBER(20,5),
    time          VARCHAR(128),
    wind_dir      VARCHAR(128),
    pressure_in   NUMBER(20,5)
);


-- Create integration object for external stage
create or replace storage integration s3_int
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::851725559797:role/Snowflake-Role'
  storage_allowed_locations = ('s3://weather-data1/snowflake/');


-- Describe integration object to fetch external_id and to be used in s3
DESC INTEGRATION s3_int;

create or replace file format csv_format
    type = csv
    field_delimiter = ','
    skip_header = 1
    null_if = ('NULL', 'null')
    empty_field_as_null = true;

-- Create External Stage linked to Integration Object
create or replace stage ext_weather_stage
  URL = 's3://weather-data1/snowflake/'
  STORAGE_INTEGRATION = s3_int
  file_format = csv_format;

-- Create Pipe to automate data ingestion from s3 to snowflake
create or replace pipe weather_pipe auto_ingest=true as
copy into weather_data
from @ext_weather_stage;

show pipes;



-- See the Weather Data
select * from weather_data;