/*----------------------
  context setup
  ----------------------*/
use role sysadmin;

//wh_load warehouse to load data from stage to raw_table
create or replace warehouse wh_load 
	with warehouse_size = 'medium' auto_suspend = 10 initially_suspended = true;

--wh_transform warehouse to insert, transform delta records from raw_table.
create or replace warehouse wh_transform 
	with warehouse_size = 'xsmall' auto_suspend = 25 initially_suspended = true;
        
use warehouse wh_load;

create or replace database weather_db;
use database weather_db;
use schema weather_db.public;


create or replace table raw_table (v variant);
create or replace transient table transformed_table (
  date timestamp_ntz,
  country string,
  city string,
  position_lat float,
  position_long float,
  id string,
  temp_kel float,
  temp_min_kel float,
  temp_max_kel float,
  conditions string,
  wind_dir float,
  wind_speed float
);


desc table transformed_table;

create or replace table final_table (
  date timestamp_ntz,
  country string,
  city string,
  position_lat float,
  position_long float,
  id string,
  temp_cel float,
  temp_min_cel float,
  temp_max_cel float,
  conditions string,
  wind_dir float,
  wind_speed float
);
show tables like '%_table';

create or replace file format json_ff type = 'json' compression = 'gzip';
desc file format json_ff;

create or replace stage weather_data_stage
    url = "s3://snowflake-workshop-lab/aws-sa-enable/weather/";

list @weather_data_stage/;

select $1 from @weather_data_stage/2016/ (file_format => 'json_ff') limit 10;


-- create the stream to monitor data changes against raw table
create or replace stream raw_data_stream on table raw_table;

-- create the stream to monitor data changes against transformed table
create or replace stream transformed_data_stream on table transformed_table;

-- list the streams
show streams; 

-- read the content of the newly created stream (should be empty as no data has been inserted yet)
select count(*) from raw_data_stream;
select count(*) from transformed_data_stream;

-- create a stored procedure that will read the content of raw_data_stream and extract inserted json data to transformed_table
create or replace procedure stored_proc_extract_json()
    returns string
    language javascript
    strict
    execute as owner
    as
    $$
    var sql_cmd =`insert into transformed_table (date,country,city,position_lat,position_long,id,temp_kel,temp_min_kel,temp_max_kel,conditions,wind_dir,wind_speed)
                    select        
                      v:time::timestamp_ntz  date,        
                      v:city.country::string country,       
                      v:city.name::string city,		     
                      v:city.coord.lat::float position_lat,		
                      v:city.coord.lon::float position_long,        
                      v:city.id::string id,        
                      v:main.temp::float temp_kel,        
                      v:main.temp_min::float temp_min_kel,         
                      v:main.temp_max::float temp_max_kel,        
                      v:weather[0].main::string conditions,        
                      v:wind.deg::float wind_dir,        
                      v:wind.speed::float wind_speed    
                    from raw_data_stream    
                    where metadata$action = 'INSERT'`; 
        
      try {
        snowflake.execute ({sqlText: sql_cmd});
        return "JSON extracted.";   // Return a success indicator.
          }
      catch (err){
          return "Failed: " + err;   // Return a error indicator.
          }
    $$
    ;
--call stored_proc_extract_json();

-- create a stored procedure that will read the content of the transformed_data_stream data and transform inserted data to aggregated_final_table
create or replace procedure stored_proc_aggregate_final()
    returns string
    language javascript
    strict
    execute as owner
    as
    $$
    var sql_cmd = `insert into final_table (date,country,city,position_lat, position_long,id,temp_cel,temp_min_cel,temp_max_cel,conditions,wind_dir,wind_speed)
                     select
                         date,
                         country,
                         city,
                         position_lat,
                         position_long,
                         id,
                         temp_kel-273.15 temp_cel,
                         temp_min_kel-273.15 temp_min_cel,
                         temp_max_kel-273.15 temp_max_cel,
                         conditions,
                         wind_dir,
                         wind_speed
                     from transformed_data_stream
                     where metadata$action = 'INSERT'`
    try {
        snowflake.execute ({sqlText: sql_cmd});
        return "transformed json - aggregated.";   // return a success indicator.
        }
    catch (err) {
        return "failed: " + err;   // return a error indicator.
        }
    $$
    ;

--call stored_proc_aggregate_final();

select system$stream_has_data('raw_data_stream');
select system$stream_has_data('transformed_data_stream');

-- create a task to look for newly inserted raw data every 1 minute
create or replace task extract_data 
    warehouse = wh_transform 
    schedule = '1 minute' 
    when system$stream_has_data('raw_data_stream') 
    as call stored_proc_extract_json();

-- create a sub-task to run after raw data has been extracted (run after)
create or replace task aggregate_final_data 
    warehouse = wh_transform 
    after extract_data 
    when system$stream_has_data('transformed_data_stream') 
    as call stored_proc_aggregate_final();

show tasks;

alter task aggregate_final_data resume;
alter task extract_data resume;

show grants to role sysadmin;
