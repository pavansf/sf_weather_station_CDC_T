use role accountadmin;

grant execute task on account to role sysadmin;
--revoke execute task on account from role sysadmin;

use role sysadmin;
show grants to role sysadmin;
use warehouse wh_load;
use database weather_db;
use schema weather_db.public;

alter task aggregate_final_data resume;
alter task extract_data resume;

show tasks;

alter warehouse wh_load set warehouse_size=large;

--insert some data to the raw_table (you can repeat the copy with others yrs json files in the year sub-folder)     ---  0000000
copy into raw_table from (select $1 from @weather_data_stage/2016/ (file_format => 'json_ff')); ---  4696471 -04.6m ---  4696471 
copy into raw_table from (select $1 from @weather_data_stage/2017/ (file_format => 'json_ff')); --- 20153665 -20.0m --- 24850136
copy into raw_table from (select $1 from @weather_data_stage/2018/ (file_format => 'json_ff')); --- 35852486 -35.8m --- 60702622
copy into raw_table from (select $1 from @weather_data_stage/2019/ (file_format => 'json_ff')); --- 42244424 -42.2m ---102947046
copy into raw_table from (select $1 from @weather_data_stage/2020/ (file_format => 'json_ff')); ---  8640365 -08.6m ---111587411(total_records)
                                                                                                
                                                                                               
--read the content of the newly created stream
select * from raw_data_stream limit 10;
select count(*) from raw_data_stream;

select * from transformed_data_stream;
select count(*) from transformed_data_stream;

select * from table(information_schema.task_history())
where  database_name = 'WEATHER_DB' and scheduled_time > dateadd(hour, -0.5, current_timestamp)
order by scheduled_time desc;

--select current_timestamp, dateadd(hour, -1, current_date());
--row count check in tables
select 
  (select count(*) from raw_table) as raw_Table_row_count,
  (select count(*) from transformed_table) as transform_Table_row_count,
  (select count(*) from final_table) as final_Table_row_count
  ;

-- suspend tasks and warehouses
alter task extract_data suspend;
alter task aggregate_final_data suspend;

show tasks;

alter warehouse wh_load suspend;
alter warehouse wh_transform suspend;

-- clean up
truncate table raw_table;
truncate table transformed_table;
truncate table final_table;

drop database weather_db;
drop warehouse wh_load;
drop warehouse wh_transform;
