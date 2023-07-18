--Loading the data into bronze layer raw table from rwa layer raw table with adding column merge_flag
-- appending the data in bronze layer raw table

INSERT INTO project_crime.bronze_layer.asc_street_raw_br
select 
  crime_id,
  month,
  reported_by,
  falls_within,
  longitude,
  latitude,
  location,
  lsoa_code,
  lsoa_name,
  crime_type,
  last_outcome_category,
  context,
  False as merge_flag
from 
  project_crime.raw_layer.asc_street_raw
;

INSERT INTO project_crime.bronze_layer.sy_street_raw_br
select 
  crime_id,
  month,
  reported_by,
  falls_within,
  longitude,
  latitude,
  location,
  lsoa_code,
  lsoa_name,
  crime_type,
  last_outcome_category,
  context,
  False as merge_flag
from 
  project_crime.raw_layer.sy_street_raw
;

-- loading the into bronze layer table with following transformation
-- triming and replacing nulls in crime_id column with default value
-- replacing null values in last_outcome_category column with some default value
-- adding current_date as proc date 

INSERT OVERWRITE project_crime.bronze_layer.sy_street_br
select 
  coalesce(trim(crime_id),'unknown_crime_id') as crime_id,
  month,
  reported_by,
  falls_within,
  longitude,
  latitude,
  location,
  lsoa_code,
  lsoa_name,
  crime_type,
  coalesce(last_outcome_category,'unknown_state') as last_outcome_category,
  current_date() as proc_date
from 
  project_crime.bronze_layer.sy_street_raw_br
where merge_flag= False
;

INSERT OVERWRITE project_crime.bronze_layer.asc_street_br
select 
  coalesce(trim(crime_id),'unknown_crime_id') as crime_id,
  month,
  reported_by,
  falls_within,
  longitude,
  latitude,
  location,
  lsoa_code,
  lsoa_name,
  crime_type,
  coalesce(last_outcome_category,'unknown_state') as last_outcome_category,
  current_date() as proc_date
from 
  project_crime.bronze_layer.asc_street_raw_br
where merge_flag= False
;