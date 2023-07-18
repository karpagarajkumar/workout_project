--inserting data into hist table in silver layer

INSERT INTO project_crime.silver_layer.sy_strt_slvr_hist  
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
  proc_date
from (
select crime_id,
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
  proc_date,
  row_number() over (partition by crime_id,month,reported_by,falls_within,longitude,latitude,lsoa_code,lsoa_name,crime_type order by proc_date desc) as rownum
from 
  project_crime.bronze_layer.sy_street_br
where merge_flag= False and proc_date=current_date()
) 
where rownum=1
;

INSERT INTO project_crime.silver_layer.asc_strt_slvr_hist  
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
  proc_date
from (
select crime_id,
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
  proc_date,
  row_number() over (partition by crime_id,month,reported_by,falls_within,longitude,latitude,lsoa_code,lsoa_name,crime_type order by proc_date desc) as rownum
from 
  project_crime.bronze_layer.asc_street_br
where merge_flag= False and proc_date=current_date()
) 
where rownum=1