-- Fact table loaded by using various street table

INSERT INTO project_crime.gold_layer.crime_data_fact_tbl
select 
crime_id,
month,
reported_by,
location,
lsoa_code,
crime_type,
last_outcome_category,
area_id,
event_date
from
(
select 
crime_id,
'101' as area_id,
month,
reported_by,
location,
lsoa_code,
crime_type,
last_outcome_category,
proc_date as event_date,
row_number() over(partition by crime_id,month,reported_by,falls_within,longitude,latitude,lsoa_code,lsoa_name,crime_type order by proc_date desc) as rownum
FROM project_crime.silver_layer.sy_strt_slvr_incr
WHERE proc_date = current_date()
) 
WHERE rownum=1
;

INSERT INTO crimesilver.crime_data_fact_tbl 
select 
crime_id,
month,
reported_by,
location,
lsoa_code,
crime_type,
last_outcome_category,
area_id,
event_date
from
(
select 
crime_id,
'102' as area_id,
month,
reported_by,
location,
lsoa_code,
crime_type,
last_outcome_category,
proc_date as event_date,
row_number() over(partition by crime_id,month,reported_by,falls_within,longitude,latitude,lsoa_code,lsoa_name,crime_type order by proc_date desc) as rownum
FROM project_crime.silver_layer.asc_strt_slvr_incr
WHERE proc_date = current_date()
) 
WHERE rownum=1
;

--dimention table loading

INSERT INTO project_crime.gold_layer.area_dim (area_id,area_name) values (101,'South Yorkshire'),(102,'Avon and Somerset');
