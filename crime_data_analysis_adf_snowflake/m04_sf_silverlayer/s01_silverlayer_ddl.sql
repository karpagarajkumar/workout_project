--ddl statement for silver layer tables

create or replace table project_crime.silver_layer.asc_strt_slvr_hist
(
	crime_id 				varchar,	
	month 					varchar,	
	reported_by				varchar,
	falls_within 				varchar,	
	longitude 				varchar,	
	latitude 				varchar,	
	location 				varchar,	
	lsoa_code 				varchar,	
	laoa_name 				varchar,	
	crime_type 				varchar,	
	last_outcome_category 			varchar,	
	proc_date 				date,
	cluster by(month)
)
;

create or replace table project_crime.silver_layer.sy_strt_slvr_hist
(
	crime_id 				varchar,	
	month 					varchar,	
	reported_by				varchar,
	falls_within 				varchar,	
	longitude 				varchar,	
	latitude 				varchar,	
	location 				varchar,	
	lsoa_code 				varchar,	
	laoa_name 				varchar,	
	crime_type 				varchar,	
	last_outcome_category 	string,	
	proc_date 				date
	cluster by(month)
)
;

create or replace table project_crime.silver_layer.asc_strt_slvr_incr
(
	crime_id 				varchar,	
	month 					varchar,	
	reported_by				varchar,
	falls_within 				varchar,	
	longitude 				varchar,	
	latitude 				varchar,	
	location 				varchar,	
	lsoa_code 				varchar,	
	laoa_name 				varchar,	
	crime_type 				varchar,	
	last_outcome_category 			varchar,	
	proc_date 				date
)
;

create or replace table project_crime.silver_layer.sy_strt_slvr_incr
(
	crime_id 				varchar,	
	month 					varchar,	
	reported_by				varchar,
	falls_within 				varchar,	
	longitude 				varchar,	
	latitude 				varchar,	
	location 				varchar,	
	lsoa_code 				varchar,	
	laoa_name 				varchar,	
	crime_type 				varchar,	
	last_outcome_category 			varchar,	
	proc_date 				date
)
;
