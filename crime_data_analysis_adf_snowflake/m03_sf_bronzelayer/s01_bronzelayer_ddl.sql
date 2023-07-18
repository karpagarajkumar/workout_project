--ddl statement for bronze table
create or replace table project_crime.bronze_layer.asc_street_raw_br
(
	crime_id 				varchar,	
	month 					varchar,	
	reported_by				varchar,
	falls_within 			varchar,	
	longitude 				varchar,	
	latitude 				varchar,	
	location 				varchar,	
	lsoa_code 				varchar,	
	laoa_name 				varchar,	
	crime_type 				varchar,	
	last_outcome_category 	varchar,	
	context 				varchar,
	merge_flag 				boolean
)
;

create or replace table project_crime.bronze_layer.sy_street_raw_br
(
	crime_id 				varchar,	
	month 					varchar,	
	reported_by				varchar,
	falls_within 			varchar,	
	longitude 				varchar,	
	latitude 				varchar,	
	location 				varchar,	
	lsoa_code 				varchar,	
	laoa_name 				varchar,	
	crime_type 				varchar,	
	last_outcome_category 	varchar,	
	context 				varchar,
	merge_flag 				boolean
)
;

create or replace table project_crime.bronze_layer.asc_street_br
(
	crime_id 				varchar,	
	month 					varchar,	
	reported_by				varchar,
	falls_within 			varchar,	
	longitude 				varchar,	
	latitude 				varchar,	
	location 				varchar,	
	lsoa_code 				varchar,	
	laoa_name 				varchar,	
	crime_type 				varchar,	
	last_outcome_category 	varchar,	
	proc_date 				date
)
;

create or replace table project_crime.bronze_layer.sy_street_br
(
	crime_id 				varchar,	
	month 					varchar,	
	reported_by				varchar,
	falls_within 			varchar,	
	longitude 				varchar,	
	latitude 				varchar,	
	location 				varchar,	
	lsoa_code 				varchar,	
	laoa_name 				varchar,	
	crime_type 				varchar,	
	last_outcome_category 	varchar,	
	proc_date 				date
)
;




