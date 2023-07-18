--ddl statement for raw table
create or replace table project_crime.raw_layer.asc_street_raw
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
	context 				varchar
)
;

create or replace table project_crime.raw_layer.sy_street_raw
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
	context 				varchar
)
;




