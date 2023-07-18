--ddl for reporting table
create table project_crime.rpt_layer.crime_num_mnth_rpt
(
	rpt_date				date,
	area_id 				varchar,
	area_name 				varchar,
	month 					varchar,
	crime_numbers 			number
	
)
;

create table project_crime.rpt_layer.crime_type_rpt
(
	rpt_date				date,
	area_id 				varchar,
	area_name 				varchar,
	month 					varchar,
	crime_type				varchar
	crime_numbers 			number
)
;
