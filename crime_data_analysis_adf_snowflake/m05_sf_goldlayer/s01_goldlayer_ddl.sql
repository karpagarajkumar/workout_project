--ddl for gold layer dim table and fact table

create or replace table project_crime.gold_layer.area_dim
(
	area_id 	Varchar,
	area_name 	Varchar
)
;

create or replace table project_crime.gold_layer.lsoa_dim
(
	lsoa_id 	Varchar,
	lsoa_name 	Varchar
)
;

create table project_crime.gold_layer.crime_data_fact_tbl
(
	crime_id 				Varchar,
	month 					Varchar,
	area_id 				Varchar
	reported_by 				Varchar,
	location 				Varchar,
	lsoa_code 				Varchar,
	crime_type 				Varchar,
	last_outcome_category 			Varchar
	event_date date
	cluster by (area_id,event_date)
);
