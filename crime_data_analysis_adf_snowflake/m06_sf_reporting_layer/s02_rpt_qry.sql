--rpt query

INSERT INTO project_crime.rpt_layer.crime_num_mnth_rpt
select 
proc_date as rpt_date,
a.area_id,
b.area_name,
month,
count(a.crime_id) as crime_numbers 
from crimesilver.crime_data_fact_tbl a left join crimesilver.area_dim b 
on a.area_id = b.area_id
where proc_date=current_date()
group by 1,2,3,4
;

INSERT INTO project_crime.rpt_layer.crime_type_rpt
select 
proc_date as rpt_date, 
a.area_id,b.area_name,
month,
crime_type,
count(a.crime_id) as crime_numbers 
from crimesilver.crime_data_fact_tbl a left join crimesilver.area_dim b 
on a.area_id = b.area_id
where proc_date=current_date()
group by 1,2,3,4,5
;

---creating views
CREATE OR REPLACE VIEW project_crine.my_view.crime_num_mnth_rpt_vw
AS
select 
proc_date as rpt_date,
a.area_id,
b.area_name,
month,
count(a.crime_id) as crime_numbers 
from crimesilver.crime_data_fact_tbl a left join crimesilver.area_dim b 
on a.area_id = b.area_id
where proc_date=current_date()
group by 1,2,3,4
;
CREATE OR REPLACE VIEW project_crine.my_view.crime_type_rpt_vw
AS
select 
proc_date as rpt_date, 
a.area_id,b.area_name,
month,
crime_type,
count(a.crime_id) as crime_numbers 
from crimesilver.crime_data_fact_tbl a left join crimesilver.area_dim b 
on a.area_id = b.area_id
where proc_date=current_date()
group by 1,2,3,4,5
;
