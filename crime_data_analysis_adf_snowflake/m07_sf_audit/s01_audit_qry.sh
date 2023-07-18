#count check b/w source and destination
#!bin/bash

sy_src_chk= `snowsql -c connection -q 'select count(*) from project_crime.silver_layer.sy_strt_slvr_incr where proc_date=current_date()'`

asc_src_chk= `snowsql -c connection -q 'select count(*) from project_crime.silver_layer.asc_strt_slvr_incr where proc_date=current_date()'`

sy_dest_chk= `snowsql -c connection -q 'select count(*) from project_crime.gold_layer.crime_data_fact_tbl where area_id='101' and proc_date=current_date()'`

asc_dest_chk= `snowsql -c connection -q 'select count(*) from project_crime.gold_layer.crime_data_fact_tbl where area_id='102' and proc_date=current_date()'`

echo "sy_src_chk: $ sy_src_chk"
echo "asc_src_chk: $ asc_src_chk"
echo "sy_dest_chk: $ sy_dest_chk"
echo "asc_dest_chk: $ asc_dest_chk"

if [[ $sy_src_chk == $sy_dest_chk && $asc_src_chk == $asc_dest_chk ]]; then
	echo "counts source and destination is matching"
	exit 0;
else
	echo "counts source and destination is not matching"
	echo " sy_src_chk: $ sy_src_chk and asc_src_chk: $ asc_src_chk and sy_dest_chk: $ sy_dest_chk and asc_dest_chk: $ asc_dest_chk"
	exit 1;
fi
