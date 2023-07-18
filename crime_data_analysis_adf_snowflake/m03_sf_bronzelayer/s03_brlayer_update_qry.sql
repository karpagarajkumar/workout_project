---update statement bronzelayer raw table once processing done
---update mergeflag=True for mergeflag=False 

update project_crime.bronze_layer.asc_street_raw_br set merge_flag = True where merge_flag = False;

update project_crime.bronze_layer.sy_street_raw_br set merge_flag = True where merge_flag = False;