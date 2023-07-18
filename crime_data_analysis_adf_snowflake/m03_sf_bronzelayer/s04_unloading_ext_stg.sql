
---create storage Integration
CREATE OR REPLACE STORAGE INTEGRATION BLOB_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = Azure
  ENABLED = TRUE 
  AZURE_TENANT_ID = 'KEY'
  STORAGE_ALLOWED_LOCATIONS = ('path/asc_street/', 'path/sy_street/')
  COMMENT = 'Integration with Azure blob storage' ;
  
  
---Create file format object
CREATE OR REPLACE FILE FORMAT Project_crime.stage_schema.csv_fileformat
    type = csv
    field_delimiter = '|'
    skip_header = 1
    empty_field_as_null = TRUE;

---Create stage object with integration object & file format object
--- Using the Storeage Integration object that was already created

CREATE OR REPLACE STAGE Project_crime.stage_schema.blob_asc_street
    URL = 'path/asc_street/'
    STORAGE_INTEGRATION = BLOB_INT
    FILE_FORMAT = Project_crime.stage_schema.csv_fileformat ;
	
CREATE OR REPLACE STAGE Project_crime.stage_schema.blob_sy_street
    URL = 'path/sy_street/'
    STORAGE_INTEGRATION = BLOB_INT
    FILE_FORMAT = Project_crime.stage_schema.csv_fileformat ;

---Generate files and store them in the stage location
COPY INTO @Project_crime.stage_schema.blob_asc_street/asc_street_current_date()
FROM project_crime.raw_layer.asc_street_raw
SINGLE = TRUE;

COPY INTO @Project_crime.stage_schema.blob_sy_street/sy_street_current_date()
FROM project_crime.raw_layer.sy_street_raw
SINGLE = TRUE;