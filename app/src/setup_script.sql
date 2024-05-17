/***************************************************************************************************
This SETUP script runs when the Consumer installs or upgrades a Snowflake Native Application
***************************************************************************************************/

-- Create Application Role and Schema to provide Consumer visibility
create application role if not exists app_instance_role;
create or alter versioned schema app_instance_schema;
grant usage on schema app_instance_schema to application role app_instance_role;

-- Create proxy view on shared content to make visible to consumer
CREATE OR REPLACE VIEW app_instance_schema.orders_v 
    AS SELECT * FROM shared_content_schema.orders_v;

-- Create Row-Access Policy to limit access by Consumer region using a local mapping table
CREATE OR REPLACE TABLE app_instance_schema.region_country_map(region STRING, country STRING);

INSERT INTO app_instance_schema.region_country_map
    VALUES  ('PUBLIC.AZURE_WESTEUROPE','Germany'),
            ('PUBLIC.AZURE_WESTEUROPE','Spain'),
            ('PUBLIC.AZURE_WESTEUROPE','England'),
            ('PUBLIC.AZURE_WESTEUROPE','Sweden'),
            ('PUBLIC.AZURE_WESTEUROPE','Poland'),
            ('PUBLIC.AZURE_WESTEUROPE','France'),
            ('PUBLIC.AZURE_WESTEUROPE','Egypt'),
            ('PUBLIC.AZURE_WESTEUROPE','South Africa'),
            ('PUBLIC.AWS_US_WEST_2','United States'),
            ('PUBLIC.AWS_US_WEST_2','Brazil'),
            ('PUBLIC.AWS_US_WEST_2','Canada'),
            ('PUBLIC.AWS_AP_SOUTHEAST_1','South Korea'),
            ('PUBLIC.AWS_AP_SOUTHEAST_1','India'),
            ('PUBLIC.AWS_AP_SOUTHEAST_1','Japan'),
            ('PUBLIC.AWS_AP_SOUTHEAST_1','Australia');

CREATE OR REPLACE ROW ACCESS POLICY app_instance_schema.country_row_policy
    AS (country STRING) RETURNS BOOLEAN -> 
    country = 'Canada' 
    OR current_role() IN ('ACCOUNTADMIN','SYSADMIN','SALES_MANAGER_ROLE')
    OR EXISTS 
    (
        SELECT 1
        FROM app_instance_schema.region_country_map map
        WHERE 1=1
        AND map.region = current_region()
        AND map.country = country
    )
COMMENT = 'Policy to limit rows returned based on region';

ALTER VIEW app_instance_schema.orders_v
  ADD ROW ACCESS POLICY app_instance_schema.country_row_policy ON (country);

-- Create Streamlit app
create or replace streamlit app_instance_schema.sales_streamlit 
   from '/libraries' main_file='/frosty_bytes_sis.py';

-- Create UDFs
create or replace function app_instance_schema.hello_world()
returns string
language python
runtime_version = '3.8'
packages = ('snowflake-snowpark-python')
imports = ('/libraries/udf.py')
handler = 'udf.hello_world';

create or replace function app_instance_schema.calc_distance(slat float,slon float,elat float,elon float)
returns float
language python
runtime_version = '3.8'
packages = ('snowflake-snowpark-python','pandas','scikit-learn==1.1.1')
imports = ('/libraries/udf.py')
handler = 'udf.calc_distance';

-- Create Stored Procedure
create or replace procedure app_instance_schema.billing_event(number_of_rows int)
returns string
language python
runtime_version = '3.8'
packages = ('snowflake-snowpark-python')
imports = ('/libraries/procs.py')
handler = 'procs.billing_event';

-- Grant usage and permissions on objects
grant usage on procedure app_instance_schema.billing_event(int) to application role app_instance_role;
grant usage on function app_instance_schema.calc_distance(float,float,float,float) to application role app_instance_role;
grant usage on function app_instance_schema.hello_world() to application role app_instance_role;
grant SELECT on view app_instance_schema.orders_v to application role app_instance_role;
grant usage on streamlit app_instance_schema.sales_streamlit to application role app_instance_role;