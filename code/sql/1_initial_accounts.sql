use role accountadmin;

create or replace warehouse compute_wh WAREHOUSE_SIZE=medium INITIALLY_SUSPENDED=TRUE;

create database if not exists snowflake_sample_data from share sfc_samples.sample_data;
grant imported privileges on database snowflake_sample_data to public;

select current_user(), current_account(), current_region(), current_version();

use role orgadmin;

-- FILL IN YOUR CREDENTIALS: admin_password, email
create account horizon_lab_azure
  admin_name = horizonadmin
  admin_password = ''
  email = ''
  must_change_password = false
  edition = business_critical
  region = AZURE_WESTEUROPE;

-- FILL IN YOUR CREDENTIALS: admin_password, email
create account horizon_lab_aws_consumer
  admin_name = horizonadmin
  admin_password = ''
  email = ''
  must_change_password = false
  edition = business_critical
  region = AWS_US_WEST_2;

show organization accounts;


