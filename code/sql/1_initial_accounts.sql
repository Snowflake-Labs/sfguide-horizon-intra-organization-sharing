use role accountadmin;

create or replace warehouse compute_wh WAREHOUSE_SIZE=medium INITIALLY_SUSPENDED=TRUE;

create database if not exists snowflake_sample_data from share sfc_samples.sample_data;
grant imported privileges on database snowflake_sample_data to public;

select current_user(), current_account(), current_region(), current_version();

use role orgadmin;

create account horizon_lab_azure
  admin_name = horizonadmin
  admin_password = 'Summit2024!'
  email = 'vinay.srihari@snowflake.com'
  must_change_password = false
  edition = business_critical
  region = AZURE_WESTEUROPE;

create account horizon_lab_aws_consumer
  admin_name = horizonadmin
  admin_password = 'Summit2024!'
  email = 'vinay.srihari@snowflake.com'
  must_change_password = false
  edition = business_critical
  region = AWS_US_WEST_2;

show organization accounts;


