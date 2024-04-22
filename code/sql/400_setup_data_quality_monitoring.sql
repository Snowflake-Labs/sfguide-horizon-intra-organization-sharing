/***************************************************************************************************
  _______           _            ____          _
 |__   __|         | |          |  _ \        | |
    | |  __ _  ___ | |_  _   _  | |_) | _   _ | |_  ___  ___
    | | / _` |/ __|| __|| | | | |  _ < | | | || __|/ _ \/ __|
    | || (_| |\__ \| |_ | |_| | | |_) || |_| || |_|  __/\__ \
    |_| \__,_||___/ \__| \__, | |____/  \__, | \__|\___||___/
                          __/ |          __/ |
                         |___/          |___/

Demo:         Tasty Bytes
Version:      DataOps v2
Vignette:     Governance with Snowflake Horizon
Create Date:  2023-01-13
Author:       Jacob Kranzler
Copyright(c): 2024 Snowflake Inc. All rights reserved.
****************************************************************************************************
  Audit Your Data
    1 - Data Quality Monitoring

 Discovery with Snowflake Horizon
    2 - Universal Search
****************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ------------------------------------------------------------
2023-01-13          Jacob Kranzler      Initial Data Governance Release
2024-02-01          Charlie Hammond     Initial DataOps Release
2024-04-03          Jacob Kranzler      DataOps Zero to Snowflake Refresh | V2
***************************************************************************************************/

/*----------------------------------------------------------------------------------
Step 1 - Data Quality Monitoring 

 Within Snowflake, you can measure the quality of your data by using Data Metric
 Functions. Using these, we want to ensure that there are not duplicate or invalid
 Customer Email Addresses present in our system. While our team works to resolve any
 existing bad records, we will work to monitor these occuring moving forward.

 Within this step, we will walk through adding Data Metric Functions to our Customer
 Loyalty Table to capture Duplicate and Invalid Email Address counts everytime
 data is updated.
----------------------------------------------------------------------------------*/

USE ROLE accountadmin;
USE DATABASE frostbyte_tasty_bytes;
USE WAREHOUSE tasty_dev_wh;

-- let's first use the the Snowflake-provided Duplicate Count Data Metric Function (DMF)
-- to see if E-mail duplicates exist in our Customer Loyalty Table
SELECT snowflake.core.duplicate_count (SELECT e_mail FROM frostbyte_tasty_bytes.raw_customer.customer_loyalty);
SELECT snowflake.core.unique_count (SELECT e_mail FROM frostbyte_tasty_bytes.raw_customer.customer_loyalty);

-- to accompany the Duplicate Count DMF, let's also create a Custom Data Metric Function
-- that uses Regular Expression (RegEx) to Count Invalid Email Addresses
CREATE OR REPLACE DATA METRIC FUNCTION governance.invalid_email_count(iec TABLE(iec_c1 STRING))
RETURNS NUMBER 
    AS
'SELECT COUNT_IF(FALSE = (iec_c1 regexp ''^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,4}$'')) FROM iec';


-- for demo purposes, let's grant this to everyone
GRANT ALL ON FUNCTION governance.invalid_email_count(TABLE(STRING)) TO ROLE public;


-- as we did above, let's see how many Invalid Email Addresses currently exist
SELECT governance.invalid_email_count(SELECT e_mail FROM raw_customer.customer_loyalty) AS invalid_email_count;


-- before we can apply our DMF's to the table, we must first set the Data Metric Schedule. for our
-- testing we will Trigger this to run every time the table is modified
ALTER TABLE raw_customer.customer_loyalty
    SET data_metric_schedule = 'TRIGGER_ON_CHANGES';

    /**
      Data Metric Schedule specifies the schedule for running Data Metric Functions
      for tables and can leverage MINUTE, USING CRON or TRIGGER_ON_CHANGES
     **/

-- add the Duplicate Count Data Metric Function (DMF)
ALTER TABLE raw_customer.customer_loyalty 
    ADD DATA METRIC FUNCTION snowflake.core.duplicate_count ON (e_mail);


-- add our Invalid Email Count Data Metric Function (DMF)
ALTER TABLE raw_customer.customer_loyalty 
    ADD DATA METRIC FUNCTION governance.invalid_email_count ON (e_mail);


-- to test our work so far, let's insert 6 records with duplicate and invalid e-mail addresses
INSERT INTO raw_customer.customer_loyalty (customer_id, e_mail) VALUES
    (0000001, 'invalidemail@com'), (0000002, 'invalidemail@com') , (0000003, 'invalidemail@com'),
    (0000004, 'invalidemaildotcom') , (0000005, 'invalidemaildotcom') , (0000006, 'invalidemaildotcom');


-- before moving on, let's validate Trigger on Changes Schedule is in place
SHOW PARAMETERS LIKE 'DATA_METRIC_SCHEDULE' IN TABLE raw_customer.customer_loyalty;


-- let's also confirm the schedule has been Started
SELECT 
    metric_name,
    ref_entity_schema_name,
    ref_entity_name,
    schedule,
    schedule_status  
FROM TABLE(information_schema.data_metric_function_references
(
    ref_entity_name => 'frostbyte_tasty_bytes.raw_customer.customer_loyalty',
    ref_entity_domain => 'table')
);


-- the results our Data Metric Functions are written to an Event table, let's start by taking a look at the Raw output
    -- Note: Latency can be up to a few minutes. If the queries below are empty please wait a few minutes.
SELECT * FROM snowflake.local.data_quality_monitoring_results_raw;


-- for ease of use, a flattened View is also provided so let's take a look at this as well
SELECT 
    change_commit_time,
    measurement_time,
    table_schema,
    table_name,
    metric_name,
    value
FROM snowflake.local.data_quality_monitoring_results
WHERE lower(table_database) = 'frostbyte_tasty_bytes'
ORDER BY change_commit_time DESC;


