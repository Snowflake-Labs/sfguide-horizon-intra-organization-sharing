USE ROLE ACCOUNTADMIN;
GRANT CREATE EXTERNAL VOLUME ON ACCOUNT TO SYSADMIN;

USE ROLE SYSADMIN;

/*
Before you create an Iceberg table, you must have an external volume. 
An external volume is a Snowflake object that stores information about your cloud storage locations
and identity and access management (IAM) entities. Snowflake uses an external volume to establish 
a connection with your cloud storage in order to access Iceberg metadata and Parquet table data.

You will first need to configure an external volume for your cloud service provider:

AWS S3: https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-external-volume-s3
Azure Storage: https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-external-volume-azure
Google GCS: https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-external-volume-gcs
*/

CREATE OR REPLACE EXTERNAL VOLUME iceberg_external_volume
   STORAGE_LOCATIONS =
      (
         (
            NAME = 'xxxxxxxx-s3-us-west-2'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://horizon-iceberg/'
            STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::xxxxxxxxxxxx:role/horizon_iceberg_role'
            STORAGE_AWS_EXTERNAL_ID = 'horizon_iceberg_external_id'
         )
      );

DESCRIBE EXTERNAL VOLUME iceberg_external_volume; 

/*
Record the value for the STORAGE_AWS_IAM_USER_ARN property, which is the AWS IAM user created for this account.
For example: STORAGE_AWS_IAM_USER_ARN":"arn:aws:iam::533267078010:user/rzzl0000-s

Edit the IAM Role Trust Policy with this value.
On AWS: https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-external-volume-s3#step-6-grant-the-iam-user-permissions-to-access-bucket-objects)
*/

CREATE OR REPLACE DATABASE frostbyte_iceberg;
CREATE OR REPLACE SCHEMA frostbyte_iceberg.analytics;
CREATE OR REPLACE SCHEMA frostbyte_iceberg.raw_pos;
CREATE OR REPLACE SCHEMA frostbyte_iceberg.raw_customer;
CREATE OR REPLACE SCHEMA frostbyte_iceberg.tpch;
CREATE OR REPLACE SCHEMA frostbyte_iceberg.governance;

USE ROLE securityadmin;

GRANT USAGE ON DATABASE frostbyte_iceberg TO ROLE tasty_admin;
GRANT USAGE ON DATABASE frostbyte_iceberg TO ROLE tasty_data_engineer;
GRANT USAGE ON DATABASE frostbyte_iceberg TO ROLE tasty_data_scientist;

GRANT USAGE ON ALL SCHEMAS IN DATABASE frostbyte_iceberg TO ROLE tasty_admin;
GRANT USAGE ON ALL SCHEMAS IN DATABASE frostbyte_iceberg TO ROLE tasty_data_engineer;
GRANT USAGE ON ALL SCHEMAS IN DATABASE frostbyte_iceberg TO ROLE tasty_data_scientist;

GRANT ALL ON FUTURE ICEBERG TABLES IN SCHEMA frostbyte_iceberg.raw_pos TO ROLE tasty_admin;
GRANT ALL ON FUTURE ICEBERG TABLES IN SCHEMA frostbyte_iceberg.raw_pos TO ROLE tasty_data_engineer;
GRANT ALL ON FUTURE ICEBERG TABLES IN SCHEMA frostbyte_iceberg.raw_pos TO ROLE tasty_data_scientist;

GRANT ALL ON FUTURE ICEBERG TABLES IN SCHEMA frostbyte_iceberg.raw_customer TO ROLE tasty_admin;
GRANT ALL ON FUTURE ICEBERG TABLES IN SCHEMA frostbyte_iceberg.raw_customer TO ROLE tasty_data_engineer;
GRANT ALL ON FUTURE ICEBERG TABLES IN SCHEMA frostbyte_iceberg.raw_customer TO ROLE tasty_data_scientist;

GRANT ALL ON FUTURE ICEBERG TABLES IN SCHEMA frostbyte_iceberg.tpch TO ROLE tasty_admin;
GRANT ALL ON FUTURE ICEBERG TABLES IN SCHEMA frostbyte_iceberg.tpch TO ROLE tasty_data_engineer;
GRANT ALL ON FUTURE ICEBERG TABLES IN SCHEMA frostbyte_iceberg.tpch TO ROLE tasty_data_scientist;

GRANT ALL ON FUTURE VIEWS IN SCHEMA frostbyte_iceberg.analytics TO ROLE tasty_admin;
GRANT ALL ON FUTURE VIEWS IN SCHEMA frostbyte_iceberg.analytics TO ROLE tasty_data_engineer;
GRANT ALL ON FUTURE VIEWS IN SCHEMA frostbyte_iceberg.analytics TO ROLE tasty_data_scientist;

USE ROLE sysadmin;
USE WAREHOUSE DEMO_BUILD_WH;
USE DATABASE frostbyte_iceberg;

CREATE OR REPLACE ICEBERG TABLE frostbyte_iceberg.tpch.customer_tpch_iceberg
    CATALOG='SNOWFLAKE',
    EXTERNAL_VOLUME='iceberg_external_volume',
    BASE_LOCATION='tpch'
    COMMENT = 'Customer table from the TPCH 100GB dataset'
    AS
    SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.CUSTOMER;
-- 15M rows
SELECT COUNT(*) FROM frostbyte_iceberg.tpch.customer_tpch_iceberg;

CREATE OR REPLACE ICEBERG TABLE frostbyte_iceberg.tpch.nation_tpch_iceberg
    CATALOG='SNOWFLAKE',
    EXTERNAL_VOLUME='iceberg_external_volume',
    BASE_LOCATION='tpch'
    COMMENT = 'Nation table from the TPCH 100GB dataset'
    AS
    SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.NATION;
--25 rows
SELECT COUNT(*) FROM frostbyte_iceberg.tpch.nation_tpch_iceberg;

CREATE OR REPLACE ICEBERG TABLE frostbyte_iceberg.raw_customer.customer_loyalty_iceberg 
    CATALOG='SNOWFLAKE',
    EXTERNAL_VOLUME='iceberg_external_volume',
    BASE_LOCATION='tastybytes'
    COMMENT = 'Customer Loyalty table from the RAW layer'
    AS
    SELECT * FROM FROSTBYTE_TASTY_BYTES.RAW_CUSTOMER.CUSTOMER_LOYALTY;
-- 222K rows
SELECT COUNT(*) FROM frostbyte_iceberg.raw_customer.customer_loyalty_iceberg;

CREATE OR REPLACE ICEBERG TABLE frostbyte_iceberg.raw_pos.order_header_iceberg 
    CATALOG='SNOWFLAKE',
    EXTERNAL_VOLUME='iceberg_external_volume',
    BASE_LOCATION='tastybytes'
    COMMENT = 'Order Header table from the RAW layer'
    AS
    SELECT * FROM FROSTBYTE_TASTY_BYTES.RAW_POS.ORDER_HEADER;
-- 248M rows
SELECT COUNT(*) FROM frostbyte_iceberg.raw_pos.order_header_iceberg;

USE WAREHOUSE COMPUTE_WH;

-- loyalty_metrics_v view
CREATE OR REPLACE SECURE VIEW frostbyte_iceberg.analytics.customer_loyalty_metrics_v
COMMENT = 'Customer Loyalty Metrics from the ANALYTICS layer'
    AS
SELECT 
    cl.customer_id,
    cl.city,
    cl.country,
    cl.first_name,
    cl.last_name,
    cl.phone_number,
    cl.e_mail,
    SUM(oh.order_total) AS total_sales,
    ARRAY_AGG(DISTINCT oh.location_id) AS visited_location_ids_array
FROM frostbyte_iceberg.raw_customer.customer_loyalty_iceberg cl
JOIN frostbyte_iceberg.raw_pos.order_header_iceberg oh
ON cl.customer_id = oh.customer_id
GROUP BY cl.customer_id, cl.city, cl.country, cl.first_name,
cl.last_name, cl.phone_number, cl.e_mail;

-- Create Row-Access Policy to limit access by Consumer region using a local mapping table
CREATE OR REPLACE TABLE frostbyte_iceberg.governance.region_country_map(region STRING, country STRING);

INSERT INTO frostbyte_iceberg.governance.region_country_map
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
  

  USE ROLE ACCOUNTADMIN;
  USE DATABASE frostbyte_iceberg;
  
  CREATE OR REPLACE DATABASE ROLE frostbyte_iceberg.tastybytes_manager_role;
  
  GRANT USAGE ON ALL SCHEMAS IN DATABASE frostbyte_iceberg TO DATABASE ROLE frostbyte_iceberg.tastybytes_manager_role;
  GRANT SELECT ON VIEW FROSTBYTE_ICEBERG.ANALYTICS.CUSTOMER_LOYALTY_METRICS_V TO DATABASE ROLE frostbyte_iceberg.tastybytes_manager_role;
  GRANT SELECT ON ALL TABLES IN SCHEMA frostbyte_iceberg.raw_customer TO DATABASE ROLE frostbyte_iceberg.tastybytes_manager_role;
  GRANT SELECT ON ALL TABLES IN SCHEMA frostbyte_iceberg.raw_pos TO DATABASE ROLE frostbyte_iceberg.tastybytes_manager_role;
  GRANT SELECT ON ALL TABLES IN SCHEMA frostbyte_iceberg.tpch TO DATABASE ROLE frostbyte_iceberg.tastybytes_manager_role;
  GRANT SELECT ON FROSTBYTE_ICEBERG.RAW_CUSTOMER.CUSTOMER_LOYALTY_ICEBERG TO DATABASE ROLE frostbyte_iceberg.tastybytes_manager_role;
  GRANT SELECT ON FROSTBYTE_ICEBERG.RAW_POS.ORDER_HEADER_ICEBERG TO DATABASE ROLE frostbyte_iceberg.tastybytes_manager_role;
  GRANT SELECT ON FROSTBYTE_ICEBERG.TPCH.CUSTOMER_TPCH_ICEBERG TO DATABASE ROLE frostbyte_iceberg.tastybytes_manager_role;
  GRANT SELECT ON FROSTBYTE_ICEBERG.TPCH.NATION_TPCH_ICEBERG TO DATABASE ROLE frostbyte_iceberg.tastybytes_manager_role;
  
  CREATE OR REPLACE DATABASE ROLE frostbyte_iceberg.tastybytes_analyst_role;
  GRANT USAGE ON ALL SCHEMAS IN DATABASE frostbyte_iceberg TO DATABASE ROLE frostbyte_iceberg.tastybytes_analyst_role;
  GRANT SELECT ON VIEW FROSTBYTE_ICEBERG.ANALYTICS.CUSTOMER_LOYALTY_METRICS_V TO DATABASE ROLE frostbyte_iceberg.tastybytes_analyst_role;
  GRANT SELECT ON ALL ICEBERG TABLES IN SCHEMA frostbyte_iceberg.raw_customer TO DATABASE ROLE frostbyte_iceberg.tastybytes_analyst_role;
  GRANT SELECT ON ALL ICEBERG TABLES IN SCHEMA frostbyte_iceberg.raw_pos TO DATABASE ROLE frostbyte_iceberg.tastybytes_analyst_role;
  GRANT SELECT ON ALL ICEBERG TABLES IN SCHEMA frostbyte_iceberg.tpch TO DATABASE ROLE frostbyte_iceberg.tastybytes_analyst_role;
  GRANT SELECT ON VIEW FROSTBYTE_ICEBERG.RAW_CUSTOMER.CUSTOMER_LOYALTY_ICEBERG TO DATABASE ROLE frostbyte_iceberg.tastybytes_analyst_role;
  
  SHOW DATABASE ROLES IN DATABASE frostbyte_iceberg;
  
  --REVOKE DATABASE ROLE TASTYBYTES_MANAGER_ROLE FROM ROLE tasty_admin;
  --REVOKE DATABASE ROLE TASTYBYTES_ANALYST_ROLE FROM ROLE tasty_bi;
  GRANT DATABASE ROLE TASTYBYTES_MANAGER_ROLE TO ROLE tasty_admin;
  GRANT DATABASE ROLE TASTYBYTES_ANALYST_ROLE TO ROLE tasty_bi;

  CREATE SHARE frostbyte_iceberg_share;
  GRANT USAGE ON DATABASE FROSTBYTE_ICEBERG TO SHARE frostbyte_iceberg_share;
  GRANT DATABASE ROLE FROSTBYTE_ICEBERG.TASTYBYTES_MANAGER_ROLE TO SHARE frostbyte_iceberg_share;
  GRANT DATABASE ROLE FROSTBYTE_ICEBERG.TASTYBYTES_ANALYST_ROLE TO SHARE frostbyte_iceberg_share;
  GRANT USAGE ON ALL SCHEMAS IN DATABASE FROSTBYTE_ICEBERG TO SHARE frostbyte_iceberg_share;
  GRANT SELECT ON ALL ICEBERG TABLES IN SCHEMA frostbyte_iceberg.raw_customer TO SHARE frostbyte_iceberg_share;
  GRANT SELECT ON ALL ICEBERG TABLES IN SCHEMA frostbyte_iceberg.raw_pos TO SHARE frostbyte_iceberg_share;
  GRANT SELECT ON ALL ICEBERG TABLES IN SCHEMA frostbyte_iceberg.tpch TO SHARE frostbyte_iceberg_share;
  GRANT SELECT ON VIEW FROSTBYTE_ICEBERG.RAW_CUSTOMER.CUSTOMER_LOYALTY_ICEBERG TO SHARE frostbyte_iceberg_share;

  USE ROLE tasty_admin;
  USE ROLE tasty_bi;
  SELECT IS_DATABASE_ROLE_IN_SESSION('TASTYBYTES_MANAGER_ROLE');
  SELECT IS_DATABASE_ROLE_IN_SESSION('TASTYBYTES_ANALYST_ROLE');

  USE ROLE SYSADMIN;
  
  CREATE OR REPLACE ROW ACCESS POLICY frostbyte_iceberg.governance.country_row_policy
    AS (country STRING) RETURNS BOOLEAN ->
    country = 'Canada'
    OR current_role() IN ('ACCOUNTADMIN','SYSADMIN')
    OR IS_DATABASE_ROLE_IN_SESSION('TASTYBYTES_MANAGER_ROLE')
    OR EXISTS
    (
        SELECT 1
        FROM frostbyte_iceberg.governance.region_country_map map
        WHERE 1=1
        AND map.region = current_region()
        AND map.country = country
    )
COMMENT = 'Policy to limit rows returned based on region';

ALTER ICEBERG TABLE raw_customer.customer_loyalty_iceberg DROP ALL ROW ACCESS POLICIES;

ALTER ICEBERG TABLE raw_customer.customer_loyalty_iceberg ADD ROW ACCESS POLICY governance.country_row_policy ON (country);

USE ROLE SYSADMIN;

CREATE OR REPLACE AGGREGATION POLICY governance.tasty_order_agg_policy
  AS () RETURNS AGGREGATION_CONSTRAINT ->
    CASE
      WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN','SYSADMIN')
        THEN NO_AGGREGATION_CONSTRAINT()
      -- this database role in the Consumer account has ADMIN access
--      WHEN ((INVOKER_SHARE() IS NOT NULL) AND (IS_DATABASE_ROLE_IN_SESSION('TASTYBYTES_MANAGER_ROLE')))
      WHEN IS_DATABASE_ROLE_IN_SESSION('TASTYBYTES_MANAGER_ROLE')
        THEN NO_AGGREGATION_CONSTRAINT()
      ELSE AGGREGATION_CONSTRAINT(MIN_GROUP_SIZE => 100) -- atleast 100 rows in aggregate
    END;

-- with the Aggregation Policy created, let's apply it to our Order Header table
ALTER ICEBERG TABLE raw_pos.order_header_iceberg
    -- SET AGGREGATION POLICY governance.tasty_order_agg_policy;
    UNSET AGGREGATION POLICY;
    
/*
TPCH Query10 - Returned Item Reporting Query

The Returned Item Reporting Query finds the top 10 customers, in terms of their effect on lost revenue for a given quarter, who have returned parts. The query considers only parts that were ordered in the specified quarter. The query lists the customer's name, address, nation, phone number, account balance, comment information and revenue lost. The customers are listed in descending order of lost revenue. Revenue lost is defined as sum(l_extendedprice*(1-l_discount)) for all qualifying lineitems.

https://docs.deistercloud.com/content/Databases.30/TPCH%20Benchmark.90/Sample%20querys.20.xml?embedded=true
*/
SELECT
     c_custkey,
     c_name,
     TRUNCATE(SUM(l_extendedprice * (1 - l_discount))) AS lost_revenue,
     c_acctbal,
     n_name,
     c_address,
     c_phone,
     c_comment
FROM
     tpch.customer_tpch_iceberg,
     SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.ORDERS,
     SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.LINEITEM,
     tpch.nation_tpch_iceberg
WHERE
     c_custkey = o_custkey
     AND l_orderkey = o_orderkey
     AND o_orderdate >= to_date('1993-10-01')
     AND o_orderdate < dateadd(month, 3, to_date('1993-10-01'))
     AND l_returnflag = 'R'
     AND c_nationkey = n_nationkey
GROUP BY
     c_custkey,
     c_name,
     c_acctbal,
     c_phone,
     n_name,
     c_address,
     c_comment
ORDER BY
     lost_revenue DESC
LIMIT 10
;

USE ROLE sysadmin;
USE ROLE tasty_admin;
USE ROLE tasty_data_engineer;
USE ROLE tasty_data_scientist;
SELECT
    clm.city,
    ROUND(SUM(clm.total_sales), 0) AS total_sales_usd
FROM analytics.customer_loyalty_metrics_v clm
GROUP BY clm.city
ORDER BY total_sales_usd DESC;

SELECT DISTINCT COUNTRY FROM analytics.customer_loyalty_metrics_v;
