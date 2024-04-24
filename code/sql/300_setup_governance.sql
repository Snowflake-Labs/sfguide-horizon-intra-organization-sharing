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
 Governance with Snowflake Horizon
  Protect Your Data
    1 - System Defined Roles and Privileges
    2 - Role Based Access Control
    3 - Tag-Based Masking
    4 - Row-Access Policies
    5 - Aggregation Policies
    6 - Projection Policies

  Know Your Data
    7 – Sensitive Data Classification
    8 – Sensitive Custom Classification
    9 – Access History (Read and Writes)

  Audit Your Data
    10 - Data Quality Monitoring

 Discovery with Snowflake Horizon
    11 - Universal Search
****************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author              Comments
------------------- ------------------- ------------------------------------------------------------
2023-01-13          Jacob Kranzler      Initial Data Governance Release
2024-02-01          Charlie Hammond     Initial DataOps Release
2024-04-03          Jacob Kranzler      DataOps Zero to Snowflake Refresh | V2
***************************************************************************************************/

/*----------------------------------------------------------------------------------
Before we begin, the Snowflake Access Control Framework is based on:
  • Role-based Access Control (RBAC): Access privileges are assigned to roles, which 
    are in turn assigned to users.
  • Discretionary Access Control (DAC): Each object has an owner, who can in turn 
    grant access to that object.

The key concepts to understanding access control in Snowflake are:
  • Securable Object: An entity to which access can be granted. Unless allowed by a 
    grant, access is denied. Securable Objects are owned by a Role (as opposed to a User)
      • Examples: Database, Schema, Table, View, Warehouse, Function, etc
  • Role: An entity to which privileges can be granted. Roles are in turn assigned 
    to users. Note that roles can also be assigned to other roles, creating a role 
    hierarchy.
  • Privilege: A defined level of access to an object. Multiple distinct privileges 
    may be used to control the granularity of access granted.
  • User: A user identity recognized by Snowflake, whether associated with a person 
    or program.

In Summary:
  • In Snowflake, a Role is a container for Privileges to a Securable Object.
  • Privileges can be granted Roles
  • Roles can be granted to Users
  • Roles can be granted to other Roles (which inherit that Roles Privileges)
  • When Users choose a Role, they inherit all the Privileges of the Roles in the 
    hierarchy.
----------------------------------------------------------------------------------*/


/*----------------------------------------------------------------------------------
Step 1 - Role Creation, GRANTS and SQL Variables

 Leverage System Defined Roles to create a Test Role and provide it access to the Customer 
 Loyalty data we will deploy our initial Snowflake Horizon Governance features against.
----------------------------------------------------------------------------------*/

-- let's use the Useradmin Role to create a Test Role
USE ROLE useradmin;

CREATE OR REPLACE ROLE tastybytes_test_role
    COMMENT = 'Test role for Tasty Bytes';

-- now we will switch to Securityadmin to handle our privilege GRANTS
USE ROLE securityadmin;

-- first we will grant ALL privileges on the Development Warehouse to our Sysadmin
GRANT ALL ON WAREHOUSE tasty_dev_wh TO ROLE sysadmin;

-- next we will grant only OPERATE and USAGE privileges to our Test Role
GRANT OPERATE, USAGE ON WAREHOUSE tasty_dev_wh TO ROLE tastybytes_test_role;

-- now we will grant USAGE on our Database and all Schemas within it
GRANT USAGE ON DATABASE frostbyte_tasty_bytes TO ROLE tastybytes_test_role;

GRANT USAGE ON ALL SCHEMAS IN DATABASE frostbyte_tasty_bytes TO ROLE tastybytes_test_role;

-- we are going to test Data Governance features as our Test Role, so let's ensure it can run SELECT statements against our Data Model
GRANT SELECT ON ALL TABLES IN SCHEMA frostbyte_tasty_bytes.raw_customer TO ROLE tastybytes_test_role;

GRANT SELECT ON ALL TABLES IN SCHEMA frostbyte_tasty_bytes.raw_pos TO ROLE tastybytes_test_role;

GRANT SELECT ON ALL VIEWS IN SCHEMA frostbyte_tasty_bytes.analytics TO ROLE tastybytes_test_role;

-- before we proceed, let's SET a SQL Variable to equal our CURRENT_USER()
SET my_user_var  = CURRENT_USER();

-- now we can GRANT our Role to the User we are currently logged in as
GRANT ROLE tastybytes_test_role TO USER identifier($my_user_var);


/*----------------------------------------------------------------------------------
Step 2 - Column-Level Security and Tagging = Tag-Based Masking

  The first Governance feature set we want to deploy and test will be Snowflake Tag
  Based Dynamic Data Masking. This will allow us to mask PII data in columns from
  our Test Role but not from more privileged Roles.

  CUSTOMER_LOYALTY Table in the RAW_CUSTOMER layer has PII that needs to be masked
  before users in this account or Consumers in another account are allowed to access.
----------------------------------------------------------------------------------*/

-- we can now USE the Test Role and Development Warehouse
USE ROLE tastybytes_test_role;
USE WAREHOUSE tasty_dev_wh;
USE DATABASE frostbyte_tasty_bytes;

    /**
     A tag-based masking policy combines the object tagging and masking policy features
     to allow a masking policy to be set on a tag using an ALTER TAG command. When the data type in
     the masking policy signature and the data type of the column match, the tagged column is
     automatically protected by the conditions in the masking policy.
    **/

-- first let's create Tags and Governance schemas to keep ourselves organized and follow best practices
USE ROLE accountadmin;

-- create a Tag Schema to contain our Object Tags
CREATE OR REPLACE SCHEMA tags
    COMMENT = 'Schema containing Object Tags';


-- we want everyone with access to this table to be able to view the tags 
GRANT USAGE ON SCHEMA tags TO ROLE public;


-- now we will create a Governance Schema to contain our Security Policies
CREATE OR REPLACE SCHEMA governance
    COMMENT = 'Schema containing Security Policies';

GRANT ALL ON SCHEMA governance TO ROLE sysadmin;

    /**
     Create database roles for fine-grained access to shared data:
     
        TASTYBYTES_AMERICAS_ROLE, TASTYBYTES_APJ_ROLE, TASTYBYTES_ADMIN_ROLE
        
     Consumer Admin will assign each database role to a corresponding local role.
    **/

CREATE OR REPLACE DATABASE ROLE tastybytes_americas_role
    COMMENT = 'Americas database role for Tasty Bytes';

CREATE OR REPLACE DATABASE ROLE tastybytes_apj_role
    COMMENT = 'APJ database role for Tasty Bytes';

CREATE OR REPLACE DATABASE ROLE tastybytes_admin_role
    COMMENT = 'Admin (all access) database role for Tasty Bytes';

-- now we will grant USAGE on all Schemas to the database roles
GRANT USAGE ON ALL SCHEMAS IN DATABASE frostbyte_tasty_bytes TO DATABASE ROLE tastybytes_americas_role;
GRANT USAGE ON ALL SCHEMAS IN DATABASE frostbyte_tasty_bytes TO DATABASE ROLE tastybytes_apj_role;
GRANT USAGE ON ALL SCHEMAS IN DATABASE frostbyte_tasty_bytes TO DATABASE ROLE tastybytes_admin_role;

-- ensure that the database roles can run SELECT statements against our Data Model
GRANT SELECT ON ALL TABLES IN SCHEMA frostbyte_tasty_bytes.raw_customer TO DATABASE ROLE tastybytes_americas_role;
GRANT SELECT ON ALL TABLES IN SCHEMA frostbyte_tasty_bytes.raw_customer TO DATABASE ROLE tastybytes_apj_role;
GRANT SELECT ON ALL TABLES IN SCHEMA frostbyte_tasty_bytes.raw_customer TO DATABASE ROLE tastybytes_admin_role;

GRANT SELECT ON ALL TABLES IN SCHEMA frostbyte_tasty_bytes.raw_pos TO DATABASE ROLE tastybytes_americas_role;
GRANT SELECT ON ALL TABLES IN SCHEMA frostbyte_tasty_bytes.raw_pos TO DATABASE ROLE tastybytes_apj_role;
GRANT SELECT ON ALL TABLES IN SCHEMA frostbyte_tasty_bytes.raw_pos TO DATABASE ROLE tastybytes_admin_role;

GRANT SELECT ON ALL TABLES IN SCHEMA frostbyte_tasty_bytes.analytics TO DATABASE ROLE tastybytes_americas_role;
GRANT SELECT ON ALL TABLES IN SCHEMA frostbyte_tasty_bytes.analytics TO DATABASE ROLE tastybytes_apj_role;
GRANT SELECT ON ALL TABLES IN SCHEMA frostbyte_tasty_bytes.analytics TO DATABASE ROLE tastybytes_admin_role;

GRANT DATABASE ROLE tastybytes_americas_role TO ROLE sysadmin;
GRANT DATABASE ROLE tastybytes_apj_role TO ROLE sysadmin;
GRANT DATABASE ROLE tastybytes_admin_role TO ROLE sysadmin;


-- next we will create one Tag for PII that allows these values: NAME, PHONE_NUMBER, EMAIL, BIRTHDAY
-- not only will this prevent free text values, but will also add the selection menu to Snowsight
CREATE OR REPLACE TAG tags.tasty_pii
    ALLOWED_VALUES 'NAME', 'PHONE_NUMBER', 'EMAIL', 'BIRTHDAY'
    COMMENT = 'Tag for PII, allowed values are: NAME, PHONE_NUMBER, EMAIL, BIRTHDAY';


-- with the Tags created, let's assign them to the relevant columns in our Customer Loyalty table
ALTER TABLE raw_customer.customer_loyalty
    MODIFY COLUMN 
    first_name SET TAG tags.tasty_pii = 'NAME',
    last_name SET TAG tags.tasty_pii = 'NAME',
    phone_number SET TAG tags.tasty_pii = 'PHONE_NUMBER',
    e_mail SET TAG tags.tasty_pii = 'EMAIL',
    birthday_date SET TAG tags.tasty_pii = 'BIRTHDAY';


-- now we can use the TAG_REFERENCE_ALL_COLUMNS function to return the Tags associated with our Customer Loyalty table
SELECT
    tag_database,
    tag_schema,
    tag_name,
    column_name,
    tag_value
FROM TABLE(information_schema.tag_references_all_columns
    ('frostbyte_tasty_bytes.raw_customer.customer_loyalty','table'));

    /**
     With our Tags in place we can now create our Masking Policies that will mask data for all but privileged Roles.

     We need to create 1 policy for every data type where the return data type can be implicitly cast
     into the column datatype. We can only assign 1 policy per datatype to an individual Tag.
    **/


    
-- create our String Datatype Masking Policy
  --> Note: a Masking Policy is made of standard conditional logic, such as a CASE statement
CREATE OR REPLACE MASKING POLICY governance.tasty_pii_string_mask AS (val STRING) RETURNS STRING ->
    CASE
        -- these active roles have access to unmasked values 
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN','SYSADMIN')
            THEN val
        -- only this database role in the Consumer account has access to unmasked values
        WHEN ((INVOKER_SHARE() IS NOT NULL) AND (IS_DATABASE_ROLE_IN_SESSION('TASTYBYTES_ADMIN_ROLE')))
            THEN val
        -- if a column is tagged with TASTY_PII : PHONE_NUMBER 
        -- then mask everything but the first 3 digits   
        WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('TAGS.TASTY_PII') = 'PHONE_NUMBER'
            THEN CONCAT(LEFT(val,3), '-***-****')
        -- if a column is tagged with TASTY_PII : EMAIL  
        -- then mask everything before the @ sign  
        WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('TAGS.TASTY_PII') = 'EMAIL'
            THEN CONCAT('**~MASKED~**','@', SPLIT_PART(val, '@', -1))
        -- all other conditions should be fully masked   
    ELSE '**~MASKED~**' 
END;

    /**
     The combination of an individuals City, first 3 Phone Number digits, and Birthday
     to re-identify them. Let's play it safe and also truncate Birthdays into 5 year buckets
     which will fit the use case of our Analyst
    **/

-- create our Date Masking Policy to return the modified Birthday
CREATE OR REPLACE MASKING POLICY governance.tasty_pii_date_mask AS (val DATE) RETURNS DATE ->
    CASE
        -- these active roles have access to unmasked values 
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN','SYSADMIN')
            THEN val
        -- this database role in the Consumer account has access to unmasked values
        WHEN ((INVOKER_SHARE() IS NOT NULL) AND (IS_DATABASE_ROLE_IN_SESSION('TASTYBYTES_ADMIN_ROLE')))
            THEN val
        -- if a column is tagged with TASTY_PII : BIRTHDAY  
        -- then truncate to 5 year buckets 
        WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('TAGS.TASTY_PII') = 'BIRTHDAY'
            THEN DATE_FROM_PARTS(YEAR(val) - (YEAR(val) % 5),1,1)
        -- if a Date column is not tagged with BIRTHDAY, return NULL
    ELSE NULL 
END;


-- now we are able to use an ALTER TAG statement to set the Masking Policies on the PII tagged columns
ALTER TAG tags.tasty_pii SET
    MASKING POLICY governance.tasty_pii_string_mask,
    MASKING POLICY governance.tasty_pii_date_mask;


-- with Tag Based Masking in-place, let's give our work a test using our Test Role and Development Warehouse
USE ROLE tastybytes_test_role;
USE WAREHOUSE tasty_dev_wh;

SELECT
    cl.customer_id,
    cl.first_name,
    cl.last_name,
    cl.phone_number,
    cl.e_mail,
    cl.birthday_date,
    cl.city,
    cl.country
FROM raw_customer.customer_loyalty cl
WHERE cl.country IN ('United States','Canada','Brazil');


-- the masking is working! let's also check the downstream Analytic layer View that leverages this table
SELECT TOP 10
    clm.customer_id,
    clm.first_name,
    clm.last_name,
    clm.phone_number,
    clm.e_mail,
    SUM(clm.total_sales) AS lifetime_sales_usd
FROM analytics.customer_loyalty_metrics_v clm
WHERE clm.city = 'San Mateo'
GROUP BY clm.customer_id, clm.first_name, clm.last_name, clm.phone_number, clm.e_mail
ORDER BY lifetime_sales_usd;


-- before moving on, let's quickly check our privileged users are able to see the data unmasked
USE ROLE accountadmin;

SELECT TOP 10
    clm.customer_id,
    clm.first_name,
    clm.last_name,
    clm.phone_number,
    clm.e_mail,
    SUM(clm.total_sales) AS lifetime_sales_usd
FROM analytics.customer_loyalty_metrics_v clm
WHERE clm.city = 'San Mateo'
GROUP BY clm.customer_id, clm.first_name, clm.last_name, clm.phone_number, clm.e_mail
ORDER BY lifetime_sales_usd;


/*----------------------------------------------------------------------------------
Step 4 - Row-Access Policies

 Within our CUSTOMER_LOYALTY table, our test role should only see Customers who are
 based in Germany. Database roles for Americas and APJ should be restricted to their regions.
 We will leverage the mapping table approach.
----------------------------------------------------------------------------------*/

USE ROLE accountadmin;

CREATE OR REPLACE TABLE governance.row_policy_map
    (role STRING, country_permissions STRING);

-- SELECT DISTINCT country from FROSTBYTE_TASTY_BYTES.RAW_CUSTOMER.CUSTOMER_LOYALTY;

-- INSERT the relevant Role to Country Permissions mapping
INSERT INTO governance.row_policy_map
    VALUES ('TASTYBYTES_TEST_ROLE','Germany'),
            ('TASTYBYTES_AMERICAS_ROLE','United States'),
            ('TASTYBYTES_AMERICAS_ROLE','Canada'),
            ('TASTYBYTES_AMERICAS_ROLE','Brazil'),
            ('TASTYBYTES_APJ_ROLE','South Korea'),
            ('TASTYBYTES_APJ_ROLE','India'),
            ('TASTYBYTES_APJ_ROLE','Japan'),
            ('ACCOUNTADMIN','Japan'),
            ('TASTYBYTES_APJ_ROLE','Australia');

 /**
     Snowflake supports row-level security through the use of Row Access Policies to
     determine which rows to return in the query result. The row access policy can be relatively
     simple to allow one particular role to view rows, or be more complex to include a mapping
     table in the policy definition to determine access to rows in the query result.
    **/

CREATE OR REPLACE ROW ACCESS POLICY governance.customer_country_row_policy
    AS (country STRING) RETURNS BOOLEAN ->
       CURRENT_ROLE() IN ('ACCOUNTADMIN','SYSADMIN') -- list of roles that will not be subject to the policy
        -- admin db-role is not subject to the policy
        OR ((INVOKER_SHARE() IS NOT NULL) AND (IS_DATABASE_ROLE_IN_SESSION('TASTYBYTES_ADMIN_ROLE'))) 
        OR EXISTS -- this clause references our mapping table from above to handle the row level filtering
            (
            SELECT rp.role
                FROM governance.row_policy_map rp
            WHERE 1=1
                AND (rp.role = CURRENT_ROLE() 
                    OR ((INVOKER_SHARE() IS NOT NULL) 
                        AND (IS_DATABASE_ROLE_IN_SESSION('TASTYBYTES_ADMIN_ROLE')))
                    )
                AND rp.country_permissions = country
            )
COMMENT = 'Policy to limit rows returned based on mapping table of ROLE and COUNTRY with database role for shared data';

 -- let's now apply the Row Access Policy to our Country column in the Customer Loyalty table
ALTER TABLE raw_customer.customer_loyalty
    ADD ROW ACCESS POLICY governance.customer_country_row_policy ON (country);
    -- DROP ROW ACCESS POLICY governance.customer_country_row_policy;

-- as we did for our masking, let's double check our Row Level Security is flowing into downstream Analytic Views
USE ROLE tastybytes_test_role;

SELECT
    clm.city,
    ROUND(SUM(clm.total_sales), 0) AS total_sales_usd
FROM analytics.customer_loyalty_metrics_v clm
GROUP BY clm.city
ORDER BY total_sales_usd DESC;

-- let's now check that our privileged Sysadmin is not impacted
USE ROLE sysadmin;

SELECT
    clm.city,
    ROUND(SUM(clm.total_sales), 0) AS total_sales_usd
FROM analytics.customer_loyalty_metrics_v clm
GROUP BY clm.city
ORDER BY total_sales_usd DESC;

/*----------------------------------------------------------------------------------
Step 5 - Aggregation Policies

 For Tasty Bytes and the Test role we have created, let's test an Aggregation Policy
 out against our Raw Order Header table.
----------------------------------------------------------------------------------*/

    /**
     An Aggregation Policy is a schema-level object that controls what type of
     query can access data from a table or view. When an aggregation policy is applied to a table,
     queries against that table must aggregate data into groups of a minimum size in order to return results,
     thereby preventing a query from returning information from an individual record.
    **/

-- to begin, let's once again assume our Accountadmin role
USE ROLE accountadmin;


-- for our use case, we will create a Conditional Aggregation Policy in our Governance
-- Schema that will only allow queries from non-admin users to return results for queries 
-- that aggregate more than 1000 rows
CREATE OR REPLACE AGGREGATION POLICY governance.tasty_order_test_aggregation_policy
  AS () RETURNS AGGREGATION_CONSTRAINT ->
    CASE
      WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN','SYSADMIN')
        THEN NO_AGGREGATION_CONSTRAINT()
      -- this database role in the Consumer account has ADMIN access
      WHEN ((INVOKER_SHARE() IS NOT NULL) AND (IS_DATABASE_ROLE_IN_SESSION('TASTYBYTES_ADMIN_ROLE')))
        THEN NO_AGGREGATION_CONSTRAINT()
      ELSE AGGREGATION_CONSTRAINT(MIN_GROUP_SIZE => 1000) -- atleast 1000 rows in aggregate
    END;


-- with the Aggregation Policy created, let's apply it to our Order Header table
ALTER TABLE raw_pos.order_header
    SET AGGREGATION POLICY governance.tasty_order_test_aggregation_policy;
    -- UNSET AGGREGATION POLICY;

-- now let's test our work by assuming our Test Role and executing a few queries
USE ROLE tastybytes_test_role;

  /**
     Bringing in the Customer Loyalty table that we have previously:
        1) Deployed Masking against PII columns
        2) Deployed Row Level Security to restrict our Test Role to only Tokyo results

     Let's answer a few aggregate business questions.
    **/

--> Note: If the query returns a group that contains fewer records than the minimum group size
--> of the policy, then Snowflake combines those groups into a remainder group.

-- What are the Total Order amounts by Gender?
SELECT 
    cl.gender,
    cl.city,
    COUNT(oh.order_id) AS count_order,
    ROUND(SUM(oh.order_amount),0) AS order_total
FROM raw_pos.order_header oh
JOIN raw_customer.customer_loyalty cl
    ON oh.customer_id = cl.customer_id
GROUP BY ALL
ORDER BY order_total DESC;

-- What are the Total Order amounts by Postal Code?
SELECT *
FROM frostbyte_tasty_bytes.analytics.orders_by_postal_code_v;

-- switching to our Accountadmin Role, let's now run that same query to see what the results
-- look like in a privileged Role not restricted by Row Access and Aggregation policies.
USE ROLE accountadmin;

-- What are the Total Order amounts by Postal Code?
SELECT *
FROM frostbyte_tasty_bytes.analytics.orders_by_postal_code_v;



/*----------------------------------------------------------------------------------
Step 6 - Projection Policies

 Within this step, we will cover another Privacy Policy framework provided by Snowflake
 Horizon this time diving into Projection Policies which in short will prevent queries
 from using a SELECT statement to project values from a column.
----------------------------------------------------------------------------------*/
USE ROLE accountadmin;

-- for our use case, we will create a Conditional Projection Policy in our Governance Schema
-- that will only allow our Admin Roles to project the columns we will assign it to
CREATE OR REPLACE PROJECTION POLICY governance.tasty_customer_test_projection_policy
  AS () RETURNS PROJECTION_CONSTRAINT -> 
  CASE
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN','SYSADMIN')
        THEN PROJECTION_CONSTRAINT(ALLOW => true)
    -- this database role in the Consumer account has ADMIN access
    WHEN ((INVOKER_SHARE() IS NOT NULL) AND (IS_DATABASE_ROLE_IN_SESSION('TASTYBYTES_ADMIN_ROLE')))
        THEN PROJECTION_CONSTRAINT(ALLOW => true)
    ELSE PROJECTION_CONSTRAINT(ALLOW => false)
  END;

-- with the Projection Policy in place, let's assign it to our Postal Code column
ALTER TABLE raw_customer.customer_loyalty
 MODIFY COLUMN postal_code
 SET PROJECTION POLICY governance.tasty_customer_test_projection_policy;
 -- UNSET PROJECTION POLICY;

-- let's assume our Test Role and begin testing
USE ROLE tastybytes_test_role;

-- what does a SELECT * against the table yield?
SELECT TOP 100 * FROM raw_customer.customer_loyalty;


-- what if we EXCLUDE the postal_code column?
SELECT TOP 100 * EXCLUDE postal_code FROM raw_customer.customer_loyalty;

    /**
     Although our Projection Policy blocks our Test Role from including the Postal Code column
     in the SELECT clause it can still be used in the WHERE clause to assist with analysis

     Knowing this, let's now help our marketing team by addressing a few of their questions
    **/

-- which members from postal_code 144-0000 should recieve a program anniversary promotion this month?
SELECT 
    customer_id,
    preferred_language,
    sign_up_date
FROM raw_customer.customer_loyalty
WHERE 1=1
    AND postal_code = '144-0000'
    AND MONTH(sign_up_date) = MONTH(CURRENT_DATE());


-- which members from postal_code V5K 0A6 have children and should recieve a family night discount code?
SELECT 
    customer_id,
    preferred_language,
    children_count
FROM raw_customer.customer_loyalty
WHERE 1=1
    AND postal_code = 'V5K 0A6'
    AND children_count NOT IN ('0','Undisclosed')
ORDER BY customer_id;
