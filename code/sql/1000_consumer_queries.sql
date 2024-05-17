/*----------------------------------------------------------------------------------
Step 1 - Query Analytics Listing
----------------------------------------------------------------------------------*/

USE ROLE consumer_americas_role;
USE WAREHOUSE tasty_dev_wh;
USE DATABASE frostbyte_tasty_bytes;

-- Where are the most TastyBytes franchises in the world?
SELECT * 
FROM analytics.franchise_city_v
ORDER BY FRANCHISES DESC;

-- How many times were daily sales zero by city and month?
SELECT
    month(dcm.date) as monthnum,
    dcm.city_name as city,
    dcm.country_desc as country,
    count(*) as zero_sales_days 
FROM analytics.daily_city_metrics dcm
WHERE 1=1
    AND dcm.daily_sales = 0
GROUP BY monthnum, city, country
ORDER BY zero_sales_days;

-- Were weather factors responsible for the zero sales days in selected cities?
-- (Berlin, Boston)
SELECT 
    dcm.date,
    dcm.city_name,
    dcm.country_desc,
    dcm.daily_sales,
    dcm.avg_temperature_fahrenheit || 'F (' || 
        round(analytics.fahrenheit_to_celsius(dcm.avg_temperature_fahrenheit),2) 
        || 'C)' avg_temp,
    dcm.avg_precipitation_inches || 'in (' || 
        round(analytics.inch_to_millimeter(dcm.avg_precipitation_inches),2)
        || 'mm)' avg_precipitation,   
    dcm.max_wind_speed_100m_mph
FROM analytics.daily_city_metrics dcm
WHERE 1=1
    AND dcm.city_name IN ('Berlin','Boston')
    AND month(dcm.date) IN (1,2)
ORDER BY date DESC;


/*----------------------------------------------------------------------------------
Step 2 - Column-Level Security and Tagging = Tag-Based Masking

Verify that CUSTOMER_LOYALTY_METRICS_V PII is masked for a non-admin role
----------------------------------------------------------------------------------*/

USE ROLE consumer_apj_role;
USE WAREHOUSE tasty_dev_wh;

SELECT TOP 10
    clm.customer_id,
    clm.first_name,
    clm.last_name,
    clm.phone_number,
    clm.e_mail,
    SUM(clm.total_sales) AS lifetime_sales_usd
FROM analytics.customer_loyalty_metrics_v clm
WHERE clm.city = 'Mumbai'
GROUP BY clm.customer_id, clm.first_name, clm.last_name, clm.phone_number, clm.e_mail
ORDER BY lifetime_sales_usd;

-- before moving on, let's quickly check our privileged users are able to see the data unmasked
USE ROLE sysadmin;

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
Step 3 - Row-Access Policies

 Verify that APJ, AMERICAS see restricted rows from CUSTOMER_LOYALTY_METRICS_V 
----------------------------------------------------------------------------------*/

USE ROLE consumer_apj_role;

SELECT
    clm.city,
    SUM(clm.total_sales) AS total_sales_usd
FROM analytics.customer_loyalty_metrics_v clm
GROUP BY clm.city;

USE ROLE consumer_apj_role;

SELECT
    clm.city,
    SUM(clm.total_sales) AS total_sales_usd
FROM analytics.customer_loyalty_metrics_v clm
GROUP BY clm.city;

-- let's now check that our privileged Sysadmin is not impacted
USE ROLE sysadmin;

SELECT
    clm.city,
    SUM(clm.total_sales) AS total_sales_usd
FROM analytics.customer_loyalty_metrics_v clm
GROUP BY clm.city;


/*----------------------------------------------------------------------------------
Step 4 - Aggregation Policies

 Verify aggregation constraint on Raw ORDER_HEADER table for test role.
----------------------------------------------------------------------------------*/

-- What are the Total Order amounts by Postal Code?

USE ROLE consumer_apj_role;

SELECT *
FROM analytics.orders_by_postal_code_v;

-- let's now check that our privileged Sysadmin is not impacted
USE ROLE sysadmin;

SELECT *
FROM analytics.orders_by_postal_code_v;


/*----------------------------------------------------------------------------------
Step 5 - Projection Policies

Verify that the POSTAL_CODE column in the CUSTOMER_LOYALTY table cannot be projected 
----------------------------------------------------------------------------------*/

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
