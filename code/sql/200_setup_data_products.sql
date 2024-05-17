-- Step 1(a) - Acquire "Weather Source LLC: frostbyte" Snowflake Marketplace Listing

/*--- 
    1. Click -> Data Products (Cloud Icon in left sidebar)
    2. Click -> Marketplace
    3. Search -> frostbyte
    4. Click -> Weather Source LLC: frostbyte
    5. Click -> Get
    6. Click -> Options
    6. Database Name -> FROSTBYTE_WEATHERSOURCE (all capital letters)
    7. "Which roles, in addition to ACCOUNTADMIN, can access this database?" -> PUBLIC
    8. Click -> Get
---*/

-- Step 1(b) - Create local tables from Weather Source shared tables for data products to share
USE ROLE sysadmin;
USE WAREHOUSE demo_build_wh;

DESCRIBE TABLE frostbyte_weathersource.onpoint_id.history_day;
SELECT DISTINCT country 
FROM frostbyte_weathersource.onpoint_id.postal_codes
ORDER BY country;

CREATE OR REPLACE TABLE frostbyte_tasty_bytes.weather.history_day
    AS
SELECT * FROM frostbyte_weathersource.onpoint_id.history_day 
WHERE country in ('US','DE','ZA') AND year(date_valid_std) = 2022;

SELECT COUNT(*) FROM frostbyte_tasty_bytes.weather.history_day;

CREATE OR REPLACE TABLE frostbyte_tasty_bytes.weather.postal_codes
    AS
SELECT * FROM frostbyte_weathersource.onpoint_id.postal_codes where country in ('US','DE','ZA');

SELECT COUNT(*) FROM frostbyte_tasty_bytes.weather.postal_codes;

-- Step 2 - Harmonizing First (Point of Sale) and Third Party Data (Weather)
CREATE OR REPLACE SECURE VIEW frostbyte_tasty_bytes.harmonized.daily_weather_v
    AS
SELECT 
    hd.*,
    TO_VARCHAR(hd.date_valid_std, 'YYYY-MM') AS yyyy_mm,
    pc.city_name AS city,
    c.country AS country_desc
FROM frostbyte_tasty_bytes.weather.history_day hd
JOIN frostbyte_tasty_bytes.weather.postal_codes pc
    ON pc.postal_code = hd.postal_code
    AND pc.country = hd.country
JOIN frostbyte_tasty_bytes.raw_pos.country c
    ON c.iso_country = hd.country
    AND c.city = hd.city_name;

-- SELECT COUNT(*) FROM frostbyte_tasty_bytes.harmonized.daily_weather_v;

-- Step 3 - Creating SQL Functions
    --> create the SQL function that translates Fahrenheit to Celsius
CREATE OR REPLACE SECURE FUNCTION frostbyte_tasty_bytes.analytics.fahrenheit_to_celsius(temp_f NUMBER(35,4))
RETURNS NUMBER(35,4)
AS
$$
    (temp_f - 32) * (5/9)
$$;

    --> create the SQL function that translates Inches to Millimeter
CREATE OR REPLACE SECURE FUNCTION frostbyte_tasty_bytes.analytics.inch_to_millimeter(inch NUMBER(35,4))
RETURNS NUMBER(35,4)
    AS
$$
    inch * 25.4
$$;


-- Step 4 - Deploy Daily City Metrics as a Dynamic Table for sharing in a listing

CREATE OR REPLACE DYNAMIC TABLE frostbyte_tasty_bytes.analytics.daily_city_metrics
COMMENT = 'Daily Weather Source Metrics and Orders Data for our Cities'
TARGET_LAG = '15 MINUTES'
WAREHOUSE = tasty_de_wh
    AS
SELECT 
    fd.date_valid_std AS date,
    fd.city_name,
    fd.country_desc,
    ZEROIFNULL(ROUND(SUM(odv.price),0)) AS daily_sales,
    ROUND(AVG(fd.avg_temperature_air_2m_f),2) AS avg_temperature_fahrenheit,-
    ROUND(AVG(frostbyte_tasty_bytes.analytics.fahrenheit_to_celsius(fd.avg_temperature_air_2m_f)),2) AS avg_temperature_celsius,
    ROUND(AVG(fd.tot_precipitation_in),2) AS avg_precipitation_inches,
    ROUND(AVG(frostbyte_tasty_bytes.analytics.inch_to_millimeter(fd.tot_precipitation_in)),2) AS avg_precipitation_millimeters,
    MAX(fd.max_wind_speed_100m_mph) AS max_wind_speed_100m_mph
FROM frostbyte_tasty_bytes.harmonized.daily_weather_v fd
LEFT JOIN frostbyte_tasty_bytes.harmonized.orders_v odv
    ON fd.date_valid_std = DATE(odv.order_ts)
    AND fd.city_name = odv.primary_city
    AND fd.country_desc = odv.country
    AND year(DATE(odv.order_ts)) = 2022
GROUP BY fd.date_valid_std, fd.city_name, fd.country_desc;


-- Step 5 - Deploy Franchise Aggregates as a Materialized View

CREATE OR REPLACE SECURE MATERIALIZED VIEW frostbyte_tasty_bytes.analytics.franchise_city_v
AS
SELECT
    country,
    city,
    COUNT(*) as franchises,
FROM frostbyte_tasty_bytes.raw_pos.franchise
GROUP BY country, city;

-- Step 6 - Deploy Total Order amounts by Postal Code as a Secure View

CREATE OR REPLACE SECURE VIEW frostbyte_tasty_bytes.analytics.orders_by_postal_code_v
  (
	POSTAL_CODE    COMMENT 'Postal code area for order management',
	CITY           COMMENT 'City for which order data is aggregated',
	COUNT_ORDER    COMMENT 'Total number of orders for the area',
	ORDER_TOTAL    COMMENT 'Total order volume for the area'
  )
AS
SELECT 
    cl.postal_code,
    cl.city,
    COUNT(oh.order_id) AS count_order,
    ROUND(SUM(oh.order_amount),0) AS order_total
FROM raw_pos.order_header oh
JOIN raw_customer.customer_loyalty cl
    ON oh.customer_id = cl.customer_id
GROUP BY ALL
ORDER BY order_total DESC;
