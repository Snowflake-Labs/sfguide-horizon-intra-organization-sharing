/*----------------------------------------------------------------------------------
Step 1 - Configure Database Roles to enable privacy policies on shared data

Setup TEST and ADMIN shared database roles that allow Provider governance policies
to be enforced on roles in the Consumer account.

Note: FILL IN <share_name> that was implicitly created in the Listing.
----------------------------------------------------------------------------------*/

USE ROLE sysadmin;
USE WAREHOUSE tasty_dev_wh;
USE DATABASE frostbyte_tasty_bytes;

SHOW SHARES; -- fill in <share_name>

GRANT DATABASE ROLE tastybytes_americas_role TO SHARE <share_name>;
GRANT DATABASE ROLE tastybytes_apj_role TO SHARE <share_name>;
GRANT DATABASE ROLE tastybytes_admin_role TO ROLE <share_name>;

use ROLE accountadmin;

-- allow all users access to Snowflake Cortext LLM functions
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE PUBLIC;

