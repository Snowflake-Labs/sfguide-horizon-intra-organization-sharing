-- ################################################################
-- SETUP_PACKAGE_SCRIPT.SQL
-- 
-- This script is run whenever the application is deployed (creation or upgrade)
-- by the Provider. It provides access controls on data that the application is able
-- to query from the Provider when installed in the Consumer account.
--
-- SHARED_CONTENT_SCHEMA is created to securely share Provider data needed by the app.
-- ################################################################
USE {{ package_name }};
create schema if not exists shared_content_schema;

use schema shared_content_schema;

CREATE OR REPLACE VIEW orders_v AS 
    SELECT * FROM frostbyte_tasty_bytes_app.analytics.orders_v;

grant usage on schema shared_content_schema to share in application package {{ package_name }};

grant reference_usage on database FROSTBYTE_TASTY_BYTES_APP to share in application package {{ package_name }};

grant select on view orders_v to share in application package {{ package_name }};