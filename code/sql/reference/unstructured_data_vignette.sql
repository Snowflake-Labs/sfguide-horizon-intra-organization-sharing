USE ROLE sysadmin;
USE WAREHOUSE tasty_de_wh;
USE SCHEMA frostbyte_tasty_bytes.movie_reviews;

ALTER STAGE movie_stage refresh;
SELECT * FROM DIRECTORY(@movie_stage);

CREATE FUNCTION extract_review(file_path string)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'parse_text_file'
AS
$$

def parse_text_file(file_path):
    from snowflake.snowpark.files import SnowflakeFile
    with SnowflakeFile.open(file_path, 'r') as f:
        lines = [ line.strip() for line in f ]
        return lines
$$
;

CREATE OR REPLACE SECURE VIEW movie_reviews_v
    AS
SELECT relative_path as review_file_name, 
    extract_review(build_scoped_file_url(@movie_stage, relative_path)) as review_content,
    build_scoped_file_url(@movie_stage, relative_path) as scoped_url
FROM DIRECTORY(@movie_stage);

SELECT * FROM movie_reviews_v limit 10;


SELECT review_file_name, 
SNOWFLAKE.CORTEX.SUMMARIZE(review_content) as Summary, 
TO_DECIMAL(SNOWFLAKE.CORTEX.SENTIMENT(review_content),3,2) as Sentiment,
scoped_url
FROM movie_reviews_v
ORDER BY Sentiment DESC
LIMIT 10;

SELECT *
  FROM snowflake.account_usage.metering_daily_history
  WHERE SERVICE_TYPE='AI_SERVICES';





