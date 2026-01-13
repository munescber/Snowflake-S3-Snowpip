-- ============================================================
-- STAGE CREATION FOR A SNOWPIPE PIPELINE
-- This script:
--   1. Creates a target table
--   2. Defines a CSV file format
--   3. Creates a Snowflake Storage Integration for S3 access
--   4. Creates an external stage pointing to an S3 bucket
--   5. Tests file access and data preview
--   6. Provides commands for monitoring Snowpipe loads
-- ============================================================


-- ============================================================
-- 0. CREATE TARGET TABLE
-- This table will store the Titanic passenger data loaded
-- through Snowpipe or COPY INTO.
-- ============================================================
CREATE OR REPLACE TABLE TEST_DB.FIRST_SCHEMA.titanic_passengers (
    PassengerId     INTEGER,
    Survived        BOOLEAN,
    Pclass          INTEGER,
    Name            STRING,
    Sex             STRING,
    Age             FLOAT,
    SibSp           INTEGER,
    Parch           INTEGER,
    Ticket          STRING,
    Fare            FLOAT,
    Cabin           STRING,
    Embarked        STRING
);


-- ============================================================
-- 1. CREATE FILE FORMAT FOR CSV FILES
-- Defines how Snowflake should interpret CSV files:
--   - Comma delimiter
--   - Skip header row
--   - Treat common NULL markers as NULL
--   - Allow optional quotes
--   - Do not fail on column count mismatches
-- ============================================================
CREATE OR REPLACE FILE FORMAT manage_db.file_formats.csv_fileformat
    TYPE = csv
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '\\N', '\N', '')
    EMPTY_FIELD_AS_NULL = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    FIELD_OPTIONALLY_ENCLOSED_BY = '"';


-- ============================================================
-- 2. CREATE STORAGE INTEGRATION FOR AWS S3
-- This allows Snowflake to securely access S3 using an IAM role.
-- After creation:
--   - Run DESC INTEGRATION to obtain:
--       * STORAGE_AWS_IAM_USER_ARN
--       * STORAGE_AWS_EXTERNAL_ID
--   - Add these values to the AWS IAM role trust policy.
-- ============================================================
CREATE OR REPLACE STORAGE INTEGRATION s3_snowpipe
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::926849997916:role/Snowflake_S3'
  STORAGE_ALLOWED_LOCATIONS = ('s3://snowflake-test-bm02/')
  COMMENT = 'S3 Bucket: snowflake-test-bm02 / data-samples';

-- View integration details needed for IAM trust policy
DESC INTEGRATION s3_snowpipe;


-- ============================================================
-- 3. CREATE EXTERNAL STAGE
-- This stage points to the S3 folder where Snowpipe will read files.
-- It uses:
--   - The storage integration created above
--   - The CSV file format defined earlier
-- ============================================================
CREATE OR REPLACE STAGE MANAGE_DB.external_stages.aws_snowpipe
    URL = 's3://snowflake-test-bm02/snowpipe'
    STORAGE_INTEGRATION = s3_snowpipe
    FILE_FORMAT = MANAGE_DB.FILE_FORMATS.CSV_FILEFORMAT;

-- View stage details
DESC STAGE MANAGE_DB.external_stages.aws_snowpipe;

-- List files currently in the stage
LIST @manage_db.external_stages.aws_snowpipe;


-- ============================================================
-- 4. PREVIEW FILE CONTENTS FROM THE STAGE
-- This reads the CSV directly from S3 without loading it.
-- Useful for validating file format settings.
-- ============================================================
SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
FROM @MANAGE_DB.EXTERNAL_STAGES.aws_snowpipe/titanic.csv
    (FILE_FORMAT => MANAGE_DB.FILE_FORMATS.CSV_FILEFORMAT);


-- ============================================================
-- 5. TABLE VALIDATION COMMANDS
-- ============================================================
SELECT * FROM TEST_DB.FIRST_SCHEMA.titanic_passengers;

-- Clear table before reloading
TRUNCATE TEST_DB.FIRST_SCHEMA.titanic_passengers;


-- ============================================================
-- 6. SNOWPIPE MONITORING COMMANDS
-- These commands help inspect Snowpipe status and load history.
-- ============================================================

-- View pipe definition
DESC PIPE MANAGE_DB.PIPES.TITANIC_PIPE;

-- Check Snowpipe's current status
SELECT SYSTEM$PIPE_STATUS('MANAGE_DB.PIPES.TITANIC_PIPE');

-- View historical loads performed by Snowpipe
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.LOAD_HISTORY
WHERE PIPE_NAME = 'MANAGE_DB.PIPES.TITANIC_PIPE'
ORDER BY LAST_LOAD_TIME DESC;


-- ============================================================
-- 7. MANUAL LOAD (COPY INTO)
-- Useful for testing before enabling Snowpipe automation.
-- ============================================================
COPY INTO TEST_DB.FIRST_SCHEMA.TITANIC_PASSENGERS
FROM @MANAGE_DB.EXTERNAL_STAGES.aws_snowpipe/titanic.csv
FILE_FORMAT = (FORMAT_NAME = MANAGE_DB.FILE_FORMATS.CSV_FILEFORMAT)
ON_ERROR = 'CONTINUE';


-- ============================================================
-- 8. DEBUGGING: CHECK RAW FILE STRUCTURE
-- This helps identify malformed rows or column count mismatches.
-- ============================================================
SELECT
    METADATA$FILENAME,
    METADATA$ROW_NUMBER,
    ARRAY_SIZE(SPLIT($0, ',')) AS raw_column_count
FROM @MANAGE_DB.EXTERNAL_STAGES.aws_snowpipe/titanic.csv
(FILE_FORMAT => MANAGE_DB.FILE_FORMATS.CSV_FILEFORMAT)
LIMIT 20;
