-- ============================================================
-- MODIFYING AN EXISTING SNOWPIPE
-- This script:
--   1. Creates a new target table
--   2. Pauses the existing pipe
--   3. Verifies pipe status
--   4. Recreates the pipe to load into the new table
--   5. Refreshes the pipe to re-evaluate staged files
--   6. Performs a manual COPY test
--   7. Resumes the pipe
-- ============================================================


-- ============================================================
-- 1. CREATE NEW TARGET TABLE
-- This new table will replace the original load destination.
-- The structure matches the original Titanic dataset.
-- ============================================================
CREATE OR REPLACE TABLE TEST_DB.FIRST_SCHEMA.titanic_passengers_2 (
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
-- 2. PAUSE THE EXISTING PIPE
-- Required before modifying or replacing a pipe.
-- Ensures no new files are ingested during the update.
-- ============================================================
ALTER PIPE MANAGE_DB.PIPES.TITANIC_PIPE 
SET PIPE_EXECUTION_PAUSED = TRUE;


-- ============================================================
-- 3. VERIFY PIPE STATUS
-- SYSTEM$PIPE_STATUS returns:
--   - executionState (RUNNING / PAUSED)
--   - pendingFileCount
--   - last ingestion times
-- Ensure pendingFileCount = 0 before modifying the pipe.
-- ============================================================
SELECT SYSTEM$PIPE_STATUS('MANAGE_DB.PIPES.TITANIC_PIPE');


-- ============================================================
-- 4. RECREATE PIPE WITH NEW TARGET TABLE
-- The pipe is recreated to load into titanic_passengers_2
-- instead of the original table.
-- auto_ingest = TRUE keeps S3 event notifications active.
-- ============================================================
CREATE OR REPLACE PIPE MANAGE_DB.PIPES.TITANIC_PIPE
  AUTO_INGEST = TRUE
AS
COPY INTO TEST_DB.FIRST_SCHEMA.TITANIC_PASSENGERS_2
FROM @MANAGE_DB.EXTERNAL_STAGES.aws_snowpipe;


-- ============================================================
-- 5. REFRESH PIPE
-- Forces Snowflake to re-check the stage for any files that
-- may not have been processed due to the pipe recreation.
-- ============================================================
ALTER PIPE MANAGE_DB.PIPES.TITANIC_PIPE REFRESH;


-- ============================================================
-- 6. MANUAL COPY TEST
-- Useful for validating:
--   - File format
--   - Table structure
--   - Data quality
-- before resuming automated ingestion.
-- ============================================================
COPY INTO TEST_DB.FIRST_SCHEMA.TITANIC_PASSENGERS_2
FROM @MANAGE_DB.EXTERNAL_STAGES.aws_snowpipe/titanic.csv
FILE_FORMAT = (FORMAT_NAME = MANAGE_DB.FILE_FORMATS.CSV_FILEFORMAT)
ON_ERROR = 'CONTINUE';


-- ============================================================
-- 7. RESUME PIPE
-- Reactivates automatic ingestion from S3.
-- ============================================================
ALTER PIPE MANAGE_DB.PIPES.TITANIC_PIPE 
SET PIPE_EXECUTION_PAUSED = FALSE;
