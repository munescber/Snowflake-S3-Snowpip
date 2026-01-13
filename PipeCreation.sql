-- ============================================================
-- PIPE CREATION FOR SNOWPIPE
-- This script:
--   1. Creates a Snowpipe that auto-loads files from an S3 stage
--   2. Describes the pipe to verify configuration
--   3. Queries the target table
--   4. Checks Snowpipe load history
-- ============================================================


-- ============================================================
-- 1. CREATE SNOWPIPE
-- This pipe automatically loads new files from the external stage
-- MANAGE_DB.EXTERNAL_STAGES.aws_snowpipe into the target table
-- TEST_DB.FIRST_SCHEMA.TITANIC_PASSENGERS.
--
-- auto_ingest = TRUE enables event-based loading using S3 notifications.
-- ============================================================
CREATE OR REPLACE PIPE MANAGE_DB.PIPES.TITANIC_PIPE
  AUTO_INGEST = TRUE
AS
COPY INTO TEST_DB.FIRST_SCHEMA.TITANIC_PASSENGERS
FROM @MANAGE_DB.EXTERNAL_STAGES.aws_snowpipe;


-- ============================================================
-- 2. DESCRIBE PIPE
-- Shows pipe details including:
--   - Notification channel
--   - Integration used
--   - Copy statement
--   - Last load time
-- ============================================================
DESC PIPE MANAGE_DB.PIPES.TITANIC_PIPE;


-- ============================================================
-- 3. VIEW LOADED DATA
-- Displays the contents of the target table after Snowpipe loads.
-- Useful for verifying that ingestion is working correctly.
-- ============================================================
SELECT * 
FROM TEST_DB.FIRST_SCHEMA.TITANIC_PASSENGERS;


-- ============================================================
-- 4. CHECK LOAD HISTORY
-- Shows historical Snowpipe loads across the account.
-- You can filter by PIPE_NAME if needed.
-- ============================================================
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.LOAD_HISTORY
ORDER BY LAST_LOAD_TIME DESC;
