-- COSIRA data extraction task with automatic retry-- COSIRA data extraction task with automatic retry

CREATE OR REPLACE TASK extract_cosira_taskCREATE OR REPLACE TASK extract_cosira_task

WAREHOUSE = COMPUTE_WHWAREHOUSE = COMPUTE_WH

SCHEDULE = 'USING CRON 0 6 * * * America/New_York'  -- Daily at 6 AM ETSCHEDULE = 'USING CRON 0 6 * * * America/New_York'  -- Daily at 6 AM ET

ASAS

CALL extract_cosira_data();CALL extract_cosira_data();