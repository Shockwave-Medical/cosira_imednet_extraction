-- Simple VARIANT staging table for raw COSIRA API data
CREATE OR REPLACE TABLE COSIRA_RAW_STAGING (
    LOAD_DATE DATE DEFAULT CURRENT_DATE(),
    FORM_ID NUMBER(38,0),
    FORM_KEY VARCHAR(100),
    RAW_RECORDS VARIANT,  -- Complete JSON response from records endpoint
    RECORD_COUNT NUMBER(38,0),
    
    PRIMARY KEY (FORM_ID, LOAD_DATE)
) COMMENT = 'Raw staging for COSIRA API responses - simple data storage';