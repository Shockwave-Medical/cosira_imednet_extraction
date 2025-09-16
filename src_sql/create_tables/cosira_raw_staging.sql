-- Raw staging table for COSIRA form data in VARIANT format
-- Stores all historical extractions for audit trail

CREATE OR REPLACE TABLE COSIRA_RAW_STAGING (
    LOAD_DATE DATE DEFAULT CURRENT_DATE(),
    FORM_ID NUMBER(38,0),
    FORM_KEY VARCHAR(100),
    FORM_NAME VARCHAR(500),
    RAW_RECORDS VARIANT,
    RECORD_COUNT NUMBER(38,0),
    EXTRACTION_TIMESTAMP TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    
    PRIMARY KEY (LOAD_DATE, FORM_ID)
) COMMENT = 'Raw VARIANT staging for COSIRA form data - maintains historical audit trail';