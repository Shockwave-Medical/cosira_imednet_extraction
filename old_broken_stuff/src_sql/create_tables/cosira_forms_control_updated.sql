-- Updated COSIRA forms control table with essential API metadata
CREATE OR REPLACE TABLE COSIRA_FORMS_CONTROL (
    -- Essential API metadata (from forms endpoint)
    STUDY_KEY VARCHAR(100),
    FORM_ID NUMBER(38,0),
    FORM_KEY VARCHAR(100),
    FORM_NAME VARCHAR(500),
    FORM_TYPE VARCHAR(100),
    REVISION NUMBER(38,0),
    DISABLED BOOLEAN,  -- Key field from API for control
    DATE_CREATED TIMESTAMP_NTZ(9),
    DATE_MODIFIED TIMESTAMP_NTZ(9),
    
    -- Extraction tracking
    RECORD_COUNT NUMBER(38,0), -- NULL=never tried, 0=empty, >0=data
    EXTRACTION_STATUS VARCHAR(100), -- SUCCESS, FAILED, PENDING
    LAST_EXTRACTED TIMESTAMP_NTZ(9),
    ERROR_MESSAGE VARCHAR(1000),
    
    -- History tracking
    LOAD_DATE DATE DEFAULT CURRENT_DATE(),
    
    PRIMARY KEY (FORM_ID, LOAD_DATE)
) COMMENT = 'COSIRA forms control and history - hybrid staging approach with SFCS column cleaning';