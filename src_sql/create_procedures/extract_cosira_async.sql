-- Async procedure to extract all pending COSIRA forms concurrently
-- Uses SQL async child jobs for true concurrency

CREATE OR REPLACE PROCEDURE extract_cosira_async(batch_size INTEGER DEFAULT 20)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  DECLARE
    total_forms INTEGER DEFAULT 0;
    batch_count INTEGER DEFAULT 0;
    
  -- Get count of pending forms
  SELECT COUNT(*) INTO total_forms
  FROM COSIRA_FORMS_CONTROL 
  WHERE DISABLED = FALSE 
    AND (EXTRACTION_STATUS != 'SUCCESS' OR EXTRACTION_STATUS IS NULL)
    AND LOAD_DATE = CURRENT_DATE();
    
  IF (total_forms = 0) THEN
    RETURN 'No pending forms to extract';
  END IF;
  
  -- Process forms with async child jobs - batch by batch_size to control concurrency
  FOR form_batch IN (
    SELECT 
      FORM_ID,
      FORM_KEY,
      CEIL(ROW_NUMBER() OVER (ORDER BY FORM_ID) / :batch_size) as batch_num
    FROM COSIRA_FORMS_CONTROL 
    WHERE DISABLED = FALSE 
      AND (EXTRACTION_STATUS != 'SUCCESS' OR EXTRACTION_STATUS IS NULL)
      AND LOAD_DATE = CURRENT_DATE()
    ORDER BY FORM_ID
  ) DO
    
    -- Launch async extraction for this form
    ASYNC (
      SELECT extract_single_form_udf(:form_batch.FORM_ID, :form_batch.FORM_KEY) as result,
             :form_batch.FORM_ID as form_id,
             :form_batch.FORM_KEY as form_key
    );
    
    -- Wait for batch completion every batch_size forms
    IF (ROW_NUMBER() OVER () % batch_size = 0) THEN
      AWAIT ALL;
      SET batch_count = batch_count + 1;
    END IF;
    
  END FOR;
  
  -- Wait for any remaining async jobs
  AWAIT ALL;
  
  RETURN 'Launched async extraction for ' || total_forms || ' forms in batches of ' || batch_size;
END;
$$;