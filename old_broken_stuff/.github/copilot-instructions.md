# COSIRA iMedNet Extraction - AI Agent Instructions

## Architecture Overview
This is a Snowflake-based ETL system for extracting clinical trial data from the iMedNet EDC API. The architecture follows a hybrid staging approach with governance through external access integrations.

**Key Components:**
- **External Access Integration** (`create_integrations/`): Governs API calls to `edc.prod.imednetapi.com`
- **Control Tables** (`create_tables/cosira_forms_control_updated.sql`): Tracks form metadata and extraction status
- **Raw Staging** (`create_tables/cosira_raw_staging.sql`): Stores complete JSON responses in VARIANT columns
- **Data Processing**: Python UDFs in `functions/` for SFCS (Snowflake Column Cleaning) patterns
- **Procedures** (`create_procedures/`): API discovery and data population using Snowpark Python

## Critical Patterns

### External API Integration
Always use external access integrations for API calls. Example from `cosira_imednet_access.sql`:
```sql
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION cosira_imednet_integration
ALLOWED_NETWORK_RULES = (cosira_imednet_network_rule)
ENABLED = TRUE;
```
Reference this integration in procedures: `EXTERNAL_ACCESS_INTEGRATIONS = (cosira_imednet_integration)`

### Secrets Management
Follow "Japan DAG pattern" for secrets. Retrieve via UDF calls:
```sql
SELECT STAGING_PROD.IMEDNET.get_imednet_api_secret();
```
Parse JSON response for `x-api-key` and `x-imn-security-key` headers.

### Data Flow Architecture
1. **Discovery**: `populate_cosira_forms()` procedure fetches form metadata via `/api/v1/edc/studies/COSIRA-II/forms`
2. **Control**: Forms stored in `COSIRA_FORMS_CONTROL` with extraction tracking
3. **Raw Staging**: API responses stored as VARIANT in `COSIRA_RAW_STAGING` via SQL INSERT
4. **Processing**: Apply SFCS cleaning functions (`clean_column_name`, `dedupe_columns`) in SQL
5. **Clean Tables**: Auto-create relational tables from VARIANT using LATERAL FLATTEN and SQL transformations

### Extraction Pattern (UDF + Async Procedure)
**UDF Approach**: `extract_form_udf(form_id, form_key)` - synchronous SDK calls with built-in retry/error handling
**Procedure Approach**: Async procedure calls UDFs concurrently using `session.call()` + `collect_nowait()`

**Concurrency Control**:
- Use `ROW_NUMBER() / 8` for concurrency groups (8 forms per group)
- Process groups sequentially, forms within groups concurrently
- `collect_nowait()` for maximum async throughput

**Data Flow**:
1. **UDF**: `extract_form_udf()` → returns VARIANT array of records
2. **Procedure**: Calls UDFs concurrently → `collect_nowait()` results
3. **SQL Processing**: VARIANT staging → auto-create clean tables via LATERAL FLATTEN
4. **Control Updates**: Status tracking per form completion

**External Access Integration**:
- API credentials configured in integration (not retrieved in code)
- UDF uses integration's built-in authentication
- Follows Snowflake external access best practices

### SFCS Column Cleaning
Use Python UDFs for consistent column naming:
- `clean_column_name()`: Removes special chars, replaces spaces with underscores, uppercases
- `dedupe_columns()`: Handles duplicate column names by appending numbers

### Error Handling & Logging
Log extraction status to `COSIRA_FORMS_STATUS` table. Include `RECORD_COUNT`, `STATUS`, `ERROR_MESSAGE` fields. Never delete records - maintain history with `LOAD_DATE` partitions.

### Table Design Patterns
- Primary keys include `LOAD_DATE` for historical tracking
- Use `VARIANT` for flexible JSON storage
- Include `RECORD_COUNT` and `EXTRACTION_STATUS` for operational monitoring
- Add `COMMENT` clauses explaining table purpose and patterns followed

## Deployment Patterns

### SPCS Container Services (Recommended for Complex Workflows)
When using Snowpark Container Services (SPCS) like in `SFCS_Selenium_Clintrak` repo:
- **Containerization**: Use Docker containers for complex processing (Selenium, heavy dependencies)
- **Build Process**: Use Makefiles for registry login, build, and push automation
- **Service Execution**: Run via `EXECUTE JOB SERVICE` with compute pools
- **External Access**: Include `EXTERNAL_ACCESS_INTEGRATIONS` in service specs
- **Logging**: Query `DEVELOPMENT.SPCS.SPCS_LOGS` for container logs

### Stored Procedure Pattern (Current Approach)
For API-based extraction like this repo and `clinicaltrails_forwardjapan_rave_data_fetch`:
- **Python Procedures**: Use Snowpark Python stored procedures for API calls
- **Secrets via UDFs**: Retrieve secrets through dedicated UDF functions
- **Direct Data Loading**: Use `session.write_pandas()` for efficient data insertion
- **Environment Logic**: Support both local development and production execution

### Async Processing for Multiple Endpoints
For processing ~120 endpoints efficiently:
- **Child Job Pattern**: Use Snowflake's asynchronous child jobs (`SYSTEM$WAIT_FOR_CHILD_JOBS`)
- **UDTF with Partitioning**: Create User-Defined Table Functions that partition work across buckets
- **Control Table Driven**: Select unprocessed endpoints from control tables
- **Parallel Execution**: Use `OVER (PARTITION BY bucket_id)` to trigger parallel processing

Example async procedure structure:
```sql
CREATE OR REPLACE PROCEDURE extract_cosira_data()
RETURNS STRING
LANGUAGE SQL
AS
$$
-- Select unprocessed forms and assign to buckets
CREATE OR REPLACE TEMP TABLE forms_to_process AS
SELECT FORM_ID, FORM_KEY, 
       MOD(FORM_ID, 10) as BUCKET_ID  -- 10 parallel workers
FROM COSIRA_FORMS_CONTROL
WHERE EXTRACTION_STATUS != 'SUCCESS';

-- Execute UDTF with partitioning for parallel processing
INSERT INTO COSIRA_RAW_STAGING
SELECT * FROM TABLE(extract_forms_udtf(
  (SELECT ARRAY_AGG(OBJECT_CONSTRUCT(...)) FROM forms_to_process)
)) OVER (PARTITION BY BUCKET_ID);
$$;
```

## Development Workflow
- Deploy SQL objects in order: integrations → tables → functions → procedures
- Test API connectivity using external access integration
- Validate data flows from control → raw staging → processed tables
- Monitor extraction status via control and status tables

## Naming Conventions
- Table names: `COSIRA_*` prefix
- Integration names: `cosira_*_integration`
- Function names: Descriptive lowercase with underscores
- Follow existing patterns for consistency with broader iMedNet ecosystem

## Integration Points
- **iMedNet SDK**: Consider using `imednet-python-sdk` for simplified async API calls
- **GitHub Integration**: Use for CI/CD and automated deployments
- **External Access**: Required for all outbound API calls to `edc.prod.imednetapi.com`</content>
<parameter name="filePath">/Users/aloksubbarao/svm/cosira_imednet_extraction/.github/copilot-instructions.md