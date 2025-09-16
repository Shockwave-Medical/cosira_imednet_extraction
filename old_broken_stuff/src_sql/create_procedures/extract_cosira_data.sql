-- Simple COSIRA extraction: SDK async → VARIANT staging → auto-create clean tables-- Async procedure calling UDFs for high-concurrency extraction-- SQL-centric COSIRA extraction: Python for API calls, SQL for data processing-- Simple COSIRA extraction: SDK async → VARIANT staging → auto-create clean tables

CREATE OR REPLACE PROCEDURE extract_cosira_data()

RETURNS STRINGCREATE OR REPLACE PROCEDURE extract_cosira_data()

LANGUAGE PYTHON

RUNTIME_VERSION = '3.11'RETURNS STRINGCREATE OR REPLACE PROCEDURE extract_cosira_data()CREATE OR REPLACE PROCEDURE extract_cosira_data()

PACKAGES = ('snowflake-snowpark-python', 'pandas', 'asyncio', 'imednet')

HANDLER = 'main'LANGUAGE PYTHON

EXTERNAL_ACCESS_INTEGRATIONS = (cosira_imednet_integration)

SECRETS = ('cred' = (cosira_imednet_api_key, cosira_imednet_security_key))RUNTIME_VERSION = '3.11'RETURNS STRINGRETURNS STRING

AS

$$PACKAGES = ('snowflake-snowpark-python')

import json

import asyncioHANDLER = 'main'LANGUAGE PYTHONLANGUAGE PYTHON

import pandas as pd

from datetime import datetimeEXTERNAL_ACCESS_INTEGRATIONS = (cosira_imednet_integration)



async def extract_single_form(session, api_key, security_key, form_info):ASRUNTIME_VERSION = '3.11'RUNTIME_VERSION = '3.11'

    """Extract single form: SDK → VARIANT → auto-create → update control"""

    form_id = form_info['FORM_ID']$$

    form_key = form_info['FORM_KEY']

    form_name = form_info['FORM_NAME']import jsonPACKAGES = ('snowflake-snowpark-python', 'asyncio', 'imednet')PACKAGES = ('snowflake-snowpark-python', 'pandas', 'asyncio', 'imednet')



    try:from datetime import datetime

        # Use imednet SDK (handles retries, pagination, errors)

        from imednet import AsyncImednetSDKHANDLER = 'main'HANDLER = 'main'



        async with AsyncImednetSDK(def main(session):

            api_key=api_key,

            security_key=security_key,    """Async procedure: Call extract_form_udf concurrently on pending forms using collect_nowait"""EXTERNAL_ACCESS_INTEGRATIONS = (cosira_imednet_integration)EXTERNAL_ACCESS_INTEGRATIONS = (cosira_imednet_integration)

            base_url="https://edc.prod.imednetapi.com"

        ) as sdk:

            # SDK handles async, retries, pagination

            records = await sdk.forms.records.list(    try:ASAS

                study_id="COSIRA-II",

                form_key=form_key        # Get pending forms with concurrency groups (8 per group for controlled parallelism)

            )

        pending_forms = session.sql("""$$$$

            print(f"✅ {form_name}: {len(records)} records")

            SELECT FORM_ID, FORM_KEY, FORM_NAME,

            if records:

                # 1. VARIANT staging (audit trail)                   ROW_NUMBER() OVER (ORDER BY FORM_TYPE, FORM_NAME) as rn,import jsonimport json

                staging_data = pd.DataFrame([{

                    'LOAD_DATE': datetime.now().date(),                   CEIL(ROW_NUMBER() OVER (ORDER BY FORM_TYPE, FORM_NAME) / 8.0) as concurrency_group

                    'FORM_ID': form_id,

                    'FORM_KEY': form_key,            FROM COSIRA_FORMS_CONTROLimport asyncioimport asyncio

                    'RAW_RECORDS': records,

                    'RECORD_COUNT': len(records)            WHERE DISABLED = FALSE

                }])

            AND LOAD_DATE = CURRENT_DATE()from datetime import datetimeimport pandas as pd

                session.write_pandas(

                    staging_data,            AND (EXTRACTION_STATUS != 'SUCCESS' OR EXTRACTION_STATUS IS NULL)

                    table_name='COSIRA_RAW_STAGING',

                    auto_create_table=False,            ORDER BY FORM_TYPE, FORM_NAMEfrom datetime import datetime

                    overwrite=False

                )        """).collect()



                # 2. Auto-create clean table (Japan pattern)async def extract_form_data(api_key, security_key, form_id, form_key, form_name):

                records_df = pd.json_normalize(records)

        if len(pending_forms) == 0:

                # Clean columns with SQL function

                clean_columns = []            session.sql("""    """Extract single form data using SDK - return raw JSON for SQL processing"""async def extract_single_form(session, api_key, security_key, form_info):

                for col in records_df.columns:

                    clean_result = session.sql(f"SELECT clean_column_name('{col}')").collect()                INSERT INTO TASK_STATUS (TASK_ID, STATUS, TIMESTAMP)

                    clean_columns.append(clean_result[0][0])

                VALUES ('COSIRA_EXTRACT', 'COMPLETED_NO_WORK', CURRENT_TIMESTAMP())    try:    """Extract single form: SDK fetch → VARIANT → auto-create → update control"""

                records_df.columns = clean_columns

            """).collect()

                # Auto-create clean table

                clean_table_name = f"COSIRA_{form_key.replace('-', '_').upper()}"            return "No forms to extract"        from imednet import AsyncImednetSDK    form_id = form_info['FORM_ID']

                session.write_pandas(

                    records_df,

                    table_name=clean_table_name,

                    auto_create_table=True,        print(f"Processing {len(pending_forms)} forms in {pending_forms[-1]['CONCURRENCY_GROUP']} concurrency groups")    form_key = form_info['FORM_KEY']

                    overwrite=True

                )



                print(f"📊 Created {clean_table_name}")        # Process each concurrency group sequentially to control total parallelism        async with AsyncImednetSDK(    form_name = form_info['FORM_NAME']



            # Update control table        total_success = 0

            session.sql(f"""

                UPDATE COSIRA_FORMS_CONTROL        total_failed = 0            api_key=api_key,

                SET RECORD_COUNT = {len(records)},

                    EXTRACTION_STATUS = 'SUCCESS',

                    LAST_EXTRACTED = CURRENT_TIMESTAMP(),

                    ERROR_MESSAGE = NULL        for group_num in range(1, pending_forms[-1]['CONCURRENCY_GROUP'] + 1):            security_key=security_key,    try:

                WHERE FORM_ID = {form_id} AND LOAD_DATE = CURRENT_DATE()

            """).collect()            group_forms = [row for row in pending_forms if row['CONCURRENCY_GROUP'] == group_num]



            return 'SUCCESS'            print(f"Processing concurrency group {group_num} with {len(group_forms)} forms")            base_url="https://edc.prod.imednetapi.com"        # Use imednet SDK (handles async, pagination, retries)



    except Exception as e:

        print(f"❌ {form_name}: {str(e)}")

            # Call UDFs concurrently for this group using session.call        ) as sdk:        from imednet import AsyncImednetSDK

        # Update control table with error

        error_msg = str(e).replace("'", "''")[:1000]            udf_futures = []

        session.sql(f"""

            UPDATE COSIRA_FORMS_CONTROL            for form in group_forms:            records = await sdk.forms.records.list(

            SET EXTRACTION_STATUS = 'FAILED',

                LAST_EXTRACTED = CURRENT_TIMESTAMP(),                # Call UDF asynchronously - returns future

                ERROR_MESSAGE = '{error_msg}'

            WHERE FORM_ID = {form_id} AND LOAD_DATE = CURRENT_DATE()                udf_future = session.call("extract_form_udf", form['FORM_ID'], form['FORM_KEY'])                study_id="COSIRA-II",        async with AsyncImednetSDK(

        """).collect()

                udf_futures.append((form, udf_future))

        return 'FAILED'

                form_key=form_key            api_key=api_key,

async def main(session):

    """Main: get pending forms → async extract → task status"""            # Collect results asynchronously with collect_nowait for maximum throughput



    try:            group_success = 0            )            security_key=security_key,

        # Get API credentials from external access integration

        api_key = _snowflake.get_generic_secret_string('cred')[0]            group_failed = 0

        security_key = _snowflake.get_generic_secret_string('cred')[1]

            base_url="https://edc.prod.imednetapi.com"

        # Get pending forms (control table filter)

        forms_df = session.sql("""            for form, udf_future in udf_futures:

            SELECT FORM_ID, FORM_KEY, FORM_NAME, FORM_TYPE

            FROM COSIRA_FORMS_CONTROL                try:            print(f"✅ {form_name}: {len(records)} records")        ) as sdk:

            WHERE DISABLED = FALSE

            AND LOAD_DATE = CURRENT_DATE()                    # Use collect_nowait for async collection

            AND (EXTRACTION_STATUS != 'SUCCESS' OR EXTRACTION_STATUS IS NULL)

            ORDER BY FORM_TYPE, FORM_NAME                    result = udf_future.collect_nowait()            return {            # SDK handles everything: async, pagination, error handling

        """).to_pandas()



        if len(forms_df) == 0:

            session.sql("""                    # Check if UDF returned error                'form_id': form_id,            records = await sdk.forms.records.list(

                INSERT INTO TASK_STATUS (TASK_ID, STATUS, TIMESTAMP)

                VALUES ('COSIRA_EXTRACT', 'COMPLETED_NO_WORK', CURRENT_TIMESTAMP())                    if isinstance(result, dict) and 'error' in result:

            """).collect()

            return "No forms to extract"                        # UDF failed - update control table                'form_key': form_key,                study_id="COSIRA-II",



        print(f"Extracting {len(forms_df)} forms")                        error_msg = result['error'].replace("'", "''")[:1000]



        # Simple async gather (SDK handles concurrency internally)                        session.sql(f"""                'records': records,                form_key=form_key

        forms_list = forms_df.to_dict('records')

        tasks = [extract_single_form(session, api_key, security_key, form) for form in forms_list]                            UPDATE COSIRA_FORMS_CONTROL

        results = await asyncio.gather(*tasks, return_exceptions=True)

                            SET RECORD_COUNT = 0,                'record_count': len(records),            )

        # Task status (SQL will show counts)

        success_count = sum(1 for r in results if r == 'SUCCESS')                                EXTRACTION_STATUS = 'FAILED',

        failed_count = len(results) - success_count

                                LAST_EXTRACTED = CURRENT_TIMESTAMP(),                'status': 'SUCCESS'

        task_status = 'COMPLETED' if failed_count == 0 else 'PARTIAL_SUCCESS'

        session.sql(f"""                                ERROR_MESSAGE = '{error_msg}'

            INSERT INTO TASK_STATUS (TASK_ID, STATUS, TIMESTAMP)

            VALUES ('COSIRA_EXTRACT', '{task_status}', CURRENT_TIMESTAMP())                            WHERE FORM_ID = {form['FORM_ID']}            }            print(f"✅ {form_name}: {len(records)} records")

        """).collect()

                            AND LOAD_DATE = CURRENT_DATE()

        return f"Extracted {success_count} forms successfully, {failed_count} failed"

                        """).collect()

    except Exception as e:

        session.sql("""                        group_failed += 1

            INSERT INTO TASK_STATUS (TASK_ID, STATUS, TIMESTAMP)

            VALUES ('COSIRA_EXTRACT', 'FAILED', CURRENT_TIMESTAMP())                        print(f"❌ {form['FORM_NAME']}: {result['error']}")    except Exception as e:            if records:

        """).collect()

        raise Exception(f"Extraction failed: {str(e)}")                        continue

$$;
        print(f"❌ {form_name}: {str(e)}")                # 1. VARIANT staging (audit trail)

                    # UDF succeeded - process the records

                    if result and len(result) > 0:        return {                staging_data = pd.DataFrame([{

                        # Insert to variant staging

                        records_json = json.dumps(result)            'form_id': form_id,                    'LOAD_DATE': datetime.now().date(),

                        session.sql(f"""

                            INSERT INTO COSIRA_RAW_STAGING (LOAD_DATE, FORM_ID, FORM_KEY, RAW_RECORDS, RECORD_COUNT)            'form_key': form_key,                    'FORM_ID': form_id,

                            SELECT

                                CURRENT_DATE(),            'records': [],                    'FORM_KEY': form_key,

                                {form['FORM_ID']},

                                '{form['FORM_KEY']}',            'record_count': 0,                    'RAW_RECORDS': records,

                                PARSE_JSON('{records_json}'),

                                ARRAY_SIZE(PARSE_JSON('{records_json}'))            'status': 'FAILED',                    'RECORD_COUNT': len(records)

                        """).collect()

            'error': str(e)                }])

                        # Auto-create clean table from variant data

                        clean_table_name = f"COSIRA_{form['FORM_KEY'].replace('-', '_').upper()}"        }

                        session.sql(f"""

                            CREATE OR REPLACE TABLE {clean_table_name} AS                session.write_pandas(

                            SELECT

                                LOAD_DATE,async def main(session):                    staging_data,

                                FORM_ID,

                                FORM_KEY,    """SQL-centric main: Python fetches data, SQL processes everything else"""                    table_name='COSIRA_RAW_STAGING',

                                clean_column_name(key) as COLUMN_NAME,

                                value                    auto_create_table=False,

                            FROM COSIRA_RAW_STAGING,

                            LATERAL FLATTEN(input => RAW_RECORDS)    try:                    overwrite=False

                            WHERE FORM_KEY = '{form['FORM_KEY']}'

                            AND LOAD_DATE = CURRENT_DATE()        # Get API secrets                )

                        """).collect()

        secrets_result = session.sql('SELECT STAGING_PROD.IMEDNET.get_imednet_api_secret();').collect()

                        # Update control table success

                        record_count = len(result)        secrets = json.loads(secrets_result[0][0])                # 2. Auto-create clean table (Japan pattern)

                        session.sql(f"""

                            UPDATE COSIRA_FORMS_CONTROL                records_df = pd.json_normalize(records)

                            SET RECORD_COUNT = {record_count},

                                EXTRACTION_STATUS = 'SUCCESS',        api_key = secrets["x-api-key"]

                                LAST_EXTRACTED = CURRENT_TIMESTAMP(),

                                ERROR_MESSAGE = NULL        security_key = secrets["x-imn-security-key"]                # Clean columns using SQL function

                            WHERE FORM_ID = {form['FORM_ID']}

                            AND LOAD_DATE = CURRENT_DATE()                clean_columns = []

                        """).collect()

        # Get pending forms - pure SQL                for col in records_df.columns:

                        group_success += 1

                        print(f"✅ {form['FORM_NAME']}: {record_count} records")        pending_forms = session.sql("""                    clean_result = session.sql(f"SELECT clean_column_name('{col}')").collect()



                    else:            SELECT FORM_ID, FORM_KEY, FORM_NAME                    clean_columns.append(clean_result[0][0])

                        # UDF returned empty data

                        session.sql(f"""            FROM COSIRA_FORMS_CONTROL

                            UPDATE COSIRA_FORMS_CONTROL

                            SET RECORD_COUNT = 0,            WHERE DISABLED = FALSE                records_df.columns = clean_columns

                                EXTRACTION_STATUS = 'SUCCESS',

                                LAST_EXTRACTED = CURRENT_TIMESTAMP(),            AND LOAD_DATE = CURRENT_DATE()

                                ERROR_MESSAGE = 'No records found'

                            WHERE FORM_ID = {form['FORM_ID']}            AND (EXTRACTION_STATUS != 'SUCCESS' OR EXTRACTION_STATUS IS NULL)                # Auto-create clean table

                            AND LOAD_DATE = CURRENT_DATE()

                        """).collect()            ORDER BY FORM_TYPE, FORM_NAME                clean_table_name = f"COSIRA_{form_key.replace('-', '_').upper()}"

                        group_success += 1

                        print(f"✅ {form['FORM_NAME']}: No records")        """).collect()                session.write_pandas(



                except Exception as e:                    records_df,

                    # UDF call failed or collect_nowait failed

                    error_msg = str(e).replace("'", "''")[:1000]        if len(pending_forms) == 0:                    table_name=clean_table_name,

                    session.sql(f"""

                        UPDATE COSIRA_FORMS_CONTROL            session.sql("""                    auto_create_table=True,

                        SET RECORD_COUNT = 0,

                            EXTRACTION_STATUS = 'FAILED',                INSERT INTO TASK_STATUS (TASK_ID, STATUS, TIMESTAMP)                    overwrite=True

                            LAST_EXTRACTED = CURRENT_TIMESTAMP(),

                            ERROR_MESSAGE = '{error_msg}'                VALUES ('COSIRA_EXTRACT', 'COMPLETED_NO_WORK', CURRENT_TIMESTAMP())                )

                        WHERE FORM_ID = {form['FORM_ID']}

                        AND LOAD_DATE = CURRENT_DATE()            """).collect()

                    """).collect()

                    group_failed += 1            return "No forms to extract"                print(f"📊 Created {clean_table_name}")

                    print(f"❌ {form['FORM_NAME']}: {str(e)}")



            total_success += group_success

            total_failed += group_failed        print(f"Extracting {len(pending_forms)} forms")            # Update control table

            print(f"Group {group_num}: {group_success} success, {group_failed} failed")

            session.sql(f"""

        # Final task status

        task_status = 'COMPLETED' if total_failed == 0 else 'PARTIAL_SUCCESS'        # Async extract all forms                UPDATE COSIRA_FORMS_CONTROL

        session.sql(f"""

            INSERT INTO TASK_STATUS (TASK_ID, STATUS, TIMESTAMP)        semaphore = asyncio.Semaphore(15)  # Conservative concurrency                SET RECORD_COUNT = {len(records)},

            VALUES ('COSIRA_EXTRACT', '{task_status}', CURRENT_TIMESTAMP())

        """).collect()                    EXTRACTION_STATUS = 'SUCCESS',



        return f"Processed {total_success + total_failed} forms: {total_success} successful, {total_failed} failed"        async def extract_with_limit(form_row):                    LAST_EXTRACTED = CURRENT_TIMESTAMP(),



    except Exception as e:            async with semaphore:                    ERROR_MESSAGE = NULL

        session.sql("""

            INSERT INTO TASK_STATUS (TASK_ID, STATUS, TIMESTAMP)                return await extract_form_data(                WHERE FORM_ID = {form_id} AND LOAD_DATE = CURRENT_DATE()

            VALUES ('COSIRA_EXTRACT', 'FAILED', CURRENT_TIMESTAMP())

        """).collect()                    api_key, security_key,            """).collect()

        raise Exception(f"Extraction failed: {str(e)}")

$$;                    form_row['FORM_ID'], form_row['FORM_KEY'], form_row['FORM_NAME']

                )            return 'SUCCESS'



        # Convert to list for async processing    except Exception as e:

        forms_list = [dict(row) for row in pending_forms]        print(f"❌ {form_name}: {str(e)}")

        results = await asyncio.gather(*[extract_with_limit(form) for form in forms_list])

        # Update control table with error

        # Process results - SQL handles the heavy lifting        error_msg = str(e).replace("'", "''")[:1000]

        load_date = datetime.now().date()        session.sql(f"""

            UPDATE COSIRA_FORMS_CONTROL

        for result in results:            SET EXTRACTION_STATUS = 'FAILED',

            if result['status'] == 'SUCCESS' and result['records']:                LAST_EXTRACTED = CURRENT_TIMESTAMP(),

                # SQL: Insert to VARIANT staging                ERROR_MESSAGE = '{error_msg}'

                records_json = json.dumps(result['records'])            WHERE FORM_ID = {form_id} AND LOAD_DATE = CURRENT_DATE()

                session.sql(f"""        """).collect()

                    INSERT INTO COSIRA_RAW_STAGING (LOAD_DATE, FORM_ID, FORM_KEY, RAW_RECORDS, RECORD_COUNT)

                    SELECT        return 'FAILED'

                        '{load_date}',

                        {result['form_id']},async def main(session):

                        '{result['form_key']}',    """Main: get pending forms → async gather → task status"""

                        PARSE_JSON('{records_json}'),

                        {result['record_count']}    try:

                """).collect()        # Get API secrets

        secrets_result = session.sql('SELECT STAGING_PROD.IMEDNET.get_imednet_api_secret();').collect()

                # SQL: Create clean table from VARIANT data        secrets = json.loads(secrets_result[0][0])

                clean_table_name = f"COSIRA_{result['form_key'].replace('-', '_').upper()}"

                session.sql(f"""        api_key = secrets["x-api-key"]

                    CREATE OR REPLACE TABLE {clean_table_name} AS        security_key = secrets["x-imn-security-key"]

                    SELECT

                        LOAD_DATE,        # Get pending forms (control table filter)

                        FORM_ID,        forms_df = session.sql("""

                        FORM_KEY,            SELECT FORM_ID, FORM_KEY, FORM_NAME, FORM_TYPE

                        {result['form_id']} as FORM_ID_STATIC,            FROM COSIRA_FORMS_CONTROL

                        clean_column_name(key) as COLUMN_NAME,            WHERE DISABLED = FALSE

                        value            AND LOAD_DATE = CURRENT_DATE()

                    FROM COSIRA_RAW_STAGING,            AND (EXTRACTION_STATUS != 'SUCCESS' OR EXTRACTION_STATUS IS NULL)

                    LATERAL FLATTEN(input => RAW_RECORDS)            ORDER BY FORM_TYPE, FORM_NAME

                    WHERE FORM_KEY = '{result['form_key']}'        """).to_pandas()

                    AND LOAD_DATE = '{load_date}'

                """).collect()        if len(forms_df) == 0:

            session.sql("""

                print(f"📊 Created {clean_table_name}")                INSERT INTO TASK_STATUS (TASK_ID, STATUS, TIMESTAMP)

                VALUES ('COSIRA_EXTRACT', 'COMPLETED_NO_WORK', CURRENT_TIMESTAMP())

            # SQL: Update control table status            """).collect()

            status = result['status']            return "No forms to extract"

            record_count = result['record_count']

            error_msg = result.get('error', '').replace("'", "''")[:1000] if result.get('error') else 'NULL'        print(f"Extracting {len(forms_df)} forms")



            session.sql(f"""        # Simple async gather (SDK handles concurrency internally)

                UPDATE COSIRA_FORMS_CONTROL        forms_list = forms_df.to_dict('records')

                SET RECORD_COUNT = {record_count},        semaphore = asyncio.Semaphore(20)  # Conservative concurrency limit

                    EXTRACTION_STATUS = '{status}',

                    LAST_EXTRACTED = CURRENT_TIMESTAMP(),        async def extract_with_semaphore(form):

                    ERROR_MESSAGE = {'NULL' if error_msg == 'NULL' else f"'{error_msg}'"}            async with semaphore:

                WHERE FORM_ID = {result['form_id']}                return await extract_single_form(session, api_key, security_key, form)

                AND LOAD_DATE = CURRENT_DATE()

            """).collect()        results = await asyncio.gather(*[extract_with_semaphore(f) for f in forms_list])



        # SQL: Get final counts and task status        # Get final results from control table (SQL not Python)

        final_stats = session.sql("""        results_df = session.sql("""

            SELECT            SELECT EXTRACTION_STATUS, COUNT(*) as cnt

                SUM(CASE WHEN EXTRACTION_STATUS = 'SUCCESS' THEN 1 ELSE 0 END) as success_count,            FROM COSIRA_FORMS_CONTROL

                SUM(CASE WHEN EXTRACTION_STATUS = 'FAILED' THEN 1 ELSE 0 END) as failed_count            WHERE LOAD_DATE = CURRENT_DATE() AND DISABLED = FALSE

            FROM COSIRA_FORMS_CONTROL            GROUP BY EXTRACTION_STATUS

            WHERE LOAD_DATE = CURRENT_DATE()        """).to_pandas()

            AND DISABLED = FALSE

        """).collect()        success_count = results_df[results_df['EXTRACTION_STATUS'] == 'SUCCESS']['cnt'].sum() if 'SUCCESS' in results_df['EXTRACTION_STATUS'].values else 0

        failed_count = results_df[results_df['EXTRACTION_STATUS'] == 'FAILED']['cnt'].sum() if 'FAILED' in results_df['EXTRACTION_STATUS'].values else 0

        success_count = final_stats[0]['SUCCESS_COUNT'] or 0

        failed_count = final_stats[0]['FAILED_COUNT'] or 0        # Single task status

        task_status = 'COMPLETED' if failed_count == 0 else 'PARTIAL_SUCCESS'

        task_status = 'COMPLETED' if failed_count == 0 else 'PARTIAL_SUCCESS'        session.sql(f"""

        session.sql(f"""            INSERT INTO TASK_STATUS (TASK_ID, STATUS, TIMESTAMP)

            INSERT INTO TASK_STATUS (TASK_ID, STATUS, TIMESTAMP)            VALUES ('COSIRA_EXTRACT', '{task_status}', CURRENT_TIMESTAMP())

            VALUES ('COSIRA_EXTRACT', '{task_status}', CURRENT_TIMESTAMP())        """).collect()

        """).collect()

        return f"Extracted {success_count} forms successfully, {failed_count} failed"

        return f"Extracted {success_count} forms successfully, {failed_count} failed"

    except Exception as e:

    except Exception as e:        session.sql("""

        session.sql("""            INSERT INTO TASK_STATUS (TASK_ID, STATUS, TIMESTAMP)

            INSERT INTO TASK_STATUS (TASK_ID, STATUS, TIMESTAMP)            VALUES ('COSIRA_EXTRACT', 'FAILED', CURRENT_TIMESTAMP())

            VALUES ('COSIRA_EXTRACT', 'FAILED', CURRENT_TIMESTAMP())        """).collect()

        """).collect()        raise Exception(f"Extraction failed: {str(e)}")

        raise Exception(f"Extraction failed: {str(e)}")$$;
$$;