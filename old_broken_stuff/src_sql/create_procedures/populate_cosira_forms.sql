-- Procedure to discover and populate COSIRA forms (following Snowflake external access best practices)
CREATE OR REPLACE PROCEDURE populate_cosira_forms()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'requests', 'pandas')
HANDLER = 'main'
EXTERNAL_ACCESS_INTEGRATIONS = (cosira_imednet_integration)
SECRETS = ('cred' = (cosira_imednet_api_key, cosira_imednet_security_key))
AS
$$
import json
import requests
import pandas as pd
from datetime import datetime

def main(session):
    """
    Discover all COSIRA forms via imednet API and populate control table
    Using secrets from external access integration (Snowflake best practices)
    """

    try:
        # Get API credentials from external access integration secrets
        print("Getting imednet API credentials from external access integration...")
        api_key = _snowflake.get_generic_secret_string('cred')[0]  # x-api-key
        security_key = _snowflake.get_generic_secret_string('cred')[1]  # x-imn-security-key

        # API call to get all forms
        print("Fetching forms from imednet API...")
        url = "https://edc.prod.imednetapi.com/api/v1/edc/studies/COSIRA-II/forms"
        headers = {
            "x-api-key": api_key,
            "x-imn-security-key": security_key,
            "Content-Type": "application/json"
        }
        params = {"size": "500"}  # Get all forms

        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()

        data = response.json()
        forms = data.get('data', [])

        print(f"Found {len(forms)} forms")

        # Prepare data with full API metadata (no DELETE - history table)
        form_records = []
        for form in forms:
            form_records.append({
                'STUDY_KEY': form.get('studyKey'),
                'FORM_ID': form.get('formId'),
                'FORM_KEY': form.get('formKey'),
                'FORM_NAME': form.get('formName', ''),  # No truncation needed
                'FORM_TYPE': form.get('formType'),
                'REVISION': form.get('revision'),
                'DISABLED': form.get('disabled', False),
                'DATE_CREATED': form.get('dateCreated'),
                'DATE_MODIFIED': form.get('dateModified'),
                'RECORD_COUNT': None,  # Will be populated during extraction
                'EXTRACTION_STATUS': 'PENDING',
                'LAST_EXTRACTED': None,
                'ERROR_MESSAGE': None,
                'LOAD_DATE': datetime.now().date()
            })

        # Create DataFrame and write to Snowflake (no overwrite)
        if form_records:
            df = pd.DataFrame(form_records)
            session.write_pandas(
                df,
                table_name='COSIRA_FORMS_CONTROL',
                auto_create_table=False,
                overwrite=False
            )

            print(f"✅ Populated {len(form_records)} forms into COSIRA_FORMS_CONTROL")

            # Summary stats
            summary = df.groupby(['FORM_TYPE', 'DISABLED']).size().to_dict()
            enabled_forms = len(df[~df['DISABLED']])
            enabled_by_type = df[~df['DISABLED']].groupby('FORM_TYPE').size().to_dict()

            print(f"📊 {enabled_forms} total enabled forms ready for extraction")
            print(f"📊 Forms by type: {enabled_by_type}")

            return f"Successfully populated {len(form_records)} forms. {enabled_forms} forms enabled for extraction across all types."
        else:
            return "No forms found to populate"

    except Exception as e:
        error_msg = f"Failed to populate forms: {str(e)}"
        print(f"❌ {error_msg}")

        # Log error to table if possible
        try:
            session.sql(f"""
                INSERT INTO COSIRA_FORMS_STATUS
                (FORM_ID, FORM_KEY, FORM_NAME, FORM_TYPE, STATUS, ERROR_MESSAGE)
                VALUES (-1, 'ERROR', 'DISCOVERY_FAILED', 'ERROR', 'FAILED', '{str(e)[:1000]}')
            """).collect()
        except:
            pass  # Best effort logging

        raise Exception(error_msg)
$$;