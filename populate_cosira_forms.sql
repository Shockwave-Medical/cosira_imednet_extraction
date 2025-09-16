-- Procedure to discover and populate COSIRA forms from iMedNet API
-- Uses external access integration for proper Snowflake governance

CREATE OR REPLACE PROCEDURE populate_cosira_forms()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'requests', 'pandas')
HANDLER = 'main'
EXTERNAL_ACCESS_INTEGRATIONS = (cosira_imednet_integration)
SECRETS = ('cred' = STAGING_PROD.IMEDNET.IMEDNET_API_SECRET)
AS
$$
import json
import requests
import pandas as pd
from datetime import datetime

def main(session):
    """
    Discover all COSIRA forms via iMedNet API and populate control table
    Following Snowflake external access best practices
    """
    
    try:
        # Get API credentials from external access integration using existing STAGING_PROD secrets
        print("🔑 Getting iMedNet API credentials from external access integration...")
        secret_json = _snowflake.get_generic_secret_string('cred')
        secrets = json.loads(secret_json)
        api_key = secrets["x-api-key"]
        security_key = secrets["x-imn-security-key"]
        
        # API call to get all forms for COSIRA-II study
        print("📡 Fetching forms from iMedNet API...")
        url = "https://edc.prod.imednetapi.com/api/v1/edc/studies/COSIRA-II/forms"
        headers = {
            "x-api-key": api_key,
            "x-imn-security-key": security_key,
            "Content-Type": "application/json"
        }
        params = {"size": "500"}  # Get all forms in single call
        
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        
        data = response.json()
        forms = data.get('data', [])
        
        print(f"✅ Found {len(forms)} forms")
        
        # Prepare data with complete API metadata
        form_records = []
        load_date = datetime.now().date()
        
        for form in forms:
            form_records.append({
                'STUDY_KEY': form.get('studyKey'),
                'FORM_ID': form.get('formId'),
                'FORM_KEY': form.get('formKey'),
                'FORM_NAME': form.get('formName', ''),
                'FORM_TYPE': form.get('formType'),
                'REVISION': form.get('revision'),
                'DISABLED': form.get('disabled', False),
                'DATE_CREATED': form.get('dateCreated'),
                'DATE_MODIFIED': form.get('dateModified'),
                'RECORD_COUNT': None,  # Will be populated during extraction
                'EXTRACTION_STATUS': 'PENDING',
                'LAST_EXTRACTED': None,
                'ERROR_MESSAGE': None,
                'LOAD_DATE': load_date
            })
        
        # Write to Snowflake control table
        if form_records:
            df = pd.DataFrame(form_records)
            session.write_pandas(
                df,
                table_name='COSIRA_FORMS_CONTROL',
                auto_create_table=False,
                overwrite=False  # Append mode for history tracking
            )
            
            print(f"✅ Populated {len(form_records)} forms into COSIRA_FORMS_CONTROL")
            
            # Generate summary statistics
            enabled_forms = len(df[~df['DISABLED']])
            disabled_forms = len(df[df['DISABLED']])
            forms_by_type = df[~df['DISABLED']].groupby('FORM_TYPE').size().to_dict()
            
            print(f"📊 Summary: {enabled_forms} enabled, {disabled_forms} disabled forms")
            print(f"📊 Enabled forms by type: {forms_by_type}")
            
            return f"Successfully populated {len(form_records)} forms ({enabled_forms} enabled for extraction)"
        else:
            return "No forms found to populate"
            
    except Exception as e:
        error_msg = f"Failed to populate forms: {str(e)}"
        print(f"❌ {error_msg}")
        raise Exception(error_msg)
$$;