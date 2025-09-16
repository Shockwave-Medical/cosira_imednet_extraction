-- Single form extraction UDF using iMedNet SDK from our GitHub repository
CREATE OR REPLACE FUNCTION extract_single_form_udf(form_id INTEGER, form_key STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
IMPORTS = ('@COSIRA_IMEDNET_REPO/branches/main/src/imednet/')
HANDLER = 'extract_form'
EXTERNAL_ACCESS_INTEGRATIONS = (cosira_imednet_integration)
SECRETS = ('cred' = STAGING_PROD.IMEDNET.IMEDNET_API_SECRET)
AS
$$
import json
import _snowflake
from imednet import ImednetSDK

def extract_form(form_id, form_key):
    # Get API credentials
    secret_json = _snowflake.get_generic_secret_string('cred')
    secrets = json.loads(secret_json)
    
    try:
        # Initialize SDK
        sdk = ImednetSDK(
            api_key=secrets["x-api-key"],
            security_key=secrets["x-imn-security-key"],
            base_url="https://edc.prod.imednetapi.com"
        )
        
        # Extract form records using formId
        records = sdk.records.list(
            study="COSIRA-II",
            filter=f"formId=={form_id} and deleted!='TRUE'",
            size=500
        )
        
        return records
        
    except Exception as e:
        return []
$$;