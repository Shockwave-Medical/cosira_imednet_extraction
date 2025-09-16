-- UDF for single form extraction using imednet SDK
CREATE OR REPLACE FUNCTION extract_form_udf(form_id INTEGER, form_key STRING)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('imednet')
HANDLER = 'extract_form'
EXTERNAL_ACCESS_INTEGRATIONS = (cosira_imednet_integration)
SECRETS = ('cred' = (cosira_imednet_api_key, cosira_imednet_security_key))
AS
$$
from imednet import ImmednetSDK
import json

def extract_form(form_id, form_key):
    """Synchronous extraction of single form using SDK with built-in retry/error handling"""

    try:
        # Use secrets from external access integration
        api_key = _snowflake.get_generic_secret_string('cred')[0]  # x-api-key
        security_key = _snowflake.get_generic_secret_string('cred')[1]  # x-imn-security-key

        # SDK handles all retry/error logic automatically
        sdk = ImmednetSDK(
            api_key=api_key,
            security_key=security_key,
            base_url="https://edc.prod.imednetapi.com"
        )

        # SDK handles pagination automatically
        records = sdk.forms.records.list(
            study_id="COSIRA-II",
            form_key=form_key
        )

        return records  # Return the raw records array

    except Exception as e:
        # SDK handles retries, but we can still catch and return error info
        return {"error": str(e), "form_id": form_id, "form_key": form_key}
$$;