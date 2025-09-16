-- External access integration for COSIRA imednet API (following Snowflake best practices)
-- API credentials configured as secrets in the integration for security and governance

-- 1. Create network rule for imednet API endpoints
CREATE OR REPLACE NETWORK RULE cosira_imednet_network_rule
MODE = EGRESS
TYPE = HOST_PORT
VALUE_LIST = ('edc.prod.imednetapi.com:443');

-- 2. Create secrets for API authentication (configure actual values)
-- NOTE: Replace the SECRET_STRING values below with actual API credentials
CREATE OR REPLACE SECRET cosira_imednet_api_key
TYPE = GENERIC_STRING
SECRET_STRING = 'your_actual_x_api_key_here';  -- Replace with actual x-api-key

CREATE OR REPLACE SECRET cosira_imednet_security_key
TYPE = GENERIC_STRING
SECRET_STRING = 'your_actual_x_imn_security_key_here';  -- Replace with actual x-imn-security-key

-- 3. Create external access integration with secrets
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION cosira_imednet_integration
ALLOWED_NETWORK_RULES = (cosira_imednet_network_rule)
ALLOWED_AUTHENTICATION_SECRETS = (cosira_imednet_api_key, cosira_imednet_security_key)
ENABLED = TRUE
COMMENT = 'External access for COSIRA imednet API calls with integrated authentication';

-- 4. Grant usage to IT_DEVELOPER role
GRANT USAGE ON INTEGRATION cosira_imednet_integration TO ROLE IT_DEVELOPER;
GRANT READ ON SECRET cosira_imednet_api_key TO ROLE IT_DEVELOPER;
GRANT READ ON SECRET cosira_imednet_security_key TO ROLE IT_DEVELOPER;