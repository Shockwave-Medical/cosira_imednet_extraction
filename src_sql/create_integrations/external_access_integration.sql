-- External access integration for COSIRA iMedNet API
-- References existing secrets from STAGING_PROD.IMEDNET

-- 1. Network rule for iMedNet API endpoints
CREATE OR REPLACE NETWORK RULE cosira_imednet_network_rule
MODE = EGRESS
TYPE = HOST_PORT
VALUE_LIST = ('edc.prod.imednetapi.com:443');

-- 2. External access integration using existing secrets from STAGING_PROD
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION cosira_imednet_integration
ALLOWED_NETWORK_RULES = (cosira_imednet_network_rule)
ALLOWED_AUTHENTICATION_SECRETS = (STAGING_PROD.IMEDNET.IMEDNET_API_SECRET)
ENABLED = TRUE
COMMENT = 'External access for COSIRA iMedNet API calls using existing STAGING_PROD secrets';

-- 3. Grant usage to appropriate role
GRANT USAGE ON INTEGRATION cosira_imednet_integration TO ROLE IT_DEVELOPER;