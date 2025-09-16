-- Create Git repository for COSIRA iMedNet extraction
CREATE OR REPLACE GIT REPOSITORY cosira_imednet_repo
  ORIGIN = 'https://github.com/Shockwave-Medical/cosira_imednet_extraction.git'
  API_INTEGRATION = git_api_integration;