CREATE OR REPLACE TABLE `{{ params.dwh_dataset }}.D_PORT` AS
SELECT DISTINCT
  ID PORT_ID,
  PORT PORT_NAME
FROM
  `{{ params.project_id }}.{{ params.staging_dataset }}.USPORT`