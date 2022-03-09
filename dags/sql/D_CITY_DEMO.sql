CREATE OR REPLACE TABLE
  `{{ params.dwh_dataset }}.D_CITY_DEMO` AS
SELECT DISTINCT
  CITY CITY_NAME,
  STATE STATE_NAME,
  STATECODE STATE_ID,
  MEDIANAGE,
  MALEPOPULATION,
  FEMALEPOPULATION,
  TOTALPOPULATION,
  NUMBEROFVETERANS,
  FOREIGNBORN,
  AVERAGEHOUSEHOLDSIZE,
  AVG(
  IF
    (RACE = 'White',
      COUNT,
      NULL)) WHITE_POPULATION,
  AVG(
  IF
    (RACE = 'Black or African-American',
      COUNT,
      NULL)) BLACK_POPULATION,
  AVG(
  IF
    (RACE = 'Asian',
      COUNT,
      NULL)) ASIAN_POPULATION,
  AVG(
  IF
    (RACE = 'Hispanic or Latino',
      COUNT,
      NULL)) LATINO_POPULATION,
  AVG(
  IF
    (RACE = 'American Indian and Alaska Native',
      COUNT,
      NULL)) NATIVE_POPULATION
FROM
  `{{ params.project_id }}.{{ params.staging_dataset }}.CITY_DEMO`
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  9,
  10
