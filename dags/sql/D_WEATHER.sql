CREATE OR REPLACE TABLE `{{ params.dwh_dataset }}.D_WEATHER` AS
SELECT  
  CAST(left(w.dt,4) AS NUMERIC) YEAR,
  w.City CITY,
  w.Country COUNTRY,
  w.Latitude LATITUDE,
  w.Longitude LONGITUDE,
  w.AverageTemperature AVG_TEMPERATURE
FROM `extreme-citadel-343301.FINAL_PROJECT_STAGGING.WEATHER` w
  WHERE w.Country='United States'