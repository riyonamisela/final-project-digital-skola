#transformation.py
from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("My PySpark code") \
    .getOrCreate()

df_usstate = spark.read.options(header='true', inferSchema='true').csv('gs://final_project_digital_skola/INPUT/USSTATE.csv')
df_usstate.printSchema()
df_usstate.write.mode("Overwrite").parquet('gs://final_project_digital_skola/STAGGING/usstate.parquet')

df_uscountry = spark.read.options(header='true', inferSchema='true').csv('gs://final_project_digital_skola/INPUT/UScountry.csv')
df_uscountry.printSchema()
df_uscountry.write.mode("Overwrite").parquet('gs://final_project_digital_skola/STAGGING/uscountry.parquet')

df_weather = spark.read.options(header='true', inferSchema='true').csv('gs://final_project_digital_skola/INPUT/GlobalLandTemperaturesByCity.csv')
df_weather.printSchema()
df_weather.write.mode("Overwrite").parquet('gs://final_project_digital_skola/STAGGING/weather.parquet')

df_immigrasi = spark.read.options(header='true', inferSchema='true').csv('gs://final_project_digital_skola/INPUT/immigration_data_sample.csv')
df_immigrasi.printSchema()
df_immigrasi.write.mode("Overwrite").parquet('gs://final_project_digital_skola/STAGGING/immigrasi.parquet')

df_usport = spark.read.options(header='true', inferSchema='true').csv('gs://final_project_digital_skola/INPUT/USPORT.csv')
df_usport.printSchema()
df_usport.write.mode("Overwrite").parquet('gs://final_project_digital_skola/STAGGING/usport.parquet')

df_city_demo = spark.read.options(header='true', inferSchema='true').csv('gs://final_project_digital_skola/INPUT/us_cities_demographics.csv')
df_city_demo.printSchema()
df_city_demo.write.mode("Overwrite").parquet('gs://final_project_digital_skola/STAGGING/city_demo.parquet')

df_airport_code = spark.read.options(header='true', inferSchema='true').csv('gs://final_project_digital_skola/INPUT/airport_codes_csv.csv')
df_airport_code.printSchema()
df_airport_code.write.mode("Overwrite").parquet('gs://final_project_digital_skola/STAGGING/airport_codes.parquet')