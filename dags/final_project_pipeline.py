# STEP 1: Libraries needed

from datetime import timedelta, datetime
from airflow import models
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators import dataproc_operator
from airflow.utils import trigger_rule

from airflow.contrib.operators.bigquery_operator import (
    BigQueryOperator,
)
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.contrib.operators.gcs_to_gcs import (
    GoogleCloudStorageToGoogleCloudStorageOperator,
)
from airflow.contrib.operators import bigquery_operator
from airflow.operators.dummy_operator import DummyOperator


# STEP 2:Define a start date
#In this case yesterday
yesterday = datetime(2022, 3, 9)


# Spark references
SPARK_CODE = ('gs://us-central1-final-project-d-3149f6ad-bucket/spark_files/final_project_spark.py')
dataproc_job_name = 'spark_job_dataproc'

# STEP 3: Set default arguments for the DAG
default_dag_args = {
    'start_date': yesterday,
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# STEP 4: Define DAG
# set the DAG name, add a DAG description, define the schedule interval and pass the default arguments defined before
with models.DAG(
    'final_project_pipeline',
    description='DAG for deployment a Dataproc Cluster, and create parquet file from source',
    schedule_interval=timedelta(days=1),
    default_args=default_dag_args
) as dag:

    start_pipeline = DummyOperator(
        task_id = 'start_pipeline'
    )

# dataproc_operator
# Create small dataproc cluster
    create_dataproc = dataproc_operator.DataprocClusterCreateOperator(
        task_id = "create_dataproc",
        project_id="extreme-citadel-343301",
        cluster_name="cluster-final-project",
        num_workers=0,
        zone = "us-central1-a",
        region = "us-central1",
        master_machine_type='n1-standard-4',
        worker_machine_type='n1-standard-4'
    )

    # Run the PySpark job
    csv_to_parquet = dataproc_operator.DataProcPySparkOperator(
        task_id = "csv_to_parquet",
        main = SPARK_CODE,
        cluster_name="cluster-final-project",
        region = "us-central1",
        job_name = dataproc_job_name
    )
    
    load_to_stagging = DummyOperator(
        task_id = 'load_to_stagging'
    )

    load_parquet_to_stagging_weather = GoogleCloudStorageToBigQueryOperator(
        task_id="load_parquet_to_stagging_weather",
        bucket="final_project_digital_skola",
        source_objects=["STAGGING/weather.parquet/part*"],
        destination_project_dataset_table="extreme-citadel-343301:FINAL_PROJECT_STAGGING.WEATHER",
        source_format="PARQUET",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        google_cloud_storage_conn_id="google_cloud_default",
        bigquery_conn_id="bigquery_default"
    )
    
    load_parquet_to_stagging_immigrasi = GoogleCloudStorageToBigQueryOperator(
        task_id="load_parquet_to_stagging_immigrasi",
        bucket="final_project_digital_skola",
        source_objects=["STAGGING/immigrasi.parquet/part*"],
        destination_project_dataset_table="extreme-citadel-343301:FINAL_PROJECT_STAGGING.IMMIGRASI",
        source_format="PARQUET",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        google_cloud_storage_conn_id="google_cloud_default",
        bigquery_conn_id="bigquery_default"
    )  

    load_parquet_to_stagging_airport_code = GoogleCloudStorageToBigQueryOperator(
        task_id="load_parquet_to_stagging_airport_code",
        bucket="final_project_digital_skola",
        source_objects=["STAGGING/airport_codes.parquet/part*"],
        destination_project_dataset_table="extreme-citadel-343301:FINAL_PROJECT_STAGGING.AIRPORT_CODE",
        source_format="PARQUET",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        google_cloud_storage_conn_id="google_cloud_default",
        bigquery_conn_id="bigquery_default"
    )  
    
    load_parquet_to_stagging_usport = GoogleCloudStorageToBigQueryOperator(
        task_id="load_parquet_to_stagging_usport",
        bucket="final_project_digital_skola",
        source_objects=["STAGGING/usport.parquet/part*"],
        destination_project_dataset_table="extreme-citadel-343301:FINAL_PROJECT_STAGGING.USPORT",
        source_format="PARQUET",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        google_cloud_storage_conn_id="google_cloud_default",
        bigquery_conn_id="bigquery_default"
    )

    load_parquet_to_stagging_usstate = GoogleCloudStorageToBigQueryOperator(
        task_id="load_parquet_to_stagging_usstate",
        bucket="final_project_digital_skola",
        source_objects=["STAGGING/usstate.parquet/part*"],
        destination_project_dataset_table="extreme-citadel-343301:FINAL_PROJECT_STAGGING.USSTATE",
        source_format="PARQUET",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        google_cloud_storage_conn_id="google_cloud_default",
        bigquery_conn_id="bigquery_default"
    )

    load_parquet_to_stagging_uscountry = GoogleCloudStorageToBigQueryOperator(
        task_id="load_parquet_to_stagging_uscountry",
        bucket="final_project_digital_skola",
        source_objects=["STAGGING/uscountry.parquet/part*"],
        destination_project_dataset_table="extreme-citadel-343301:FINAL_PROJECT_STAGGING.USCOUNTRY",
        source_format="PARQUET",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        google_cloud_storage_conn_id="google_cloud_default",
        bigquery_conn_id="bigquery_default"
    )
	
    load_parquet_to_stagging_city_demo = GoogleCloudStorageToBigQueryOperator(
        task_id="load_parquet_to_stagging_city_demo",
        bucket="final_project_digital_skola",
        source_objects=["STAGGING/city_demo.parquet/part*"],
        destination_project_dataset_table="extreme-citadel-343301:FINAL_PROJECT_STAGGING.CITY_DEMO",
        source_format="PARQUET",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        google_cloud_storage_conn_id="google_cloud_default",
        bigquery_conn_id="bigquery_default"
    )
    
    import_to_dwh = DummyOperator(
        task_id = 'import_to_dwh'
    )
    
    create_country_data = BigQueryOperator(
        task_id = 'create_country_data',
        use_legacy_sql = False,
        params = {
            'project_id': "extreme-citadel-343301",
            'staging_dataset': "FINAL_PROJECT_STAGGING",
            'dwh_dataset': "FINAL_PROJECT_DWH"
        },
        sql = './SQL/D_COUNTRY.sql'
    )
    
    create_port_data = BigQueryOperator(
        task_id = 'create_port_data',
        use_legacy_sql = False,
        params = {
            'project_id': "extreme-citadel-343301",
            'staging_dataset': "FINAL_PROJECT_STAGGING",
            'dwh_dataset': "FINAL_PROJECT_DWH"
        },
        sql = './SQL/D_PORT.sql'
    )
    
    create_state_data = BigQueryOperator(
        task_id = 'create_state_data',
        use_legacy_sql = False,
        params = {
            'project_id': "extreme-citadel-343301",
            'staging_dataset': "FINAL_PROJECT_STAGGING",
            'dwh_dataset': "FINAL_PROJECT_DWH"
        },
        sql = './SQL/D_STATE.sql'
    )
    
    create_weather_data = BigQueryOperator(
        task_id = 'create_weather_data',
        use_legacy_sql = False,
        params = {
            'project_id': "extreme-citadel-343301",
            'staging_dataset': "FINAL_PROJECT_STAGGING",
            'dwh_dataset': "FINAL_PROJECT_DWH"
        },
        sql = './SQL/D_WEATHER.sql'
    )
     
    create_immigration_data = BigQueryOperator(
        task_id = 'create_immigration_data',
        use_legacy_sql = False,
        params = {
            'project_id': "extreme-citadel-343301",
            'staging_dataset': "FINAL_PROJECT_STAGGING",
            'dwh_dataset': "FINAL_PROJECT_DWH"
        },
        sql = './SQL/F_IMMIGRATION_DATA.sql'
    )


    # Transform, load IMIGRATION DATA AS FACT TABLE
    transform_airport_to_dwh = BigQueryOperator(
        task_id = 'transform_airport_to_dwh',
        use_legacy_sql = False,
        params = {
            'project_id': "extreme-citadel-343301",
            'staging_dataset': "FINAL_PROJECT_STAGGING",
            'dwh_dataset': "FINAL_PROJECT_DWH"
        },
        sql = './SQL/D_AIRPORT.sql'
    )

    # Transform, load IMIGRATION DATA AS FACT TABLE
    transform_city_to_dwh = BigQueryOperator(
        task_id = 'transform_city_to_dwh',
        use_legacy_sql = False,
        params = {
            'project_id': "extreme-citadel-343301",
            'staging_dataset': "FINAL_PROJECT_STAGGING",
            'dwh_dataset': "FINAL_PROJECT_DWH"
        },
        sql = './SQL/D_CITY_DEMO.sql'
    )    
    
    
# dataproc_operator
# Delete Cloud Dataproc cluster.
    delete_dataproc = dataproc_operator.DataprocClusterDeleteOperator(
        task_id= "delete_dataproc",
        project_id= "extreme-citadel-343301",
        region= "us-central1",
		cluster_name="cluster-final-project",
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE
    )
    
    finish_pipeline = DummyOperator(
        task_id = 'finish_pipeline'
	)
# STEP 6: Set DAGs dependencies
# Each task should run after have finished the task before.
    start_pipeline >> create_dataproc >> csv_to_parquet >> load_to_stagging >> [ load_parquet_to_stagging_weather, load_parquet_to_stagging_immigrasi,load_parquet_to_stagging_airport_code, load_parquet_to_stagging_uscountry, load_parquet_to_stagging_usstate, load_parquet_to_stagging_city_demo, load_parquet_to_stagging_usport] >>  import_to_dwh >> [create_country_data, create_port_data, create_state_data, create_weather_data,create_immigration_data, transform_airport_to_dwh, transform_city_to_dwh] >> delete_dataproc >> finish_pipeline