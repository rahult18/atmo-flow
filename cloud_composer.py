from airflow import DAG
from airflow.providers.google.cloud.operators.functions import CloudFunctionInvokeFunctionOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator
)
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.models import Variable
import os
import json

# Load environment variables
def load_env_vars():
    return {
        'PROJECT_ID': Variable.get('PROJECT_ID'),
        'REGION': Variable.get('REGION'),
        'ZONE': Variable.get('ZONE'),
        'LATITUDE': float(Variable.get('LATITUDE')),
        'LONGITUDE': float(Variable.get('LONGITUDE')),
        'BUCKET_NAME': Variable.get('BUCKET_NAME'),
        'TEMP_BUCKET': Variable.get('TEMP_BUCKET'),
        'AIR_QUALITY_PREFIX': Variable.get('AIR_QUALITY_PREFIX'),
        'WEATHER_PREFIX': Variable.get('WEATHER_PREFIX'),
        'AIR_QUALITY_TOPIC': Variable.get('AIR_QUALITY_TOPIC'),
        'WEATHER_TOPIC': Variable.get('WEATHER_TOPIC'),
        'DEAD_LETTER_TOPIC': Variable.get('DEAD_LETTER_TOPIC'),
        'DATASET_ID': Variable.get('DATASET_ID'),
    }

env = load_env_vars()

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
}

# Cluster configuration
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
    "software_config": {
        "image_version": "2.0",
        "optional_components": ["JUPYTER"],
        "properties": {
            "spark:spark.executor.memory": "4g",
            "spark:spark.driver.memory": "4g"
        },
    },
}

CLUSTER_NAME = f"weather-air-quality-cluster-{datetime.now().strftime('%Y%m%d-%H%M')}"
PYSPARK_URI = f"gs://{env['TEMP_BUCKET']}/dataproc_stream_batch_pyspark.py"

# Create DAG
with DAG(
    'weather_air_quality_pipeline',
    default_args=default_args,
    description='Weather and Air Quality Data Pipeline',
    schedule_interval='0 */12 * * *',  # Run every 12 hours
    catchup=False,
    tags=['weather', 'air_quality'],
) as dag:

    # Task 1: Trigger Batch Data Collection Cloud Function
    trigger_batch_function = CloudFunctionInvokeFunctionOperator(
        task_id='trigger_batch_collection',
        function_id='batch_data_collector',
        location=env['REGION'],
        project_id=env['PROJECT_ID'],
        input_data={
            "LATITUDE": env['LATITUDE'],
            "LONGITUDE": env['LONGITUDE'],
            "BUCKET_NAME": env['BUCKET_NAME'],
            "AIR_QUALITY_PREFIX": env['AIR_QUALITY_PREFIX'],
            "WEATHER_PREFIX": env['WEATHER_PREFIX']
        }
    )

    # Task 2: Trigger Streaming Data Collection Cloud Function
    trigger_stream_function = CloudFunctionInvokeFunctionOperator(
        task_id='trigger_stream_collection',
        function_id='stream_data_collector',
        location=env['REGION'],
        project_id=env['PROJECT_ID'],
        input_data={
            "LATITUDE": env['LATITUDE'],
            "LONGITUDE": env['LONGITUDE'],
            "AIR_QUALITY_TOPIC": env['AIR_QUALITY_TOPIC'],
            "WEATHER_TOPIC": env['WEATHER_TOPIC'],
            "DEAD_LETTER_TOPIC": env['DEAD_LETTER_TOPIC']
        }
    )

    # Task 3: Create Dataproc Cluster
    create_cluster = DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        project_id=env['PROJECT_ID'],
        cluster_name=CLUSTER_NAME,
        cluster_config=CLUSTER_CONFIG,
        region=env['REGION'],
        zone=env['ZONE']
    )

    # Task 4: Submit PySpark Job
    PYSPARK_JOB = {
        "reference": {"project_id": env['PROJECT_ID']},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": PYSPARK_URI,
            "jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"],
            "properties": {
                "spark.project.id": env['PROJECT_ID'],
                "spark.region": env['REGION'],
                "spark.dataset.id": env['DATASET_ID'],
                "spark.location.latitude": str(env['LATITUDE']),
                "spark.location.longitude": str(env['LONGITUDE']),
                "spark.bucket.historical": env['BUCKET_NAME'],
                "spark.bucket.temp": env['TEMP_BUCKET'],
                "spark.prefix.air_quality": env['AIR_QUALITY_PREFIX'],
                "spark.prefix.weather": env['WEATHER_PREFIX'],
                "spark.topic.air_quality": env['AIR_QUALITY_TOPIC'],
                "spark.topic.weather": env['WEATHER_TOPIC']
            }
        }
    }

    submit_pyspark_job = DataprocSubmitJobOperator(
        task_id='submit_pyspark_job',
        project_id=env['PROJECT_ID'],
        region=env['REGION'],
        job=PYSPARK_JOB
    )

    # Task 5: Delete Dataproc Cluster
    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_dataproc_cluster',
        project_id=env['PROJECT_ID'],
        cluster_name=CLUSTER_NAME,
        region=env['REGION'],
        trigger_rule='all_done'  # Ensure cluster is deleted even if processing fails
    )

    # Define task dependencies
    [trigger_batch_function, trigger_stream_function] >> create_cluster >> submit_pyspark_job >> delete_cluster