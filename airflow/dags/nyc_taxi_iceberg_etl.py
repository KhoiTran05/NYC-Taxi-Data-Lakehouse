import logging
import requests
import os
import pandas as pd
import boto3
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from pathlib import Path


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG(
    dag_id="nyc_taxi_iceberg_etl",
    default_args=default_args,
    description="Incremental ETL for monthly taxi data to Iceberg",
    schedule_interval="0 0 1 * *",
    catchup=False,
    max_active_runs=1,
    tags=["nyc-taxi", "iceberg", "spark", "etl"]
)

def download_monthly_taxi_data(**context):
    """Get NYC taxi data for a specific year and month"""
    batch_start_date = context["data_interval_start"]
    year = batch_start_date.year
    month = batch_start_date.month
    
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"
    url = f"{base_url}/{file_name}"
    
    data_dir = "/opt/airflow/data/taxi/monthly"
    os.makedirs(data_dir, exist_ok=True)
    
    data_dir = "/tmp/taxi"
    os.makedirs(data_dir, exist_ok=True)
    local_path = Path(data_dir) / file_name
    
    try:
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Range": "bytes=0-"
        }

        response = requests.get(
            url,
            headers=headers,
            stream=True,
            timeout=60
        )
        response.raise_for_status()
        
        downloaded = 0
        with open(local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)

        if downloaded == 0:
            raise ValueError(f"Downloaded file is empty: {file_name}")
        
        size_mb = downloaded / (1024 * 1024)
        logger.info(f"Downloaded {file_name}: {size_mb:.2f} MB")
        
        s3 = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="admin",
            aws_secret_access_key="password",
            region_name="us-east-1",
        )

        s3_key = f"data/taxi/monthly/year={year}/month={month:02d}/{file_name}"

        s3.upload_file(
            str(local_path),
            "data-lake",
            s3_key
        )

        local_path.unlink()
        
        s3_path = f"s3a://data-lake/{s3_key}"
        context["ti"].xcom_push(key="raw_data_path", value=s3_path)
    except requests.RequestException as e:
        logger.error(f"Failed to download {file_name}: {e}")
        raise
    except Exception as e:
        logger.exception(f"Error during taxi data download: {e}")
        raise

def validate_data(**context):
    data_path = context["task_instance"].xcom_pull(task_ids="download_monthly_data", key="raw_data_path")
    
    df = pd.read_parquet(data_path)
                         
    assert len(df) > 0, "Dataset is empty"
    assert 'tpep_pickup_datetime' in df.columns, "Missing pickup datetime column"
    assert 'tpep_dropoff_datetime' in df.columns, "Missing dropoff datetime column"
    
    null_percentage = df.isnull().sum() / len(df) * 100
    logger.info("Null percentage by column:\n%s", null_percentage)

    logger.info(
        "Dataset validation passed. %d records ready for processing.",
        len(df)
    )

    context['task_instance'].xcom_push(key='validated_data_path', value=data_path)
    
def choose_path():
    return (
        "monthly_etl"
        if Variable.get("backfill_done", default_var="false") == "true"
        else "backfill_etl"
    )

def mark_backfill_done():
    Variable.set("backfill_done", "true")
    
branch = BranchPythonOperator(
    task_id="branch_backfill_or_monthly",
    python_callable=choose_path    
)

backfill_etl_task = SparkSubmitOperator(
    task_id="backfill_etl",
    application="/opt/airflow/spark_jobs/nyc_taxi_to_iceberg.py",
    conn_id="spark_default",
    deploy_mode="client",
    application_args=[
        "s3a://data-lake/data/taxi/backfill/"
    ],
    packages=("org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,"
              "org.apache.iceberg:iceberg-aws-bundle:1.8.1,"
              "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,"
              "org.apache.hadoop:hadoop-aws:3.3.4,"
              "com.amazonaws:aws-java-sdk-bundle:1.12.262"),
    verbose=True,
    dag=dag
)

backfill_mark = PythonOperator(
    task_id="mark_backfill_done",
    python_callable=mark_backfill_done
)

download_monthly_data_task = PythonOperator(
    task_id="download_monthly_data",
    python_callable=download_monthly_taxi_data,
    dag=dag
)

validate_data_task = PythonOperator(
    task_id="validate_data",
    python_callable=validate_data,
    dag=dag
)

monthly_etl_task = SparkSubmitOperator(
    task_id="monthly_etl",
    application="/opt/airflow/spark_jobs/nyc_taxi_to_iceberg.py",
    conn_id="spark_default",
    deploy_mode="client",
    application_args=[
        "{{ ti.xcom_pull(task_ids='validate_data', key='validated_data_path') }}"
    ],
    packages=("org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,"
              "org.apache.iceberg:iceberg-aws-bundle:1.8.1,"
              "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,"
              "org.apache.hadoop:hadoop-aws:3.3.4,"
              "com.amazonaws:aws-java-sdk-bundle:1.12.262"),
    verbose=True,
    dag=dag
)

branch >> backfill_etl_task >> backfill_mark
branch >> download_monthly_data_task >> validate_data_task >> monthly_etl_task
