import logging
import requests
import os
import pandas as pd
import boto3
import tempfile
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

def create_sample_data_s3(s3_path, **context):
    """Create sample NYC taxi data for demo purposes"""
    import pandas as pd
    import numpy as np
    from datetime import datetime, timedelta
    
    n_records = np.random.randint(2000000, 3000001)
    start_date = context["data_interval_start"]
    end_date = context["data_interval_end"]
    
    pickup_times = pd.date_range(
        start=start_date,
        end=end_date - timedelta(seconds=1),
        periods=n_records
    )
    
    data = {
        'VendorID': np.random.choice([1, 2], n_records),
        'tpep_pickup_datetime': pickup_times,
        'tpep_dropoff_datetime': pickup_times + pd.to_timedelta(np.random.randint(5, 30, n_records), unit="m"),
        'passenger_count': np.random.choice([1, 2, 3, 4, 5], n_records, p=[0.6, 0.2, 0.1, 0.07, 0.03]),
        'trip_distance': np.random.exponential(2.0, n_records),
        'RatecodeID': np.random.choice([1, 2, 3, 4, 5], n_records, p=[0.9, 0.03, 0.03, 0.02, 0.02]),
        'store_and_fwd_flag': np.random.choice(['N', 'Y'], n_records, p=[0.95, 0.05]),
        'PULocationID': np.random.randint(1, 266, n_records),
        'DOLocationID': np.random.randint(1, 266, n_records),
        'payment_type': np.random.choice([1, 2, 3, 4], n_records, p=[0.7, 0.25, 0.03, 0.02]),
        'fare_amount': np.random.exponential(10.0, n_records),
        'extra': np.random.choice([0, 0.5, 1.0], n_records, p=[0.7, 0.2, 0.1]),
        'mta_tax': np.full(n_records, 0.5),
        'tip_amount': np.random.exponential(2.0, n_records),
        'tolls_amount': np.random.exponential(1.0, n_records) * np.random.choice([0, 1], n_records, p=[0.9, 0.1]),
        'improvement_surcharge': np.full(n_records, 0.3),
        'total_amount': None,
        'congestion_surcharge': np.random.choice([0, 2.5], n_records, p=[0.5, 0.5]),
        'airport_fee': np.random.choice([0, 1.25], n_records, p=[0.95, 0.05])
    }
    
    df = pd.DataFrame(data)
    
    df["tpep_pickup_datetime"]  = df["tpep_pickup_datetime"].dt.floor("us")
    df["tpep_dropoff_datetime"] = df["tpep_dropoff_datetime"].dt.floor("us")
    
    df['total_amount'] = (df['fare_amount'] + df['extra'] + df['mta_tax'] + 
                         df['tip_amount'] + df['tolls_amount'] + 
                         df['improvement_surcharge'] + df['congestion_surcharge'] + 
                         df['airport_fee'])
    
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        local_path = f.name
    
    df.to_parquet(local_path, engine="pyarrow", coerce_timestamps="us", index=False)
    
    s3 = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="admin",
            aws_secret_access_key="password",
            region_name="us-east-1",
        )

    bucket, key = s3_path.replace("s3a://", "").split("/", 1)
    s3.upload_file(local_path, bucket, key)

    os.remove(local_path)
    
def download_or_mock_monthly_taxi_data(**context):
    """Get NYC taxi data for a specific year and month"""
    batch_start_date = context["data_interval_start"]
    year = batch_start_date.year
    month = batch_start_date.month
    
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"
    url = f"{base_url}/{file_name}"
    
    bucket = "data-lake"
    s3_key = f"data/taxi/monthly/year={year}/month={month:02d}/{file_name}"
    s3_path = f"s3a://{bucket}/{s3_key}"

    local_dir = "/tmp/taxi"
    os.makedirs(local_dir, exist_ok=True)
    local_path = Path(local_dir) / file_name

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="admin",
        aws_secret_access_key="password",
        region_name="us-east-1",
    )
    
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
        
        s3.upload_file(str(local_path), "data-lake", s3_key)
        logger.info(f"Uploaded real data to {s3_path}")
    except Exception as e:
        logger.warning(f"Download failed for {file_name}, fallback to mock data. Reason: {e}")
        create_sample_data_s3(s3_path=s3_path, **context)
    finally:
        if local_path.exists():
            local_path.unlink()
            
    context["ti"].xcom_push(key="raw_data_path", value=s3_path)
    
def validate_data(**context):
    ti = context["task_instance"]
    s3_path = ti.xcom_pull(
        task_ids="download_monthly_data",
        key="raw_data_path"
    )

    bucket, key = s3_path.replace("s3a://", "").split("/", 1)

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="admin",
        aws_secret_access_key="password",
        region_name="us-east-1",
    )

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        local_path = f.name

    s3.download_file(bucket, key, local_path)

    df = pd.read_parquet(local_path)
                         
    assert len(df) > 0, "Dataset is empty"
    assert 'tpep_pickup_datetime' in df.columns, "Missing pickup datetime column"
    assert 'tpep_dropoff_datetime' in df.columns, "Missing dropoff datetime column"
    
    null_percentage = df.isnull().sum() / len(df) * 100
    logger.info("Null percentage by column:\n%s", null_percentage)

    logger.info(
        "Dataset validation passed. %d records ready for processing.",
        len(df)
    )

    ti.xcom_push(key="validated_data_path", value=s3_path)
    
def choose_path():
    return (
        "download_monthly_data"
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
    conf={
        "spark.pipeline.mode": "backfill"
    },
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
    python_callable=download_or_mock_monthly_taxi_data,
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
    conf={
        "spark.pipeline.mode": "incremental" 
    },
    application_args=[
        "{{ ti.xcom_pull(task_ids='validate_data', key='validated_data_path') }}",
        "{{ data_interval_start.year }}",
        "{{ data_interval_start.month }}"
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
