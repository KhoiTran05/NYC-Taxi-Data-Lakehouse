from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from datetime import datetime, timedelta
from pathlib import Path
import requests
import os
import boto3
import json
import logging
import tempfile
import pandas as pd

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
    "retries": 1,
    "retry_delay": timedelta(minutes=3)
}

dag = DAG(
    dag_id="weather_iceberg_etl",
    default_args=default_args,
    description="ETL for monthly weather data to Iceberg",
    schedule_interval=" 0 0 1 * *",
    catchup=False,
    max_active_runs=1,
    tags=["weather", "iceberg", "spark", "etl"]
)

def get_weather_data(**context):
    """Get NYC weather data for a specific year and month"""
    api_key = os.getenv("OPENWEATHER_API_KEY")
    
    batch_start_date = context["data_interval_start"]
    batch_end_date = context["data_interval_end"]
    year = batch_start_date.year
    month = batch_start_date.month 
    
    url = "https://archive-api.open-meteo.com/v1/archive"
    file_name = f"weather_{year}-{month:02d}.json"
    request_params = {
        "latitude": 40.7143,
        "longitude": -74.006,
        "start_date": batch_start_date.strftime("%Y-%m-%d"),
        "end_date": batch_end_date.strftime("%Y-%m-%d"),
        "hourly": "temperature_2m,relative_humidity_2m,rain,wind_speed_10m,surface_pressure",
    }
    
    bucket = "data-lake"
    s3_key = f"data/weather/monthly/year={year}/month={month:02d}/{file_name}"
    s3_path = f"s3a://{bucket}/{s3_key}"
    
    local_dir = "/tmp/weather"
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
        response = requests.get(url=url, params=request_params, timeout=30)
        response.raise_for_status()

        data = response.json()
        
        with open(local_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            
        s3.upload_file(str(local_path), "data-lake", s3_key)
        logger.info(f"Uploaded weather data to {s3_path}")
    except Exception as e:
        logger.exception(f"Error during getting weather data for {file_name}: {e}")
        raise
    finally:
        if local_path.exists():
            local_path.unlink()
            
    context["ti"].xcom_push(key="weather_data_path", value=s3_path)
    
def mock_location_data(**context):
    """Create NYC location/zone reference data"""
    local_dir = "/tmp/location"
    file_name = "zones.json"
    os.makedirs(local_dir, exist_ok=True)
    local_path = Path(local_dir) / file_name
    
    zones = [
        {'location_id': 1, 'borough': 'Manhattan', 'zone': 'Financial District', 'lat': 40.7074, 'lon': -74.0113},
        {'location_id': 4, 'borough': 'Manhattan', 'zone': 'Times Square', 'lat': 40.7589, 'lon': -73.9851},
        {'location_id': 13, 'borough': 'Manhattan', 'zone': 'Central Park', 'lat': 40.7812, 'lon': -73.9665},
        {'location_id': 48, 'borough': 'Manhattan', 'zone': 'Penn Station', 'lat': 40.7505, 'lon': -73.9934},
        {'location_id': 79, 'borough': 'Manhattan', 'zone': 'East Village', 'lat': 40.7264, 'lon': -73.9818},
        {'location_id': 87, 'borough': 'Manhattan', 'zone': 'Upper East Side', 'lat': 40.7736, 'lon': -73.9566},
        {'location_id': 100, 'borough': 'Manhattan', 'zone': 'Upper West Side', 'lat': 40.7870, 'lon': -73.9754},
        {'location_id': 132, 'borough': 'Queens', 'zone': 'JFK Airport', 'lat': 40.6413, 'lon': -73.7781},
        {'location_id': 138, 'borough': 'Queens', 'zone': 'LaGuardia Airport', 'lat': 40.7769, 'lon': -73.8740},
        {'location_id': 161, 'borough': 'Manhattan', 'zone': 'Midtown East', 'lat': 40.7549, 'lon': -73.9707},
        {'location_id': 162, 'borough': 'Manhattan', 'zone': 'Midtown West', 'lat': 40.7590, 'lon': -73.9845},
        {'location_id': 186, 'borough': 'Manhattan', 'zone': 'Greenwich Village', 'lat': 40.7336, 'lon': -74.0027},
        {'location_id': 230, 'borough': 'Manhattan', 'zone': 'Lower East Side', 'lat': 40.7154, 'lon': -73.9840},
        {'location_id': 237, 'borough': 'Manhattan', 'zone': 'Union Square', 'lat': 40.7359, 'lon': -73.9911},
        {'location_id': 244, 'borough': 'Brooklyn', 'zone': 'Williamsburg', 'lat': 40.7081, 'lon': -73.9571},
        {'location_id': 263, 'borough': 'Manhattan', 'zone': 'Yorkville East', 'lat': 40.7736, 'lon': -73.9566},
    ]
    
    for zone in zones:
        zone['zone_type'] = 'airport' if 'airport' in zone['zone'].lower() else 'neighborhood'
        zone['is_tourist_area'] = zone['zone'] in ['Times Square', 'Central Park', 'Greenwich Village', 'Union Square']
        zone['is_business_district'] = zone['zone'] in ['Financial District', 'Midtown East', 'Midtown West']
        zone['created_at'] = datetime.now().isoformat()
        
    with open(local_dir, "w", encoding="utf-8") as f:
        json.dump(zones, f, ensure_ascii=False, indent=2)
        
    bucket = "data-lake"
    s3_key = f"data/location/zones.json"
    s3_path = f"s3a://{bucket}/{s3_key}"
    
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="admin",
        aws_secret_access_key="password",
        region_name="us-east-1",
    )
    
    s3.uploadFile(str(local_path), "data-lake", s3_key)
    logger.info(f"Uploaded location data to {s3_path}")
    
    context["ti"].xcom_push(key="location_data_path", value=s3_path)
    
def validate_data(**context):
    "Validate weather data"
    weather_s3_path = context["ti"].xcom_pull(task_ids="fetch_weather_data", key="weather_data_path")
    
    bucket, key = weather_s3_path.replace("s3a://", "").split("/", 1)

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="admin",
        aws_secret_access_key="password",
        region_name="us-east-1",
    )
    
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        local_path = f.name
        
    s3.download_file(bucket, key, local_path)
    
    df = pd.read_json(local_path)
    
    cols = df.columns
    if len(df) == 0:
        raise ValueError("Dataset is empty")
    if 'temperature_2m' not in cols:
        raise ValueError("Missing 'temperature_2m' column")
    if 'relative_humidity_2m' not in cols:
        raise ValueError("Missing 'relative_humidity_2m' column")
    if 'rain' not in cols:
        raise ValueError("Missing 'rain' column")
    if 'wind_speed_10m' not in cols:
        raise ValueError("Missing 'rain' column")
    if 'surface_pressure' not in cols:
        raise ValueError("Missing 'surface_pressure' column")
    
    null_percentage = df.isnull().sum() / len(df) * 100
    logger.info("Null percentage by column:\n%s", null_percentage)

    logger.info(
        "Dataset validation passed. %d records ready for processing.",
        len(df)
    )

    context["task_instance"].xcom_push(key="validated_data_path", value=weather_s3_path)
    
def choose_branch():
    if Variable.get("init_done", default_var="false") != "true":
        Variable.set("init_done", "true")
        return "get_location_data"
    return "fetch_weather_data"


branch = BranchPythonOperator(
    task_id="branch_init",
    python_callable=choose_branch
)

get_location_task = PythonOperator(
    task_id="get_location_data",
    python_callable=mock_location_data,
    dag=dag
)

location_etl_task = SparkSubmitOperator(
    task_id="location_etl",
    application="/opt/airflow/spark_jobs/location_to_iceberg.py",
    conn_id="spark_default",
    deploy_mode="client",
    application_args=[
        "{{ ti.xcom_pull(task_ids='get_location_data', key='location_data_path') }}"
    ],
    packages=("org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,"
              "org.apache.iceberg:iceberg-aws-bundle:1.8.1,"
              "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,"
              "org.apache.hadoop:hadoop-aws:3.3.4,"
              "com.amazonaws:aws-java-sdk-bundle:1.12.262"),
    verbose=True,
    dag=dag
)

fetch_weather_task = PythonOperator(
    task_id="fetch_weather_data",
    python_callable=get_weather_data,
    dag=dag
)

validate_weather_task = PythonOperator(
    task_id="validate_weather_data",
    python_callable=validate_data,
    dag=dag
)

weather_etl_task = SparkSubmitOperator(
    task_id="weather_etl",
    application="/opt/airflow/spark_jobs/weather_to_iceberg.py",
    conn_id="spark_default",
    deploy_mode="client",
    application_args=[
        "{{ ti.xcom_pull(task_ids='validate_weather_data', key='validated_data_path') }}"
    ],
    packages=("org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,"
              "org.apache.iceberg:iceberg-aws-bundle:1.8.1,"
              "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,"
              "org.apache.hadoop:hadoop-aws:3.3.4,"
              "com.amazonaws:aws-java-sdk-bundle:1.12.262"),
    verbose=True,
    dag=dag
)

branch >> get_location_task >> location_etl_task
branch >> fetch_weather_task >> validate_weather_task >> weather_etl_task