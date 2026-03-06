from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import logging
import boto3

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
    dag_id="comprehensive_analytics_etl",
    default_args=default_args,
    description="ETL for monthly comprehensive analytics and ML features engineering",
    schedule_interval="0 0 1 * *",
    catchup=False,
    max_active_runs=1,
    tags=['analytics', 'ml', 'iceberg', 'spark', 'transformation'],
)

def check_minio_data_availability(**context):
    """Checks that the latest taxi and weather data partitions exist in MinIO"""
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="admin",
        aws_secret_access_key="password",
        region_name="us-east-1",
    )

    BUCKET = "lakehouse"

    end_date = context["data_interval_end"] - timedelta(days=1)
    year = end_date.year
    month = end_date.month
    day = end_date.day

    def list_keys(prefix: str) -> list[str]:
        """List all object keys under a given prefix"""
        paginator = s3.get_paginator("list_objects_v2")
        keys = []
        for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        return keys

    def find_partition(base_prefix: str, partition_suffix: str, label: str):
        """
        Searches for any table directory (with UUID) under base_prefix,
        then checks for the expected partition_suffix beneath it.
        Raises AirflowException if no data files found.
        """
        response = s3.list_objects_v2(
            Bucket=BUCKET,
            Prefix=base_prefix,
            Delimiter="/"
        )
        table_dirs = [p["Prefix"] for p in response.get("CommonPrefixes", [])]

        if not table_dirs:
            raise AirflowException(
                f"[{label}] No table directory found under: s3a://{BUCKET}/{base_prefix}"
            )

        found_files = []
        for table_dir in table_dirs:
            full_prefix = f"{table_dir}data/{partition_suffix}"
            keys = list_keys(full_prefix)
            found_files.extend(keys)

        if not found_files:
            raise AirflowException(
                f"[{label}] No data files found for partition: "
                f"s3a://{BUCKET}/{base_prefix}*/data/{partition_suffix}"
            )

        logger.info(f"[{label}] Found {len(found_files)} file(s) for partition '{partition_suffix}'")
        
        find_partition(
            base_prefix="warehouse/nyc_taxi/trips_",
            partition_suffix=f"year={year}/month={month}/",
            label="Taxi",
        )

        find_partition(
            base_prefix="warehouse/weather/hourly_weather_",
            partition_suffix=f"year={year}/month={month}/day={day}/",
            label="Weather",
        )

        logger.info("All required data is available in MinIO. Proceeding with analytics DAG.")
   
        
wait_for_taxi_task = ExternalTaskSensor(
    task_id="wait_for_taxi_data",
    external_dag_id="nyc_taxi_iceberg_etl",
    external_task_id="monthly_etl",
    timeout=300,
    allowed_states=['success'],
    failed_states=['failed', 'upstream_failed'],
    dag=dag
)

wait_for_weather_task = ExternalTaskSensor(
    task_id="wait_for_weather_data",
    external_dag_id="weather_iceberg_etl",
    external_task_id="weather_etl",
    timeout=300,
    allowed_states=['success'],
    failed_states=['failed', 'upstream_failed'],
    dag=dag
)

check_data_task = PythonOperator(
    task_id="check_minio_data_availability",
    python_callable=check_minio_data_availability,
    dag=dag
)

comprehensive_analytics_task = SparkSubmitOperator(
    task_id="comprehensive_analytics_spark_transformation",
    application="/opt/airflow/spark_jobs/comprehensive_analytics.py",
    conn_id="spark_default",
    deploy_mode="client",
    application_args=[
        "{{ data_interval_start }}"
    ],
    packages=("org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,"
              "org.apache.iceberg:iceberg-aws-bundle:1.8.1,"
              "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,"
              "org.apache.hadoop:hadoop-aws:3.3.4,"
              "com.amazonaws:aws-java-sdk-bundle:1.12.262"),
    verbose=True,
    dag=dag
)

ml_features_engineering_task = SparkSubmitOperator(
    task_id="ml_features_engineering",
    application="/opt/airflow/spark_jobs/ml_feature_engineering.py",
    conn_id="spark_default",
    deploy_mode="client",
    application_args=[
        "{{ data_interval_start }}"
    ],
    packages=("org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,"
              "org.apache.iceberg:iceberg-aws-bundle:1.8.1,"
              "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,"
              "org.apache.hadoop:hadoop-aws:3.3.4,"
              "com.amazonaws:aws-java-sdk-bundle:1.12.262"),
    verbose=True,
    dag=dag
)

[wait_for_taxi_task, wait_for_weather_task] >> check_data_task
check_data_task >> [comprehensive_analytics_task, ml_features_engineering_task]