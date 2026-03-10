#!/bin/bash

SPARK_MASTER="spark://spark-master:7077"
APP_FILE="//opt/spark-apps/cdc_processor.py"

PACKAGES=(
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1"
    "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.101.3"
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    "org.apache.hadoop:hadoop-aws:3.3.4"
    "software.amazon.awssdk:bundle:2.29.52"
)

PACKAGES_STR=$(IFS=,; echo "${PACKAGES[*]}")

docker-compose exec spark-master //opt/spark/bin/spark-submit \
    --master $SPARK_MASTER \
    --packages $PACKAGES_STR \
    --conf "spark.hadoop.fs.s3a.endpoint=http://minio:9000" \
    --conf "spark.hadoop.fs.s3a.access.key=admin" \
    --conf "spark.hadoop.fs.s3a.secret.key=password" \
    --conf "spark.hadoop.fs.s3a.path.style.access=true" \
    --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
    --conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" \
    $APP_FILE