#!/bin/bash

export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=password

docker exec -it spark-master //opt/spark/bin/spark-sql \
  --packages \
org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,\
org.apache.iceberg:iceberg-aws-bundle:1.8.1,\
org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog \
  --conf spark.sql.catalog.nessie.uri=http://nessie:19120/api/v2 \
  --conf spark.sql.catalog.nessie.ref=main \
  --conf spark.sql.catalog.nessie.warehouse=s3://lakehouse/warehouse/ \
  --conf spark.sql.catalog.nessie.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.nessie.s3.endpoint=http://minio:9000 \
  --conf spark.sql.catalog.nessie.s3.access-key-id=admin \
  --conf spark.sql.catalog.nessie.s3.secret-access-key=password \
  --conf spark.sql.catalog.nessie.s3.path-style-access=true \
  --conf spark.sql.catalog.nessie.s3.region=us-east-1 \
  --conf spark.sql.catalog.nessie.client.region=us-east-1 \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=admin \
  --conf spark.hadoop.fs.s3a.secret.key=password \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.region=us-east-1 \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false