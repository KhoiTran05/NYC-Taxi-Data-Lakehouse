#!/bin/bash

echo "Creating Iceberg CDC tables"

docker-compose exec spark-master \
    //opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf spark.executor.cores=2 \
    --conf spark.executor.memory=2g \
    --packages \
org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,\
org.apache.iceberg:iceberg-aws-bundle:1.8.1,\
org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1\
    //opt/spark-apps/create_iceberg_cdc_tables.py