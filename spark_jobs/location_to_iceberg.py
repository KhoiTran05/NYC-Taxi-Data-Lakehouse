from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_spark_session():
    try:
        os.environ['AWS_REGION'] = 'us-east-1'
        os.environ['AWS_ACCESS_KEY_ID'] = 'admin'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'password'
        
        return SparkSession.builder \
            .appName("Create Iceberg CDC Tables") \
            .master("spark://spark-master:7077") \
            .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
            .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v2") \
            .config("spark.sql.catalog.nessie.ref", "main") \
            .config("spark.sql.catalog.nessie.warehouse", "s3a://lakehouse/warehouse/") \
            .config("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
            .config("spark.sql.catalog.nessie.s3.endpoint", "http://minio:9000") \
            .config("spark.sql.catalog.nessie.s3.access-key-id", "admin") \
            .config("spark.sql.catalog.nessie.s3.secret-access-key", "password") \
            .config("spark.sql.catalog.nessie.s3.path-style-access", "true") \
            .config("spark.sql.catalog.nessie.s3.region", "us-east-1") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "admin") \
            .config("spark.hadoop.fs.s3a.secret.key", "password") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.region", "us-east-1") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.sql.catalog.nessie.client.region", "us-east-1") \
            .getOrCreate()
    except Exception:
        logger.exception("Error during creating SparkSession")
        raise
    
def create_iceberg_table(spark):
    """Create Iceberg table"""
    logger.info("Creating 'reference' namespace")
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.reference")
        logger.info("Namespace 'nessie.reference' created successfully")
    except Exception:
        logger.exception("Error during creating 'nessie.reference' namespace")
        raise
    
    taxi_zones_ddl = """
        CREATE TABLE IF NOT EXISTS nessie.reference.taxi_zones (
            location_id INT,
            borough STRING,
            zone STRING,
            latitude DOUBLE,
            longitude DOUBLE,
            zone_type STRING,
            is_tourist_area BOOLEAN,
            is_business_district BOOLEAN,
            created_at TIMESTAMP,
            loaded_at TIMESTAMP
        ) USING ICEBERG
        TBLPROPERTIES (
        'write.format.default'='parquet',
        'write.parquet.compression-codec'='zstd'
    )
    """
    
    try:
        spark.sql(taxi_zones_ddl)
        logger.info("Iceberg table 'nessie.reference.taxi_zones' created successfully")
    except Exception as e: 
        logger.exception(f"Error during creating 'nessie.reference.taxi_zones': {e}")
        raise
    
def process_location_data(spark, input_path):
    """Transform location data to Iceberg"""
    try:
        df = spark.read.option("multiline", "true").json(input_path)
        logger.info(f"Successfully read {df.count()} records from {input_path}")
    except Exception as e:
        logger.exception(f"Failed to read json from {input_path}: {e}")
        raise
    
    processed_df = df \
        .withColumnRenamed("lat", "latitude") \
        .withColumnRenamed("lon", "longitude") \
        .withColumn("created_at", col("created_at").cast("timestamp")) \
        .withColumn("loaded_at", current_timestamp())
        
    logger.info(f"Writing {processed_df.count()} records to Iceberg table")
    
    processed_df.writeTo("nessie.reference.taxi_zones") \
        .append()
        
    logger.info("Data successfully written to Iceberg table")
    
def main():
    input_path = sys.argv[1]
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        create_iceberg_table(spark)
        
        process_location_data(spark, input_path)
        
        logger.info("Job completed successfully")
    except Exception as e:
        logger.exception("Error during job execution")
        raise
    finally:
        spark.stop()
        
if __name__ == "__main__":
    main()