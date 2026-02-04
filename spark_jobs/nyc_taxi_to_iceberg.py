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
    """Create Iceberg table if it does not exist"""
    logger.info("Creating taxi namespace")
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.nyc_taxi")
        logger.info("Namespace created successfully")
    except Exception:
        logger.exception("Error during creating namespace")
        raise
    
    trips_ddl = """
        CREATE TABLE IF NOT EXISTS nessie.nyc_taxi.trips (
            vendor_id INT,
            pickup_datetime TIMESTAMP,
            dropoff_datetime TIMESTAMP,
            passenger_count INT,
            trip_distance DOUBLE,
            pu_location_id INT,
            do_location_id INT,
            payment_type INT,
            fare_amount DOUBLE,
            extra DOUBLE,
            mta_tax DOUBLE,
            tip_amount DOUBLE,
            tolls_amount DOUBLE,
            total_amount DOUBLE,
            loaded_at TIMESTAMP,
            year INT,
            month INT,
            day INT
        ) USING ICEBERG
        PARTITIONED BY (year, month)
        TBLPROPERTIES (
            "write.format.default" = "parquet",
            "write.parquet.compression-codec" = "zstd"
        )
    """
    
    try:
        spark.sql(trips_ddl)
        logger.info("Iceberg table 'nessie.nyc_taxi.trips' created successfully")
    except Exception as e:
        logger.exception(f"Error during creating 'nessie.nyc_taxi.trips': {e}")
        raise
    
    monthly_sum_ddl = """
        CREATE TABLE IF NOT EXISTS nessie.nyc_taxi.monthly_summary (
            year INT,
            month INT,
            total_trips BIGINT,
            total_amount DOUBLE,
            total_passengers BIGINT,
            avg_trip_distance DOUBLE,
            avg_fare_amount DOUBLE,
            avg_tip_amount DOUBLE,
            last_updated TIMESTAMP
        ) USING ICEBERG
        PARTITIONED BY (year, month)
    """
    
    try:
        spark.sql(monthly_sum_ddl)
        logger.info("Iceberg table 'nessie.nyc_taxi.monthly_summary' created successfully")
    except Exception as e:
        logger.exception(f"Error during creating 'nessie.nyc_taxi.monthly_summary': {e}")
        raise
    
def to_iceberg(spark, input_path):
    """Transform backfilled data to iceberg"""
    try:
        df = spark.read.parquet(input_path)
        logger.info(f"Successfully read {df.count()} records from {input_path}")
    except Exception as e:
        logger.error(f"Failed to read parquet from {input_path}: {e}")
        raise
    
    cleaned_df = df \
        .filter(col("tpep_pickup_datetime").isNotNull()) \
        .filter(col("tpep_dropoff_datetime").isNotNull()) \
        .filter(col("passenger_count") > 0) \
        .filter(col("trip_distance") > 0) \
        .filter(col("fare_amount") > 0) \
        .filter(col("total_amount") > 0) \
        .dropDuplicates()
        
    processed_df = cleaned_df \
        .withColumnRenamed("VendorID", "vendor_id") \
        .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime") \
        .withColumnRenamed("PULocationID", "pu_location_id") \
        .withColumnRenamed("DOLocationID", "do_location_id") \
        .withColumn("loaded_at", current_timestamp()) \
        .withColumn("year", year(col("pickup_datetime"))) \
        .withColumn("month", month(col("pickup_datetime"))) \
        .withColumn("day", dayofmonth(col("pickup_datetime")))
        
    result_df = processed_df.select(
        "vendor_id",
        "pickup_datetime",
        "dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "pu_location_id",
        "do_location_id",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "total_amount",
        "loaded_at",
        "year",
        "month",
        "day"
    )
    
    logger.info(f"Writing {result_df.count()} records to Iceberg table")
    
    result_df.writeTo("nessie.nyc_taxi.trips") \
        .partitionedBy("year", "month") \
        .overwritePartitions()
        
    logger.info("Data successfully written to Iceberg table")

def insert_aggregated_tables(spark):
    """Insert into aggregated tables in Iceberg"""
    logger.info("Working on nessie.nyc_taxi.monthly_summary")
    
    mode = spark.conf.get("spark.pipeline.mode", "backfill")
    
    if mode == "backfill":
        merge_query = f"""
            MERGE INTO nessie.nyc_taxi.monthly_summary t
            USING (
                SELECT
                    year,
                    month,
                    COUNT(*) AS total_trips,
                    ROUND(SUM(total_amount), 2) AS total_amount,
                    SUM(passenger_count) AS total_passengers,
                    ROUND(AVG(trip_distance), 2) AS avg_trip_distance,
                    ROUND(AVG(fare_amount), 2) AS avg_fare_amount,
                    ROUND(AVG(tip_amount), 2) AS avg_tip_amount,
                    MAX(loaded_at) AS last_updated
                FROM nessie.nyc_taxi.trips
                WHERE 
                    year = 2025
                    AND month BETWEEN 1 AND 11
                GROUP BY year, month
            ) s 
            ON t.year = s.year AND t.month = s.month
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """
    else:
        year = int(sys.argv[2])
        month = int(sys.argv[3])
        
        merge_query = f"""
            MERGE INTO nessie.nyc_taxi.monthly_summary t
            USING (
                SELECT
                    year,
                    month,
                    COUNT(*) AS total_trips,
                    ROUND(SUM(total_amount), 2) AS total_amount,
                    SUM(passenger_count) AS total_passengers,
                    ROUND(AVG(trip_distance), 2) AS avg_trip_distance,
                    ROUND(AVG(fare_amount), 2) AS avg_fare_amount,
                    ROUND(AVG(tip_amount), 2) AS avg_tip_amount,
                    MAX(loaded_at) AS last_updated
                FROM nessie.nyc_taxi.trips
                WHERE 
                    year = {year}
                    AND month = {month}
                GROUP BY year, month
            ) s 
            ON t.year = s.year AND t.month = s.month
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """
    
    try:
        spark.sql(merge_query)        
        logger.info("Merging successfully into 'nessie.nyc_taxi.monthly_summary'")
    except Exception as e:
        logger.exception(f"Error during merging into 'nessie.nyc_taxi.monthly_summary': {e}")
        raise
    
    summary_stat = f"""
        SELECT 
            year, 
            month, 
            COUNT(*) cnt
        FROM nessie.nyc_taxi.trips
        GROUP BY year, month
        ORDER BY year, month
    """
    
    try:
        df_stat = spark.sql(summary_stat)
        logger.info("Monthly trip stats:")
        df_stat.show(truncate=False)
    except Exception as e:
        logger.warning(f"Error during stat query execution: {e}")
    
def main():        
    input_path = sys.argv[1]
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        create_iceberg_table(spark)
        
        to_iceberg(spark, input_path)
        
        insert_aggregated_tables(spark)

        logger.info("Job completed successfully")
    except Exception as e:
        logger.exception("Error during job execution")
        raise
    finally:
        spark.stop()
        
if __name__ == "__main__":
    main()
    