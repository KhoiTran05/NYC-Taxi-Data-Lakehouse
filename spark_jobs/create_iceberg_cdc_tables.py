from pyspark.sql import SparkSession
import logging 
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_spark_session():
    try:
        # Set environment variables for AWS SDK
        os.environ['AWS_REGION'] = 'us-east-1'
        os.environ['AWS_ACCESS_KEY_ID'] = 'admin'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'password'
        
        return SparkSession.builder \
            .appName("Create Iceberg CDC Tables") \
            .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
            .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v2") \
            .config("spark.sql.catalog.nessie.ref", "main") \
            .config("spark.sql.catalog.nessie.warehouse", "s3://lakehouse/warehouse/") \
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
    
def create_tables(spark):
    """Create Iceberg CDC tables"""
    
    logger.info("Creating taxi namespace")
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.taxi_cdc")
        logger.info("Namespace created successfully")
    except Exception:
        logger.exception("Error during creating namespace")
        raise
    
    trips_ddl = """
    CREATE TABLE IF NOT EXISTS nessie.taxi_cdc.trips (
        id BIGINT,
        vendor_id INT,
        pickup_datetime TIMESTAMP,
        dropoff_datetime TIMESTAMP,
        passenger_count INT,
        trip_distance DECIMAL,
        pu_location_id INT,
        do_location_id INT,
        payment_type INT,
        fare_amount DECIMAL,
        extra DECIMAL,
        mta_tax DECIMAL,
        tip_amount DECIMAL,
        tolls_amount DECIMAL,
        total_amount DECIMAL,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        __op STRING,
        __source_ts_ms BIGINT,
        __source_db STRING,
        __source_table STRING
    ) USING ICEBERG
    PARTITIONED BY (day(pickup_datetime))
    TBLPROPERTIES (
        'write.format.default' = 'parquet',
        'write.parquet.compression-codec' = 'snappy',
        'write.metadata.compression-codec' = 'gzip'
    )"""
    
    try:
        spark.sql(trips_ddl)
        logger.info("nessie.taxi_cdc.trips table created successfully")
    except Exception:
        logger.exception("Error during creating trips table")
        raise 
    
    payment_types_ddl = """
    CREATE TABLE IF NOT EXISTS nessie.taxi_cdc.payment_types (
        id INT,
        name STRING,
        description STRING,
        __op STRING,
        __source_ts_ms BIGINT,
        __source_db STRING,
        __source_table STRING
    ) USING ICEBERG
    TBLPROPERTIES (
        'write.format.default' = 'parquet',
        'write.parquet.compression-codec' = 'snappy'
    )"""
    
    try:
        spark.sql(payment_types_ddl)
        logger.info("nessie.taxi_cdc.payment_types table created successfully")
    except Exception:
        logger.exception("Error during creating payment types table")
        raise
    
    vendors_ddl = """
    CREATE TABLE IF NOT EXISTS nessie.taxi_cdc.vendors (
        id INT,
        name STRING,
        description STRING,
        __op STRING,
        __source_ts_ms BIGINT,
        __source_db STRING,
        __source_table STRING
    ) USING ICEBERG
    TBLPROPERTIES (
        'write.format.default' = 'parquet',
        'write.parquet.compression-codec' = 'snappy'
    )"""
    
    try:
        spark.sql(vendors_ddl)
        logger.info("nessie.taxi_cdc.vendors table created successfully")
    except Exception:
        logger.exception("Error during creating vendors table")
        raise
    
def main():
    "Main function to create tables"
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        create_tables(spark)
        logger.info("All tables created successfully")
    except Exception:
        logger.exception("Create table jobs failed")
        raise
    finally:
        spark.stop()
        
if __name__ == "__main__":
    main()