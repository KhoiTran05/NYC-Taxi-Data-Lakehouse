from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os
import logging

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
    logger.info("Creating 'weather' namespace")
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.weather")
        logger.info("Namespace 'nessie.weather' created successfully")
    except Exception:
        logger.exception("Error during creating 'nessie.weather' namespace")
        raise
    
    hourly_weather_ddl = """
        CREATE TABLE IF NOT EXISTS nessie.weather.hourly_weather (
            timestamp TIMESTAMP,
            temperature_celsius DOUBLE,
            temperature_fahrenheit DOUBLE,
            humidity_percent INT,
            pressure_hpa DOUBLE,
            wind_speed_kmh DOUBLE,
            rain_mm DOUBLE,
            loaded_at TIMESTAMP,
            hour INT,
            day_of_week INT,
            is_weekend BOOLEAN,
            year INT,
            month INT,
            day INT
        ) USING ICEBERG 
        PARTITIONED BY (year, month, day)
        TBLPROPERTIES (
            "write.format.default" = "parquet",
            "write.parquet.compression-codec" = "zstd"
        )
    """
    
    try:
        spark.sql(hourly_weather_ddl)
        logger.info("Iceberg table 'nessie.weather.hourly_weather' created successfully")
    except Exception as e:
        logger.exception(f"Error during creating 'nessie.weather.hourly_weather': {e}")
        raise
    
def process_weather_data(spark, input_path):
    """Transform weather data to Iceberg"""
    try:
        df = spark.read.option("multiline", "true").json(input_path)
        logger.info(f"Successfully read data from {input_path}")
    except Exception as e:
        logger.exception(f"Failed to read json from {input_path}: {e}")
        raise
    
    df = df.select("hourly.*")
    
    cleaned_df = df.select(
            explode(
                arrays_zip(
                    "time",
                    "temperature_2m",
                    "relative_humidity_2m",
                    "rain",
                    "wind_speed_10m",
                    "surface_pressure"
                )
            ).alias("item")
        ) \
        .select(
            to_timestamp(col("item.time"), "yyyy-MM-dd'T'HH:mm").alias("timestamp"),
            round(col("item.temperature_2m"), 2).alias("temperature_celsius"),
            round(col("item.temperature_2m") * 9/5 + 32, 2).alias("temperature_fahrenheit"),
            col("item.relative_humidity_2m").alias("humidity_percent"),
            round(col("item.surface_pressure"), 2).alias("pressure_hpa"),
            round(col("item.wind_speed_10m"), 2).alias("wind_speed_kmh"),
            round(col("item.rain"), 2).alias("rain_mm")
        ) \
        .filter(col("temperature_celsius").isNotNull()) \
        .filter(col("humidity_percent").isNotNull()) \
        .filter(col("pressure_hpa").isNotNull()) \
        .filter(col("wind_speed_kmh").isNotNull()) \
        .filter(col("rain_mm").isNotNull()) \
        .filter(col("timestamp").isNotNull()) \
        .dropDuplicates()
        
    processed_df = cleaned_df \
        .withColumn("loaded_at", current_timestamp()) \
        .withColumn("hour", hour(col("timestamp"))) \
        .withColumn("day_of_week", dayofweek(col("timestamp"))) \
        .withColumn("is_weekend", col("day_of_week").isin([1, 7])) \
        .withColumn("year", year(col("timestamp"))) \
        .withColumn("month", month(col("timestamp"))) \
        .withColumn("day", dayofmonth(col("timestamp")))
    
    result_df = processed_df.select(
        "timestamp",
        "temperature_celsius",
        "temperature_fahrenheit",
        "humidity_percent",
        "pressure_hpa",
        "wind_speed_kmh",
        "rain_mm",
        "loaded_at",
        "hour",
        "day_of_week",
        "is_weekend",
        "year",
        "month",
        "day"
    )
    
    logger.info(f"Writing {result_df.count()} records to Iceberg table")
    
    result_df.writeTo("nessie.weather.hourly_weather") \
        .partitionedBy("year", "month", "day") \
        .overwritePartitions()
        
    logger.info("Data successfully written to Iceberg table")
    
def main():
    input_path = sys.argv[1]
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        create_iceberg_table(spark)
        
        process_weather_data(spark, input_path)
        
        logger.info("Job completed successfully")
    except Exception as e:
        logger.exception("Error during job execution")
        raise
    finally:
        spark.stop()
        
if __name__ == "__main__":
    main()