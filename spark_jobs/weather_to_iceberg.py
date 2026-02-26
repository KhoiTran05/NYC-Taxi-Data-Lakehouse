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
            temperature_celcius DOUBLE,
            temperature_fahrenheit DOUBLE,
            humidity_percent INT,
            pressure_hpa DOUBLE,
            wind_speed_kmh DOUBLE,
            rain_mm DOUBLE,
            weather_condition STRING,
            loaded_at TIMESTAMP,
            hour INT,
            day_of_week INT,
            is_weekend BOOLEAN,
            year INT,
            month INT,
            day INT
        ) USING ICEBERG 
        PARTITINED BY (year, month, day)
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
        df = spark.read.parquet(input_path)
        logger.info(f"Successfully read {df.count()} records from {input_path}")
    except Exception as e:
        logger.exception(f"Failed to read parquet from {input_path}: {e}")
        raise
    
    cleaned_df = df.select(explode("list").alias("item")) \
        .select(
            col("item.dt"),
            round(col("item.main.temp") - 273.15, 2).alias("temperature_celcius"),
            col("item.main.humidity").alias("humidity_percent"),
            round(col("item.main.pressure"), 2).alias("pressure_hpa"),
            round(col("item.wind.speed"), 2).alias("wind_speed_ms"),
            round(col("item.rain.1h"), 2).alias("rain_mm"),
            col("item.weather[0].main").alias("weather_condition"),
        ) \
        .filter(col("temperature_celcius").isNotNull()) \
        .filter(col("humidity_percent").isNotNull()) \
        .filter(col("pressure_hpa").isNotNull()) \
        .filter(col("wind_speed_ms").isNotNull()) \
        .dropDuplicates()
        
    processed_df = cleaned_df \
        .withColumn(
            "timestamp",
            from_utc_timestamp(from_unixtime(col("dt")), "America/New_York")
        ) \
        .withColumn("wind_speed_kmh", round(col("wind_speed_ms") * 3.6, 2)) \
        .withColumn("temperature_fahrenheit", round((col("temperature_celcius") * 9/5) + 32, 2)) \
        .withColumn("loaded_at", current_timestamp()) \
        .withColumn("hour", hour(col("timestamp"))) \
        .withColumn("day_of_week", dayofweek(col("timestamp"))) \
        .withColumn("is_weekend", col("day_of_week").isin([1, 7])) \
        .withColumn("year", year(col("timestamp"))) \
        .withColumn("month", month(col("timestamp"))) \
        .withColumn("day", dayofmonth(col("timestamp")))
    
    result_df = processed_df.select(
        "timestamp",
        "temperature_celcius",
        "temperature_fahrenheit",
        "humidity_percent",
        "pressure_hpa",
        "wind_speed_kmh",
        "rain_mm",
        "weather_condition",
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