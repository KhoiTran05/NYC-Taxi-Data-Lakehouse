import sys

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
    
def create_analytics_tables(spark):
    """Create analytics Iceberg tables"""
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.analytics")
        logger.info("Namespace 'nessie.analytics' created successfully")
    except Exception:
        logger.exception("Error during creating 'nessie.analytics' namespace")
        raise
    
    trip_weather_correlation_ddl = """
        CREATE TABLE IF NOT EXISTS nessie.analytics.trip_weather_correlation (
            trip_date DATE,
            hour INT,
            temperature_celsius DOUBLE,
            humidity_percent DOUBLE,
            pressure_hpa DOUBLE,
            wind_speed_kmh DOUBLE,
            rain_mm DOUBLE,
            total_trips BIGINT,
            avg_trip_distance DOUBLE,
            avg_fare_amount DOUBLE,
            avg_tip_amount DOUBLE,
            avg_trip_duration_minutes DOUBLE,
            pickup_zone_diversity INT,
            load_date timestamp
        ) USING ICEBERG
        PARTITIONED BY  (days(trip_date))
        TBLPROPERTIES (
            "write.format.default" = "parquet",
            "write.parquet.compression-codec" = "zstd"
        )
    """
    
    try:
        spark.sql(trip_weather_correlation_ddl)
        logger.info("Iceberg table 'nessie.analytics.trip_weather_correlation' created successfully")
    except Exception as e:
        logger.exception(f"Error during creating 'nessie.analytics.trip_weather_correlation': {e}")
        raise
    
    zone_performance_ddl = """
        CREATE TABLE IF NOT EXISTS nessie.analytics.zone_performance (
            location_id INT,
            zone_name STRING,
            borough STRING,
            zone_type STRING,
            is_tourist_area BOOLEAN,
            is_business_district BOOLEAN,
            trip_date DATE,
            hour INT,
            total_pickups BIGINT,
            total_dropoffs BIGINT,
            avg_fare_per_pickup DOUBLE,
            avg_tip_percentage DOUBLE,
            avg_trip_distance DOUBLE,
            load_date TIMESTAMP
        ) USING ICEBERG
        PARTITIONED BY (months(trip_date))
        TBLPROPERTIES (
            "write.format.default" = "parquet",
            "write.parquet.compression-codec" = "zstd"
        )
    """
    
    try:
        spark.sql(zone_performance_ddl)
        logger.info("Iceberg table 'nessie.analytics.zone_performance' created successfullty")
    except Exception as e:
        logger.exception(f"Error during creating 'nessie.analytics.zone_performance': {e}")
        raise
    
def process_trip_weather_correlation(spark, execution_datetime):
    """Create trip weather correlation analytics"""
    correlation_sql = f"""
        WITH trips_hourly AS (
            SELECT
                DATE(pickup_datetime) AS trip_date,
                HOUR(pickup_datetime) AS hour,
                pu_location_id,
                trip_distance,
                fare_amount,
                tip_amount,
                (unix_timestamp(dropoff_datetime) - unix_timestamp(pickup_datetime)) / 60 AS trip_duration_minutes
            FROM nessie.nyc_taxi.trips
            WHERE pickup_datetime >= '{execution_datetime}'
        ),
        weather_hourly AS (
            SELECT
                date(timestamp) AS weather_date,
                hour(timestamp) AS hour,
                temperature_celsius,
                humidity_percent,
                pressure_hpa,
                wind_speed_kmh,
                rain_mm
            FROM nessie.weather.hourly_weather
            WHERE timestamp >= '{execution_datetime}'
        )
        SELECT
            t.trip_date AS trip_date,
            t.hour AS hour,
            MAX(w.temperature_celsius) as temperature_celsius,
            MAX(w.humidity_percent) as humidity_percent,
            MAX(w.pressure_hpa) as pressure_hpa,
            MAX(w.wind_speed_kmh) as wind_speed_kmh,
            MAX(w.rain_mm) as rain_mm,
            COUNT(*) as total_trips,
            AVG(t.trip_distance) AS avg_trip_distance,
            AVG(t.fare_amount) AS avg_fare_amount,
            AVG(t.tip_amount) AS avg_tip_amount,
            AVG(t.trip_duration_minutes) AS avg_trip_duration_minutes,
            COUNT(DISTINCT t.pu_location_id) AS pickup_zone_diversity,
            current_timestamp() AS load_date
        FROM trips_hourly t
        LEFT JOIN weather_hourly w ON t.trip_date = w.weather_date AND t.hour = w.hour
        GROUP BY t.trip_date, t.hour
    """
    
    logger.info("Creating trip weather correlation analytics")
    try:
        correlation_df = spark.sql(correlation_sql)
        
        correlation_df.sortWithinPartitions("trip_date", "hour") \
            .writeTo("nessie.analytics.trip_weather_correlation") \
            .option("mergeSchema", "false") \
            .overwritePartitions()
        logger.info("Writing successfully into 'nessie.analytics.trip_weather_correlation'")
    except Exception as e:
        logger.exception(f"Error during writing into 'nessie.analytics.trip_weather_correlation': {e}")
        raise
    
def process_zone_performance(spark, execution_datetime):
    """Create zone performance analytics"""
    zone_performance_sql = f"""
        WITH zones AS (
            SELECT
                location_id,
                borough,
                zone AS zone_name,
                zone_type,
                is_tourist_area,
                is_business_district
            FROM nessie.reference.taxi_zones
        ),
        taxi_data AS (
            SELECT
                DATE(pickup_datetime) AS trip_date,
                HOUR(pickup_datetime) AS hour,
                pu_location_id,
                do_location_id,
                trip_distance,
                fare_amount,
                tip_amount
            FROM nessie.nyc_taxi.trips
            WHERE pickup_datetime >= '{execution_datetime}'
        ),
        pickup_metrics AS (
            SELECT
                pu_location_id AS location_id,
                trip_date,
                hour,
                COUNT(*) AS total_pickups,
                AVG(fare_amount) AS avg_fare_per_pickup,
                AVG(CASE WHEN fare_amount > 0 THEN tip_amount / fare_amount ELSE 0 END) AS avg_tip_percentage,
                AVG(trip_distance) AS avg_trip_distance
            FROM taxi_data
            GROUP BY pu_location_id, trip_date, hour
        ),
        dropoff_metrics AS (
            SELECT
                do_location_id AS location_id,
                trip_date,
                hour,
                COUNT(*) AS total_dropoffs
            FROM taxi_data
            GROUP BY do_location_id, trip_date, hour
        )
        SELECT
            p.location_id,
            z.zone_name,
            z.borough,
            z.zone_type,
            z.is_tourist_area,
            z.is_business_district,
            p.trip_date,
            p.hour,
            p.total_pickups,
            COALESCE(d.total_dropoffs, 0) AS total_dropoffs,
            p.avg_fare_per_pickup,
            p.avg_tip_percentage,
            p.avg_trip_distance,
            current_timestamp() AS load_date
        FROM pickup_metrics p
        LEFT JOIN dropoff_metrics d ON p.location_id = d.location_id 
            AND p.trip_date = d.trip_date AND p.hour = d.hour
        LEFT JOIN zones z ON p.location_id = z.location_id
    """
    
    logger.info("Creating zone performance analytics")
    try:
        zone_performance_df = spark.sql(zone_performance_sql)
        
        zone_performance_df.writeTo("nessie.analytics.zone_performance") \
            .overwritePartitions()
        logger.info("Writing successfully to 'nessie.analytics.zone_performance'")
    except Exception as e:
        logger.exception(f"Error during writing into 'nessie.analytics.zone_performance': {e}")
        raise
    
def main():
    """Main fucnction"""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    execution_datetime = sys.argv[1]
    try:
        create_analytics_tables(spark)
        
        process_trip_weather_correlation(spark, execution_datetime)
        process_zone_performance(spark, execution_datetime)
        
        logger.info("Task completed successfully")
    except Exception as e:
        logger.exception("Error during job execution")
        raise
    finally:
        spark.stop()
        
if __name__ == "__main__":
    main()