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
    
def create_ml_tables(spark):
    """Create ML features tables"""
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.ml")
        logger.info("Namespace 'nessie.ml' created successfully")
    except Exception:
        logger.exception("Error during creating 'nessie.ml' namespace")
        raise
    
    demand_features_ddl = """
        CREATE TABLE IF NOT EXISTS nessie.ml.demand_prediction_features (
            location_id INT,
            prediction_hour TIMESTAMP,
            demand BIGINT,
            
            month INT,
            hour_of_day INT,
            day_of_week INT,
            day_of_month INT,
            is_weekend_flag INT,
            is_holiday_flag INT,
            
            temperature_celsius DOUBLE,
            humidity_percent DOUBLE,
            wind_speed_kmh DOUBLE,
            rain_mm DOUBLE,
            
            demand_1h_ago BIGINT,
            demand_24h_ago BIGINT,
            demand_168h_ago BIGINT,
            rolling_avg_7d DOUBLE,
            rolling_avg_30d DOUBLE,
            rolling_std_7d DOUBLE,
            
            zone_type_encoded INT,
            is_tourist_area_flag INT,
            is_business_district_flag INT,
            zone_popularity_score DOUBLE,
            
            created_at TIMESTAMP
        ) USING ICEBERG
        PARTITIONED BY (days(prediction_hour))
        TBLPROPERTIES (
            "write.format.default" = "parquet",
            "write.parquet.compression-codec" = "zstd"
        )
    """
    
    try:
        spark.sql(demand_features_ddl)
        logger.info("Iceberg table 'nessie.ml.demand_prediction_features' created successfully")
    except Exception:
        logger.exception("Error during creating 'nessie.ml.demand_prediction_features'")
        raise
    
    fare_features_ddl = """
        CREATE TABLE IF NOT EXISTS nessie.ml.fare_prediction_features (
            trip_id STRING,
            fare_amount DOUBLE,
            trip_distance DOUBLE,
            trip_duration_minutes DOUBLE,
            passenger_count INT,
            
            pickup_zone_id INT,
            dropoff_zone_id INT,
            pickup_borough_encoded DOUBLE,
            dropoff_borough_encoded DOUBLE,
            zone_distance_km DOUBLE,
            is_airport_trip BOOLEAN,
            is_cross_borough BOOLEAN,
            
            pickup_hour INT,
            pickup_day_of_week INT,
            is_rush_hour BOOLEAN,
            is_weekend BOOLEAN,
            
            temperature_celsius DOUBLE,
            humidity_percent DOUBLE,
            wind_speed_kmh DOUBLE,
            rain_mm DOUBLE,
            
            avg_fare_same_route DOUBLE,
            avg_fare_pickup_zone_1h DOUBLE,
            surge_factor DOUBLE,
            distance_duration_ratio DOUBLE,
            
            feature_date DATE,
            created_at TIMESTAMP
        ) USING ICEBERG
        PARTITIONED BY (feature_date)
        TBLPROPERTIES (
            "write.format.default" = "parquet",
            "write.parquet.compression-codec" = "zstd"
        )
    """
    
    try:
        spark.sql(fare_features_ddl)
        logger.info("Iceberg table 'nessie.ml.fare_prediction_features' created successfully")
    except Exception:
        logger.exception("Error during creating 'nessie.ml.fare_prediction_features'")
        raise
    
def process_demand_features(spark, execution_datetime):
    """Create features for demand prediction model"""
    features_sql = f"""
        WITH hourly_demand AS (
            SELECT
                pu_location_id AS location_id,
                date_trunc('hour', pickup_datetime) AS prediction_hour,
                COUNT(*) AS demand
            FROM nessie.nyc_taxi.trips 
            WHERE pickup_datetime >= '{execution_datetime}' - INTERVAL 30 DAYS
            GROUP BY pu_location_id, date_trunc('hour', pickup_datetime)
        ),
        hourly_weather AS (
            SELECT
                date_trunc('hour', timestamp) AS hour_timestamp,
                temperature_celsius,
                humidity_percent,
                wind_speed_kmh,
                rain_mm
            FROM nessie.weather.hourly_weather
            WHERE timestamp >= '{execution_datetime}' - INTERVAL 30 DAYS
        ),
        zone_data AS (
            SELECT
                location_id,
                CASE 
                    WHEN zone_type = 'airport' THEN 3
                    WHEN zone_type = 'neighborhood' THEN 1
                    ELSE 2
                END AS zone_type_encoded,
                CASE
                    WHEN is_tourist_area THEN 1 ELSE 0 
                END AS is_tourist_area_flag,
                CASE 
                    WHEN is_business_district THEN 1 ELSE 0
                END AS is_business_district_flag
            FROM nessie.reference.taxi_zones
        ),
        zone_popularity AS (
            SELECT
                location_id,
                AVG(demand) AS avg_demand,
                NTILE(10) OVER (ORDER BY AVG(demand)) AS popularity_decile
            FROM hourly_demand
            WHERE prediction_hour >= '{execution_datetime}'
            GROUP BY location_id
        ),
        holiday_date AS (
            SELECT DATE '2019-01-01' AS holiday_date, 'new_year' AS holiday_name UNION ALL
            SELECT DATE '2019-01-21', 'mlk_day' UNION ALL
            SELECT DATE '2019-02-18', 'presidents_day' UNION ALL
            SELECT DATE '2019-05-27', 'memorial_day' UNION ALL
            SELECT DATE '2019-07-04', 'independence_day' UNION ALL
            SELECT DATE '2019-09-02', 'labor_day' UNION ALL
            SELECT DATE '2019-10-14', 'columbus_day' UNION ALL
            SELECT DATE '2019-11-11', 'veterans_day' UNION ALL
            SELECT DATE '2019-11-28', 'thanksgiving' UNION ALL
            SELECT DATE '2019-12-25', 'christmas'
        ),
        features AS (
            SELECT
                hd.location_id,
                hd.prediction_hour,
                hd.demand,
                
                month(hd.prediction_hour) AS month,
                hour(hd.prediction_hour) AS hour_of_day,
                dayofweek(hd.prediction_hour) AS day_of_week,
                dayofmonth(hd.prediction_hour) AS day_of_month,
                CASE
                    WHEN dayofweek(hd.prediction_hour) IN (1, 7) THEN 1
                    ELSE 0
                END AS is_weekend_flag,
                CASE
                    WHEN hol.holiday_date IS NOT NULL THEN 1
                    ELSE 0
                END AS is_holiday_flag,
                
                hw.temperature_celsius,
                hw.humidity_percent,
                hw.wind_speed_kmh,
                hw.rain_mm,
                
                LAG(hd.demand, 1, 0) OVER (
                    PARTITION BY hd.location_id 
                    ORDER BY prediction_hour
                ) AS demand_1h_ago,
                
                LAG(hd.demand, 24, 0) OVER (
                    PARTITION BY hd.location_id 
                    ORDER BY prediction_hour
                ) AS demand_24h_ago,
                
                LAG(hd.demand, 168, 0) OVER (
                    PARTITION BY hd.location_id 
                    ORDER BY prediction_hour
                ) AS demand_168h_ago,
                
                AVG(hd.demand) OVER (
                    PARTITION BY hd.location_id
                    ORDER BY prediction_hour
                    ROWS BETWEEN 168 PRECEDING AND 1 PRECEDING
                ) AS rolling_avg_7d,
                
                AVG(hd.demand) OVER (
                    PARTITION BY hd.location_id
                    ORDER BY prediction_hour
                    ROWS BETWEEN 720 PRECEDING AND 1 PRECEDING
                ) AS rolling_avg_30d,
                
                STDDEV(hd.demand) OVER (
                    PARTITION BY hd.location_id
                    ORDER BY prediction_hour
                    ROWS BETWEEN 168 PRECEDING AND 1 PRECEDING
                ) AS rolling_std_7d,
                
                zd.zone_type_encoded,
                zd.is_tourist_area_flag,
                zd.is_business_district_flag,
                zp.popularity_decile AS zone_popularity_score,
                
                current_timestamp() AS created_at
            FROM hourly_demand hd
            LEFT JOIN hourly_weather hw ON hd.prediction_hour = hw.hour_timestamp
            LEFT JOIN zone_data zd ON hd.location_id = zd.location_id
            LEFT JOIN zone_popularity zp ON hd.location_id = zp.location_id
            LEFT JOIN holiday_date hol ON date(hd.prediction_hour) = hol.holiday_date
        )
        SELECT
            *
        FROM features
        WHERE prediction_hour >= '{execution_datetime}'
    """
    
    logger.info("Creating demand prediction features")
    try:
        demand_prediction_df = spark.sql(features_sql)
        
        demand_prediction_df.writeTo("nessie.ml.demand_prediction_features") \
            .overwritePartitions()
        logger.info("Writing successfully to 'nessie.ml.demand_prediction_features'")
    except Exception as e:
        logger.exception(f"Error during writing into 'nessie.ml.demand_prediction_features': {e}")
        raise
    
def process_fare_features(spark, execution_datetime):
    """Create features for fare prediction model"""
    fare_features_sql = f"""
        WITH trip_base AS (
            SELECT
                CAST(vendor_id AS STRING) || '_' || CAST(unix_timestamp(pickup_datetime) AS STRING) AS trip_id,
                fare_amount,
                trip_distance,
                (unix_timestamp(dropoff_datetime) - unix_timestamp(pickup_datetime)) / 60 AS trip_duration_minutes,
                passenger_count,
                pu_location_id AS pickup_zone_id,
                do_location_id AS dropoff_zone_id,
                pickup_datetime,
                hour(pickup_datetime) AS pickup_hour,
                dayofweek(pickup_datetime) AS pickup_day_of_week
            FROM nessie.nyc_taxi.trips
            WHERE pickup_datetime >= '{execution_datetime}'
        ),
        zone_info AS (
            SELECT
                location_id,
                borough,
                zone_type,
                latitude,
                longitude,
                CASE 
                    WHEN borough = 'Manhattan' THEN 1.0
                    WHEN borough = 'Brooklyn' THEN 2.0
                    WHEN borough = 'Queens' THEN 3.0
                    WHEN borough = 'Bronx' THEN 4.0
                    WHEN borough = 'Staten Island' THEN 5.0
                    ELSE 0.0
                END as borough_encoded
            FROM nessie.reference.taxi_zones
        ),
        weather_info AS (
            SELECT
                date_trunc('hour', timestamp) AS hour_timestamp,
                temperature_celsius,
                humidity_percent,
                wind_speed_kmh,
                rain_mm
            FROM nessie.weather.hourly_weather
            WHERE timestamp >= '{execution_datetime}'
        ),
        route_history AS (
            SELECT
                pu_location_id AS pickup_zone_id,
                do_location_id AS dropoff_zone_id,
                AVG(fare_amount) AS avg_fare_amount
            FROM nessie.nyc_taxi.trips
            WHERE pickup_datetime >= '{execution_datetime}'
            GROUP BY pickup_zone_id, dropoff_zone_id
        ),
        zone_hourly_stats AS (
            SELECT
                pu_location_id AS pickup_zone_id,
                date_trunc('hour', pickup_datetime) as hour_timestamp,
                AVG(fare_amount) AS avg_fare_pickup_zone_1h,
                COUNT(*) AS trips_in_hour
            FROM nessie.nyc_taxi.trips
            WHERE pickup_datetime >= '{execution_datetime}'
            GROUP BY pu_location_id, date_trunc('hour', pickup_datetime)
        )
        SELECT
            tb.trip_id,
            tb.fare_amount,
            tb.trip_distance,
            tb.trip_duration_minutes,
            tb.passenger_count,
            tb.pickup_zone_id,
            tb.dropoff_zone_id,
            pickup_zone.borough_encoded AS pickup_borough_encoded,
            dropoff_zone.borough_encoded AS dropoff_borough_encoded,
            
            SQRT(POW(pickup_zone.latitude - dropoff_zone.latitude, 2) + 
                POW(pickup_zone.longitude - dropoff_zone.longitude, 2)) * 111 as zone_distance_km,
            
            CASE 
                WHEN pickup_zone.zone_type = 'airport' OR dropoff_zone.zone_type = 'airport' THEN TRUE 
                ELSE FALSE 
            END AS is_airport_trip,
            
            CASE 
                WHEN pickup_zone.borough != dropoff_zone.borough THEN TRUE 
                ELSE FALSE 
            END AS is_cross_borough,
                
            tb.pickup_hour,
            tb.pickup_day_of_week,
            
            CASE
                WHEN tb.pickup_hour BETWEEN 7 AND 9 OR tb.pickup_hour BETWEEN 17 AND 19 THEN TRUE
                ELSE FALSE
            END AS is_rush_hour,
            
            CASE
                WHEN tb.pickup_day_of_week IN (1, 7) THEN TRUE
                ELSE FALSE
            END AS is_weekend,
            
            w.temperature_celsius,
            w.humidity_percent,
            w.wind_speed_kmh,
            w.rain_mm,
            rh.avg_fare_amount AS avg_fare_same_route,
            zh.avg_fare_pickup_zone_1h,
            
            CASE
                WHEN zh.trips_in_hour > AVG(zh.trips_in_hour) OVER (PARTITION BY zh.pickup_zone_id) * 1.5 THEN 1.2
                WHEN zh.trips_in_hour < AVG(zh.trips_in_hour) OVER (PARTITION BY zh.pickup_zone_id) * 0.5 THEN 0.8
                ELSE 1.0
            END AS surge_factor,
            
            CASE 
                WHEN tb.trip_duration_minutes > 0 
                THEN tb.trip_distance / (tb.trip_duration_minutes / 60.0) 
                ELSE 0.0 
            END as distance_duration_ratio,
            
            date(tb.pickup_datetime) AS feature_date,
            current_timestamp() AS created_at
        FROM trip_base tb 
        LEFT JOIN zone_info pickup_zone ON tb.pickup_zone_id = pickup_zone.location_id
        LEFT JOIN zone_info dropoff_zone ON tb.dropoff_zone_id = dropoff_zone.location_id
        LEFT JOIN weather_info w ON date_trunc('hour', tb.pickup_datetime) = w.hour_timestamp
        LEFT JOIN route_history rh ON tb.pickup_zone_id = rh.pickup_zone_id 
            AND tb.dropoff_zone_id = rh.dropoff_zone_id
        LEFT JOIN zone_hourly_stats zh ON tb.pickup_zone_id = zh.pickup_zone_id
            AND date_trunc('hour', tb.pickup_datetime) = zh.hour_timestamp
    """
    
    logger.info("Creating fare prediction features")
    try:
        demand_prediction_df = spark.sql(fare_features_sql)
        
        demand_prediction_df.writeTo("nessie.ml.fare_prediction_features") \
            .overwritePartitions()
        logger.info("Writing successfully to 'nessie.ml.fare_prediction_features'")
    except Exception as e:
        logger.exception(f"Error during writing into 'nessie.ml.fare_prediction_features': {e}")
        raise
    
def main():
    """Main function"""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    execution_datetime = sys.argv[1]
    
    try:
        create_ml_tables(spark)
        
        process_demand_features(spark, execution_datetime)
        process_fare_features(spark, execution_datetime)
        
        logger.info("Task completed successfully")
    except Exception as e:
        logger.exception("Error during job execution")
        raise
    finally:
        spark.stop()
        
if __name__ == "__main__":
    main()