from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
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
    
def create_iceberg_tables(spark):
    """Create Iceberg tables"""
    logger.info("Creating 'realtime' namespace")
    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.realtime")
        logger.info("Namespace 'nessie.realtime' created successfully")
    except Exception:
        logger.exception("Error during creating 'nessie.realtime' namespace")
        raise
    
    trip_aggregations_ddl = """
        CREATE TABLE IF NOT EXISTS nessie.realtime.trip_aggregations (
            window_start TIMESTAMP,
            window_end TIMESTAMP,
            location_id INT,
            pickup_count INT,
            dropoff_count INT,
            total_revenue DOUBLE,
            avg_trip_distance DOUBLE,
            avg_fare_amount DOUBLE,
            unique_vendors INT,
            processed_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(window_start))
        TBLPROPERTIES (
            'write.format.default'='parquet',
            'write.parquet.compression-codec'='zstd'
        )
    """
    
    try:
        spark.sql(trip_aggregations_ddl)
        logger.info("Iceberg table 'nessie.realtime.trip_aggregations' created successfully")
    except Exception:
        logger.exception("Error during creating 'nessie.realtime.trip_aggregations'")
        raise

def get_trip_cdc_schema():
    return StructType([

    StructField("before", StructType([
        StructField("id", LongType(), True),
        StructField("vendor_id", IntegerType(), True),
        StructField("pickup_datetime", LongType(), True),
        StructField("dropoff_datetime", LongType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("pu_location_id", IntegerType(), True),
        StructField("do_location_id", IntegerType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("created_at", LongType(), True),
        StructField("updated_at", LongType(), True)
    ]), True),

    StructField("after", StructType([
        StructField("id", LongType(), True),
        StructField("vendor_id", IntegerType(), True),
        StructField("pickup_datetime", LongType(), True),
        StructField("dropoff_datetime", LongType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("pu_location_id", IntegerType(), True),
        StructField("do_location_id", IntegerType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("created_at", LongType(), True),
        StructField("updated_at", LongType(), True)
    ]), True),

    StructField("source", StructType([
        StructField("version", StringType(), True),
        StructField("connector", StringType(), True),
        StructField("name", StringType(), True),
        StructField("ts_ms", LongType(), True),
        StructField("snapshot", StringType(), True),
        StructField("db", StringType(), True),
        StructField("sequence", StringType(), True),
        StructField("ts_us", LongType(), True),
        StructField("ts_ns", LongType(), True),
        StructField("schema", StringType(), True),
        StructField("table", StringType(), True),
        StructField("txId", LongType(), True),
        StructField("lsn", LongType(), True),
        StructField("xmin", LongType(), True)
    ]), True),

    StructField("transaction", StringType(), True),

    StructField("op", StringType(), True),
    StructField("ts_ms", LongType(), True),
    StructField("ts_us", LongType(), True),
    StructField("ts_ns", LongType(), True)

])
    
def process_trip_cdc_stream(spark, schema):
    """Process CDC stream for trips data"""
    cdc_stream = spark.readStream \
        .format("kafka") \
        .option("kakfa.bootstrap.server", "broker:29092") \
        .option("subscribe", "lakehouse.trips") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
        
    cdc_parsed = cdc_stream.select(
        col("timestamp").alias("kafka_timestamp"),
        from_json(col("value").cast("string"), schema).alias("data")
    ) \
    .select(
        "kafka_timestamp",
        col("data.op").alias("operation"),
        col("data.source.ts_ms").alias("source_ts_ms"),
        col("data.source.db").alias("source_db"),
        col("data.source.table").alias("source_table"),
        col("data.before").alias("before_data"),
        col("data.after").alias("after_data")
    )
    
    active_trips = cdc_parsed \
        .select(
            "operation",
            "source_ts_ms",
            col("after_data").alias("trips")
        ) \
        .filter(
            (col("operation").isin("c", "u")) &
            (col("trips.pickup_datetime").isNotNull()) &
            (col("trips.pu_location_id").isNotNull()) &
            col("trips.fare_amount") > 0
        ) \
        .select(
            "source_ts_ms",
            "trips.*"
        ) \
        .withColumn("pickup_datetime", to_timestamp(col("pickup_datetime") / 1000)) \
        .withColumn("dropoff_datetime", to_timestamp(col("dropoff_datetime") / 1000))
        
    pickup_agg = active_trips \
        .withWatermark("source_ts_ms", "10 minutes") \
        .groupBy(
            "pu_location_id",
            window("source_ts_ms", "5 minutes").alias("window")
        ) \
        .agg(
            count("*").alias("total_pickups"),
            sum("total_amount").alias("total_revenue"),
            avg("trip_distance").alias("avg_trip_distance"),
            avg("fare_amount").alias("avg_fare_amount"),
            count_distinct("vendor_id").alias("unique_vendors")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("pu_location_id").alias("location_id"),
            col("total_pickups"),
            col("total_revenue"),
            col("avg_trip_distance"),
            col("avg_fare_amount"),
            col("unique_vendors"),
        ) \
        .withWatermark("window_start", "10 minutes")
        
    dropoff_agg = active_trips \
        .withWatermark("source_ts_ms", "10 minutes") \
        .groupBy(
            "do_location_id",
            window("source_ts_ms", "5 minutes").alias("window")
        ) \
        .agg(
            count("*").alias("total_dropoffs")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("do_location_id").alias("location_id"),
            col("total_dropoffs"),
        ) \
        .withWatermark("window_start", "10 minutes")
    
    combined = pickup_agg.join(
        dropoff_agg,
        on=["window_start", "window_end", "location_id"],
        how="left"
    ) \
    .select(
        "window_start",
        "window_end",
        "location_id",
        col("total_pickups").alias("pickup_count"),
        coalesce(col("total_dropoffs"), lit(0)).alias("dropoff_count"),
        "total_revenue",
        "avg_trip_distance",
        "avg_fare_amount",
        "unique_vendors",
        current_timestamp().alias("processed_at")
    )
        
    query = combined \
        .writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("table", "nessie.realtime.trip_aggregations") \
        .option("checkpointLocation", "s3a://lakehouse/checkpoints/cdc/trip_aggs") \
        .trigger(processingTime="30 seconds") \
        .start()
        
    return query
        
def monitor_streaming_queries(queries):
    """Monitor streaming queries and handle errors"""
    try:
        for query in queries:
            query.awaitTermination()
    except Exception as e:
        logger.exception(f"Streaming query error: {e}")
        for query in queries:
            if query.isActive:
                query.stop()
        raise
        
def main():
    """Main function"""
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        create_iceberg_tables()
        
        schema = get_trip_cdc_schema()
        trip_agg_query = process_trip_cdc_stream(spark, schema)
        
        queries = [trip_agg_query]
        monitor_streaming_queries(queries)
    except Exception as e:
        logger.exception(f"Error in real-time CDC processor: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()