# Import required libraries
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, year, month, dayofmonth, hour, dayofweek, 
    when, lit, expr, udf, sum, date_format, to_timestamp,
    from_json, window, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    FloatType, BooleanType, TimestampType, MapType
)
from pyspark.sql.streaming import StreamingQuery
import datetime
import logging
import os
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass

# Configure logging with detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Schema Definitions
AIR_QUALITY_SCHEMA = StructType([
    StructField("time", StringType(), True),
    StructField("pm10", FloatType(), True),
    StructField("pm2_5", FloatType(), True),
    StructField("carbon_monoxide", FloatType(), True),
    StructField("nitrogen_dioxide", FloatType(), True),
    StructField("sulphur_dioxide", FloatType(), True),
    StructField("ozone", FloatType(), True),
    StructField("aerosol_optical_depth", FloatType(), True),
    StructField("dust", FloatType(), True),
    StructField("uv_index", FloatType(), True),
    StructField("us_aqi", IntegerType(), True)
])

WEATHER_SCHEMA = StructType([
    StructField("time", StringType(), True),
    StructField("temperature_2m", FloatType(), True),
    StructField("relative_humidity_2m", FloatType(), True),
    StructField("apparent_temperature", FloatType(), True),
    StructField("precipitation", FloatType(), True),
    StructField("rain", FloatType(), True),
    StructField("snowfall", FloatType(), True),
    StructField("cloud_cover", FloatType(), True),
    StructField("cloud_cover_low", FloatType(), True),
    StructField("cloud_cover_mid", FloatType(), True),
    StructField("cloud_cover_high", FloatType(), True),
    StructField("surface_pressure", FloatType(), True),
    StructField("visibility", FloatType(), True),
    StructField("wind_speed_10m", FloatType(), True),
    StructField("wind_direction_10m", FloatType(), True),
    StructField("wind_gusts_10m", FloatType(), True),
    StructField("weather_code", FloatType(), True),
    StructField("precipitation_probability", FloatType(), True)
])

# Holiday Configuration
HOLIDAYS = {
    2024: {
        (1, 1), (1, 15), (2, 19), (5, 27), (6, 19), (7, 4), (9, 2),
        (10, 14), (11, 11), (11, 28), (11, 29), (12, 24), (12, 25), (12, 31)
    },
    2025: {
        (1, 1), (1, 20), (2, 17), (5, 26), (6, 19), (7, 4), (9, 1),
        (10, 13), (11, 11), (11, 27), (11, 28), (12, 24), (12, 25), (12, 31)
    }
}

@dataclass
class ProcessingMetrics:
    """Data class for tracking processing metrics"""
    batch_id: str
    stream_name: str
    record_count: int
    processing_time: datetime.datetime
    null_percentage: float
    error_count: int = 0
    processed_count: int = 0

class DataQualityError(Exception):
    """Custom exception for data quality issues"""
    pass

def get_config_from_env(spark: SparkSession) -> Dict:
    """
    Get configuration values from Spark environment variables.
    Includes both batch and streaming configurations.
    
    Args:
        spark: Active SparkSession
        
    Returns:
        Dictionary containing all configuration values
    """
    try:
        config = {
            # Project Configuration
            'PROJECT_ID': spark.conf.get("spark.project.id"),
            'REGION': spark.conf.get("spark.region"),
            'DATASET_ID': spark.conf.get("spark.dataset.id"),
            
            # Location Configuration
            'LATITUDE': float(spark.conf.get("spark.location.latitude")),
            'LONGITUDE': float(spark.conf.get("spark.location.longitude")),
            
            # GCS Configuration
            'HISTORICAL_BUCKET': spark.conf.get("spark.bucket.historical"),
            'TEMP_BUCKET': spark.conf.get("spark.bucket.temp"),
            'AIR_QUALITY_PREFIX': spark.conf.get("spark.prefix.air_quality"),
            'WEATHER_PREFIX': spark.conf.get("spark.prefix.weather"),
            
            # Pub/Sub Topics
            'AIR_QUALITY_TOPIC': spark.conf.get("spark.topic.air_quality"),
            'WEATHER_TOPIC': spark.conf.get("spark.topic.weather"),
            
            # BigQuery Tables
            'AIR_QUALITY_FACT_TABLE': spark.conf.get("spark.table.air_quality_fact"),
            'WEATHER_FACT_TABLE': spark.conf.get("spark.table.weather_fact"),
            'AIR_QUALITY_STATUS_DIM_TABLE': spark.conf.get("spark.table.air_quality_status_dim"),
            'LOCATION_DIM_TABLE': spark.conf.get("spark.table.location_dim"),
            'TIME_DIM_TABLE': spark.conf.get("spark.table.time_dim"),
            'WEATHER_CONDITION_DIM_TABLE': spark.conf.get("spark.table.weather_condition_dim"),
            'HARMONIZED_DATA_TABLE': spark.conf.get("spark.table.harmonized_data")
        }
        
        # Validate required configurations
        missing_configs = [key for key, value in config.items() if value is None]
        if missing_configs:
            raise ValueError(f"Missing required configurations: {missing_configs}")
            
        return config
        
    except Exception as e:
        logger.error(f"Error loading configuration: {str(e)}")
        raise

def create_spark_session() -> SparkSession:
    """
    Create and configure Spark session with optimized settings.
    """
    try:
        spark = (SparkSession.builder
                .appName("WeatherAirQualityProcessor")
                .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                .config("spark.sql.broadcastTimeout", "600")
                .config("spark.sql.streaming.schemaInference", "true")
                .config("spark.jars.packages", 
                       "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.1.0")
                .config("spark.dynamicAllocation.enabled", "true")
                .config("spark.shuffle.service.enabled", "true")
                .config("spark.memory.fraction", "0.8")
                .config("spark.default.parallelism", "200")
                .config("spark.sql.shuffle.partitions", "200")
                .getOrCreate())
                
        # Configure log levels
        spark.sparkContext.setLogLevel("WARN")
        
        return spark
        
    except Exception as e:
        logger.error(f"Error creating Spark session: {str(e)}")
        raise

def validate_data(
    df: DataFrame, 
    table_name: str, 
    threshold: float = 10.0,
    required_columns: Optional[List[str]] = None
) -> bool:
    """
    Enhanced data validation with configurable thresholds and required columns.
    
    Args:
        df: DataFrame to validate
        table_name: Name of the table/dataset
        threshold: Maximum allowed percentage of nulls
        required_columns: List of columns that must be present
        
    Returns:
        bool: True if validation passes
        
    Raises:
        DataQualityError: If validation fails
    """
    try:
        # Check if DataFrame is empty
        total_rows = df.count()
        if total_rows == 0:
            raise DataQualityError(f"No data found in {table_name}")
            
        # Validate required columns if specified
        if required_columns:
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise DataQualityError(
                    f"Missing required columns in {table_name}: {missing_columns}"
                )
        
        # Check for null values
        null_counts = df.select([
            sum(col(c).isNull().cast("int")).alias(c) for c in df.columns
        ]).collect()[0]
        
        failed_columns = []
        for col_name in df.columns:
            null_pct = (null_counts[col_name] / total_rows) * 100
            if null_pct > threshold:
                failed_columns.append((col_name, null_pct))
        
        if failed_columns:
            raise DataQualityError(
                f"Columns exceeding null threshold ({threshold}%) in {table_name}: "
                f"{[(col, f'{pct:.2f}%') for col, pct in failed_columns]}"
            )
        
        # Log success
        logger.info(f"Data validation passed for {table_name}")
        return True
        
    except Exception as e:
        logger.error(f"Data validation failed for {table_name}: {str(e)}")
        raise DataQualityError(f"Validation failed: {str(e)}")

def collect_streaming_metrics(
    batch_df: DataFrame,
    batch_id: int,
    stream_name: str
) -> ProcessingMetrics:
    """
    Collect comprehensive metrics for streaming batches.
    
    Args:
        batch_df: Current batch DataFrame
        batch_id: Batch identifier
        stream_name: Name of the stream
        
    Returns:
        ProcessingMetrics object
    """
    try:
        start_time = datetime.datetime.now()
        
        # Calculate basic metrics
        record_count = batch_df.count()
        
        # Calculate null percentages across all columns
        null_percentages = []
        for column in batch_df.columns:
            null_count = batch_df.filter(col(column).isNull()).count()
            if record_count > 0:
                null_percentage = (null_count / record_count) * 100
                null_percentages.append(null_percentage)
        
        avg_null_percentage = sum(null_percentages) / len(null_percentages) \
            if null_percentages else 0
        
        processing_time = datetime.datetime.now()
        duration = (processing_time - start_time).total_seconds()
        
        # Create metrics object
        metrics = ProcessingMetrics(
            batch_id=str(batch_id),
            stream_name=stream_name,
            record_count=record_count,
            processing_time=processing_time,
            null_percentage=avg_null_percentage
        )
        
        # Log detailed metrics
        logger.info(f"Streaming metrics for {stream_name} - Batch {batch_id}:")
        logger.info(f"Records processed: {record_count}")
        logger.info(f"Average null percentage: {avg_null_percentage:.2f}%")
        logger.info(f"Processing duration: {duration:.2f} seconds")
        logger.info(f"Processing rate: {record_count/duration:.2f} records/second")
        
        return metrics
        
    except Exception as e:
        logger.error(f"Error collecting metrics: {str(e)}")
        raise
    


def read_stream_from_pubsub(
    spark: SparkSession,
    topic: str,
    schema: Optional[StructType] = None
) -> DataFrame:
    """
    Enhanced Pub/Sub stream reader with proper schema handling and error recovery.
    
    Args:
        spark: Active SparkSession
        topic: Pub/Sub topic name
        schema: Optional schema override
        
    Returns:
        Streaming DataFrame with parsed data
    """
    try:
        # Select schema based on topic if not provided
        if schema is None:
            schema = AIR_QUALITY_SCHEMA if topic == config['AIR_QUALITY_TOPIC'] \
                else WEATHER_SCHEMA
            
        stream_df = (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", topic)
                .option("startingOffsets", "latest")
                .option("failOnDataLoss", "false")
                .option("maxOffsetsPerTrigger", 10000)  # Control batch size
                .option("kafka.request.timeout.ms", "60000")  # 1-minute timeout
                .option("kafka.session.timeout.ms", "30000")
                .option("kafka.heartbeat.interval.ms", "10000")
                .load())
        
        # Parse JSON data with error handling
        parsed_df = (stream_df
            .select(from_json(
                col("value").cast("string"), 
                schema,
                {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"}
            ).alias("data"))
            .select("data.*"))
            
        # Add timestamp for processing
        parsed_df = parsed_df.withColumn(
            "ingestion_timestamp",
            current_timestamp()
        )
        
        return parsed_df
        
    except Exception as e:
        logger.error(f"Error creating stream from topic {topic}: {str(e)}")
        raise

def merge_batch_and_stream(
    batch_df: DataFrame,
    stream_df: DataFrame,
    window_duration: str = "1 hour",
    watermark_delay: str = "10 minutes"
) -> DataFrame:
    """
    Enhanced merge of batch and streaming data with better deduplication and late data handling.
    
    Args:
        batch_df: Batch processing DataFrame
        stream_df: Streaming DataFrame
        window_duration: Duration for the sliding window
        watermark_delay: How long to wait for late data
        
    Returns:
        Merged and deduplicated DataFrame
    """
    try:
        # Add watermark to handle late-arriving data
        stream_with_watermark = stream_df.withWatermark("timestamp", watermark_delay)
        
        # Create time windows
        windowed_batch = batch_df.withColumn(
            "window",
            window(col("timestamp"), window_duration)
        )
        
        windowed_stream = stream_with_watermark.withColumn(
            "window",
            window(col("timestamp"), window_duration)
        )
        
        # Combine batch and streaming data
        merged_df = windowed_batch.unionByName(
            windowed_stream,
            allowMissingColumns=True
        )
        
        # Remove duplicates with priority to streaming data
        deduplicated_df = (merged_df
            .dropDuplicates(["timestamp"])
            .drop("window")
            .orderBy("timestamp"))
        
        return deduplicated_df
        
    except Exception as e:
        logger.error(f"Error merging batch and stream data: {str(e)}")
        raise

def process_stream_batch(
    batch_df: DataFrame,
    batch_id: int,
    stream_name: str,
    spark: SparkSession
) -> DataFrame:
    """
    Process a single micro-batch of streaming data.
    
    Args:
        batch_df: Current batch DataFrame
        batch_id: Batch identifier
        stream_name: Name of the stream
        spark: Active SparkSession
        
    Returns:
        Processed DataFrame
    """
    try:
        start_time = datetime.datetime.now()
        
        # Collect metrics before processing
        pre_metrics = collect_streaming_metrics(batch_df, batch_id, f"{stream_name}_pre")
        
        # Determine batch type and required columns
        batch_type = "weather" if "weather_code" in batch_df.columns else "air_quality"
        required_cols = ["timestamp"]
        if batch_type == "weather":
            required_cols.extend(["temperature_2m", "weather_code"])
        else:
            required_cols.extend(["us_aqi", "pm2_5"])
        
        # Validate input data
        validate_data(batch_df, f"{stream_name}_batch_{batch_id}", 
                     required_columns=required_cols)
        
        # Process the batch
        processed_df = process_batch(batch_df, spark, batch_type)
        
        # Collect metrics after processing
        post_metrics = collect_streaming_metrics(processed_df, batch_id, 
                                               f"{stream_name}_post")
        
        # Calculate and log processing statistics
        duration = (datetime.datetime.now() - start_time).total_seconds()
        records_per_second = pre_metrics.record_count / duration if duration > 0 else 0
        
        logger.info(f"Batch {batch_id} processing statistics:")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Processing rate: {records_per_second:.2f} records/second")
        logger.info(f"Input records: {pre_metrics.record_count}")
        logger.info(f"Output records: {post_metrics.record_count}")
        
        return processed_df
        
    except Exception as e:
        logger.error(f"Error processing stream batch {batch_id}: {str(e)}")
        raise

def process_stream(
    stream_df: DataFrame,
    checkpoint_location: str,
    query_name: str,
    spark: SparkSession
) -> StreamingQuery:
    """
    Configure and start stream processing with enhanced error handling and monitoring.
    
    Args:
        stream_df: Streaming DataFrame to process
        checkpoint_location: Location to save streaming state
        query_name: Name for the streaming query
        spark: Active SparkSession
        
    Returns:
        Active StreamingQuery
    """
    try:
        def foreach_batch_function(batch_df: DataFrame, batch_id: int):
            if batch_df.count() > 0:
                try:
                    # Process the batch
                    processed_df = process_stream_batch(
                        batch_df, batch_id, query_name, spark
                    )
                    
                    # Write to BigQuery with retry logic
                    max_retries = 3
                    for attempt in range(max_retries):
                        try:
                            write_to_bigquery(
                                processed_df,
                                f"streaming_{query_name}",
                                "append"
                            )
                            break
                        except Exception as e:
                            if attempt == max_retries - 1:
                                raise
                            logger.warning(
                                f"Retry {attempt + 1}/{max_retries} writing to BigQuery: {str(e)}"
                            )
                            
                except Exception as batch_error:
                    logger.error(f"Error processing batch {batch_id}: {str(batch_error)}")
                    # Could add dead-letter queue logic here
                    raise
        
        # Configure and start the streaming query
        streaming_query = (stream_df.writeStream
            .foreachBatch(foreach_batch_function)
            .outputMode("append")
            .option("checkpointLocation", checkpoint_location)
            .trigger(processingTime='1 minute')
            .queryName(query_name)
            .start())
        
        return streaming_query
        
    except Exception as e:
        logger.error(f"Error configuring stream process: {str(e)}")
        raise

def create_streaming_triggers(
    air_quality_stream: StreamingQuery,
    weather_stream: StreamingQuery,
    monitoring_interval: int = 60
) -> None:
    """
    Create and manage streaming triggers with enhanced monitoring and graceful shutdown.
    
    Args:
        air_quality_stream: Air quality streaming query
        weather_stream: Weather streaming query
        monitoring_interval: Interval in seconds to check stream health
    """
    try:
        # Track active streams
        active_streams = {
            "air_quality": air_quality_stream,
            "weather": weather_stream
        }
        
        start_time = datetime.datetime.now()
        
        while any(stream.isActive for stream in active_streams.values()):
            # Monitor stream health
            for name, stream in active_streams.items():
                if stream.isActive:
                    # Log streaming metrics
                    logger.info(f"{name} stream status:")
                    logger.info(f"Input rate: {stream.lastProgress['inputRate']}/second")
                    logger.info(f"Processing rate: {stream.lastProgress['processedRowsPerSecond']}/second")
                    logger.info(f"Batch duration: {stream.lastProgress['batchDuration']}ms")
                    
                    # Check for errors
                    if stream.exception():
                        logger.error(f"Error in {name} stream: {stream.exception()}")
                        raise stream.exception()
                        
            # Sleep for monitoring interval
            time.sleep(monitoring_interval)
            
    except Exception as e:
        logger.error(f"Error in streaming process: {str(e)}")
        raise
        
    finally:
        # Graceful shutdown of streams
        for name, stream in active_streams.items():
            if stream and stream.isActive:
                try:
                    logger.info(f"Stopping {name} stream...")
                    stream.stop()
                    stream.awaitTermination(timeout=30)  # 30-second timeout
                except Exception as e:
                    logger.error(f"Error stopping {name} stream: {str(e)}")
                    
        # Log final statistics
        end_time = datetime.datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info("Streaming process completed:")
        logger.info(f"Total duration: {duration:.2f} seconds")
        
        
        
def create_time_dimension(
    air_quality_df: DataFrame,
    weather_df: DataFrame,
    start_date: Optional[datetime.datetime] = None,
    end_date: Optional[datetime.datetime] = None
) -> DataFrame:
    """
    Create enhanced time dimension with additional temporal attributes.
    
    Args:
        air_quality_df: Air quality DataFrame
        weather_df: Weather DataFrame
        start_date: Optional start date for time dimension
        end_date: Optional end date for time dimension
        
    Returns:
        Time dimension DataFrame
    """
    try:
        # Select timestamp columns
        air_quality_times = air_quality_df.select("timestamp")
        weather_times = weather_df.select("timestamp")
        
        # Union timestamps and add custom range if specified
        time_df = air_quality_times.union(weather_times)
        
        if start_date and end_date:
            date_range = spark.createDataFrame(
                [(start_date + datetime.timedelta(hours=i),) 
                 for i in range(int((end_date - start_date).total_hours()))]
            ).toDF("timestamp")
            time_df = time_df.union(date_range)
        
        # Create time dimension with enhanced attributes
        time_dim = (time_df
            .distinct()
            .withColumn("time_key", 
                expr("cast(date_format(timestamp, 'yyyyMMddHH') as int)"))
            .withColumn("full_date", col("timestamp").cast("date"))
            .withColumn("year", year("timestamp"))
            .withColumn("quarter", expr("ceil(month(timestamp)/3)"))
            .withColumn("month", month("timestamp"))
            .withColumn("month_name", date_format("timestamp", "MMMM"))
            .withColumn("day", dayofmonth("timestamp"))
            .withColumn("hour", hour("timestamp"))
            .withColumn("day_of_week", dayofweek("timestamp"))
            .withColumn("day_name", date_format("timestamp", "EEEE"))
            .withColumn("week_of_year", weekofyear("timestamp"))
            .withColumn("is_weekend", 
                expr("dayofweek(timestamp) in (1,7)"))
            .withColumn("is_end_of_month",
                expr("day(timestamp) = day(last_day(timestamp))"))
            .withColumn("season", expr("case " +
                "when month in (12,1,2) then 'Winter' " +
                "when month in (3,4,5) then 'Spring' " +
                "when month in (6,7,8) then 'Summer' " +
                "else 'Fall' end"))
            .withColumn("date_str", 
                date_format(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
            .withColumn("is_holiday", 
                udf(is_holiday, BooleanType())("date_str"))
            .withColumn("fiscal_year", 
                when(month >= 10, year("timestamp") + 1)
                .otherwise(year("timestamp")))
            .withColumn("fiscal_quarter",
                when(month >= 10, month - 9)
                .when(month >= 7, month - 6)
                .when(month >= 4, month - 3)
                .otherwise(month))
            .drop("date_str"))
        
        # Validate time dimension
        required_cols = ["time_key", "year", "month", "day", "hour"]
        validate_data(time_dim, "TimeDim", required_columns=required_cols)
        
        return time_dim
        
    except Exception as e:
        logger.error(f"Error creating time dimension: {str(e)}")
        raise

def create_location_dimension(
    spark: SparkSession,
    latitude: float,
    longitude: float
) -> DataFrame:
    """
    Create enhanced location dimension with geographic attributes.
    
    Args:
        spark: Active SparkSession
        latitude: Location latitude
        longitude: Location longitude
        
    Returns:
        Location dimension DataFrame
    """
    try:
        # Define climate zones based on latitude
        def get_climate_zone(lat: float) -> str:
            if abs(lat) < 23.5:
                return "Tropical"
            elif abs(lat) < 35:
                return "Subtropical"
            elif abs(lat) < 66.5:
                return "Temperate"
            else:
                return "Polar"
        
        # Define hemisphere
        hemisphere = "Northern" if latitude >= 0 else "Southern"
        
        # Create location data with enhanced attributes
        location_data = [{
            'location_key': hash(f"{latitude}_{longitude}"),
            'latitude': latitude,
            'longitude': longitude,
            'city': 'New York',
            'region': 'Northeast',
            'country': 'United States',
            'continent': 'North America',
            'climate_zone': get_climate_zone(latitude),
            'hemisphere': hemisphere,
            'elevation': 10.0,
            'timezone': 'America/New_York',
            'is_coastal': True,
            'population_density': 'High',
            'urban_density': 'High',
            'last_updated': datetime.datetime.now()
        }]
        
        location_dim = spark.createDataFrame(location_data)
        
        # Validate location dimension
        required_cols = ["location_key", "latitude", "longitude", "city"]
        validate_data(location_dim, "LocationDim", required_columns=required_cols)
        
        return location_dim
        
    except Exception as e:
        logger.error(f"Error creating location dimension: {str(e)}")
        raise

def create_weather_condition_dimension(weather_df: DataFrame) -> DataFrame:
    """
    Create enhanced weather condition dimension with detailed attributes.
    
    Args:
        weather_df: Weather DataFrame with weather codes
        
    Returns:
        Weather condition dimension DataFrame
    """
    try:
        # Get distinct weather codes
        weather_codes = weather_df.select("weather_code").distinct()
        
        # Create UDF for weather condition mapping
        get_condition_udf = udf(get_weather_condition, 
            MapType(StringType(), StringType()))
        
        # Create weather condition dimension with enhanced attributes
        weather_condition_dim = (weather_codes
            .withColumn("condition_info", get_condition_udf("weather_code"))
            .select(
                col("condition_info.condition_key"),
                col("weather_code"),
                col("condition_info.description"),
                col("condition_info.category"),
                col("condition_info.severity_level")
            )
            .withColumn("is_precipitation",
                col("category").isin(["Rain", "Snow", "Drizzle"]))
            .withColumn("is_severe",
                col("severity_level") >= 3)
            .withColumn("visibility_impact",
                when(col("category").isin(["Fog", "Storm"]), "High")
                .when(col("category") == "Clouds", "Medium")
                .otherwise("Low"))
            .withColumn("travel_impact",
                when(col("severity_level") >= 4, "Severe")
                .when(col("severity_level") >= 3, "High")
                .when(col("severity_level") >= 2, "Medium")
                .otherwise("Low")))
        
        # Validate weather condition dimension
        required_cols = ["condition_key", "weather_code", "description", "category"]
        validate_data(weather_condition_dim, "WeatherConditionDim", 
                     required_columns=required_cols)
        
        return weather_condition_dim
        
    except Exception as e:
        logger.error(f"Error creating weather condition dimension: {str(e)}")
        raise

def create_air_quality_status_dimension(df: DataFrame) -> DataFrame:
    """
    Create enhanced air quality status dimension with health implications.
    
    Args:
        df: DataFrame with AQI values
        
    Returns:
        Air quality status dimension DataFrame
    """
    try:
        # Cast AQI column to double for proper handling
        df = df.withColumn("us_aqi", col("us_aqi").cast("double"))
        
        # Get distinct AQI values
        aqi_values = df.select("us_aqi").distinct()
        
        # Create UDF for AQI status mapping
        get_status_udf = udf(get_aqi_status, MapType(StringType(), StringType()))
        
        # Create air quality status dimension with enhanced attributes
        air_quality_status_dim = (aqi_values
            .withColumn("status_info", get_status_udf("us_aqi"))
            .select(
                col("status_info.status_key"),
                col("status_info.aqi_category"),
                col("status_info.health_implications"),
                col("status_info.cautionary_statement"),
                col("status_info.color_code")
            )
            .withColumn("risk_level",
                when(col("aqi_category") == "Good", "Low")
                .when(col("aqi_category") == "Moderate", "Medium")
                .when(col("aqi_category").contains("Unhealthy"), "High")
                .otherwise("Severe"))
            .withColumn("outdoor_activity_recommendation",
                when(col("aqi_category") == "Good", "Normal outdoor activities")
                .when(col("aqi_category") == "Moderate", "Sensitive individuals should limit prolonged outdoor exposure")
                .when(col("aqi_category").contains("Unhealthy"), "Limit outdoor activities")
                .otherwise("Avoid outdoor activities"))
            .withColumn("sensitive_groups_affected",
                when(col("aqi_category").contains("Sensitive"), "Yes")
                .otherwise("No"))
            .withColumn("last_updated", current_timestamp()))
        
        # Validate air quality status dimension
        required_cols = ["status_key", "aqi_category", "health_implications"]
        validate_data(air_quality_status_dim, "AirQualityStatusDim", 
                     required_columns=required_cols)
        
        return air_quality_status_dim
        
    except Exception as e:
        logger.error(f"Error creating air quality status dimension: {str(e)}")
        raise

def create_season_dimension(spark: SparkSession) -> DataFrame:
    """
    Create enhanced season dimension with detailed characteristics.
    
    Args:
        spark: Active SparkSession
        
    Returns:
        Season dimension DataFrame
    """
    try:
        seasons_data = [
            (1, "Winter", 12, 2, 9.5, "-5°C to 5°C", "Snow, frost, cold winds",
             "December to February", "Low", "High", "Shortest", "Low"),
            (2, "Spring", 3, 5, 12.5, "5°C to 20°C", "Rain showers, mild temperatures",
             "March to May", "Medium", "Medium", "Increasing", "Medium"),
            (3, "Summer", 6, 8, 14.5, "20°C to 30°C", "Warm, thunderstorms, humid",
             "June to August", "High", "Low", "Longest", "High"),
            (4, "Fall", 9, 11, 10.5, "5°C to 20°C", "Cool, windy, rain",
             "September to November", "Medium", "Medium", "Decreasing", "Medium")
        ]
        
        season_schema = StructType([
            StructField("season_key", IntegerType(), False),
            StructField("season_name", StringType(), False),
            StructField("start_month", IntegerType(), False),
            StructField("end_month", IntegerType(), False),
            StructField("avg_daylight_hours", FloatType(), True),
            StructField("typical_temp_range", StringType(), True),
            StructField("characteristic_weather", StringType(), True),
            StructField("date_range", StringType(), True),
            StructField("precipitation_likelihood", StringType(), True),
            StructField("heating_demand", StringType(), True),
            StructField("daylight_pattern", StringType(), True),
            StructField("outdoor_activity_level", StringType(), True)
        ])
        
        season_dim = spark.createDataFrame(seasons_data, season_schema)
        
        # Validate season dimension
        required_cols = ["season_key", "season_name", "start_month", "end_month"]
        validate_data(season_dim, "SeasonDim", required_columns=required_cols)
        
        return season_dim
        
    except Exception as e:
        logger.error(f"Error creating season dimension: {str(e)}")
        raise

def create_severity_dimension(spark: SparkSession) -> DataFrame:
    """
    Create enhanced severity dimension with detailed impact information.
    
    Args:
        spark: Active SparkSession
        
    Returns:
        Severity dimension DataFrame
    """
    try:
        severity_data = [
            (1, 1, "Minor", "Minimal impact", "No specific actions needed", "Info",
             "Normal operations", "None required", "Routine monitoring"),
            (2, 2, "Moderate", "Some impact", "Be aware and monitor", "Watch",
             "Elevated awareness", "Standard precautions", "Increased monitoring"),
            (3, 3, "Significant", "Notable impact", "Take precautions", "Advisory",
             "Modified operations", "Active precautions", "Enhanced monitoring"),
            (4, 4, "Severe", "Major impact", "Take protective actions", "Warning",
             "Limited operations", "Urgent precautions", "Continuous monitoring"),
            (5, 5, "Extreme", "Critical impact", "Immediate action required", "Emergency",
             "Emergency operations", "Emergency measures", "Crisis monitoring")
        ]
        
        severity_schema = StructType([
            StructField("severity_key", IntegerType(), False),
            StructField("severity_level", IntegerType(), False),
            StructField("severity_name", StringType(), False),
            StructField("description", StringType(), False),
            StructField("recommended_actions", StringType(), False),
            StructField("alert_level", StringType(), False),
            StructField("operational_status", StringType(), False),
            StructField("safety_measures", StringType(), False),
            StructField("monitoring_requirement", StringType(), False)
        ])
        
        severity_dim = spark.createDataFrame(severity_data, severity_schema)
        
        # Validate severity dimension
        required_cols = ["severity_key", "severity_level", "severity_name"]
        validate_data(severity_dim, "SeverityDim", required_columns=required_cols)
        
        return severity_dim
        
    except Exception as e:
        logger.error(f"Error creating severity dimension: {str(e)}")
        raise
    
    
def create_air_quality_facts(
    df: DataFrame,
    latitude: float,
    longitude: float,
    include_derived_metrics: bool = True
) -> DataFrame:
    """
    Create enhanced air quality fact table with derived metrics.
    
    Args:
        df: Input DataFrame with air quality measurements
        latitude: Location latitude for key generation
        longitude: Location longitude for key generation
        include_derived_metrics: Whether to calculate additional metrics
        
    Returns:
        Air quality fact table DataFrame
    """
    try:
        # Generate location key
        location_key = hash(f"{latitude}_{longitude}")
        
        # Basic fact table creation
        air_quality_fact = (df
            .withColumn("location_key", lit(location_key))
            .withColumn("status_key", 
                udf(lambda aqi: get_aqi_status(aqi)['status_key'])("us_aqi")))
        
        # Add time key
        air_quality_fact = air_quality_fact.withColumn(
            "time_key",
            expr("cast(date_format(timestamp, 'yyyyMMddHH') as int)")
        )
        
        # Calculate derived metrics if requested
        if include_derived_metrics:
            # Air Quality Index trends
            air_quality_fact = (air_quality_fact
                .withColumn("aqi_change_1h",
                    col("us_aqi") - lag("us_aqi", 1).over(
                        Window.partitionBy("location_key")
                        .orderBy("timestamp")))
                .withColumn("aqi_change_24h",
                    col("us_aqi") - lag("us_aqi", 24).over(
                        Window.partitionBy("location_key")
                        .orderBy("timestamp")))
                
                # Pollution ratios
                .withColumn("pm_ratio",
                    when(col("pm2_5") > 0, col("pm10") / col("pm2_5"))
                    .otherwise(null()))
                
                # Composite indices
                .withColumn("pollution_index",
                    (col("pm2_5") * 0.3 +
                     col("pm10") * 0.2 +
                     col("nitrogen_dioxide") * 0.2 +
                     col("ozone") * 0.2 +
                     col("sulphur_dioxide") * 0.1))
                
                # Normalized measures
                .withColumn("pm25_normalized",
                    col("pm2_5") / 35.0)  # WHO guideline value
                .withColumn("pm10_normalized",
                    col("pm10") / 50.0))   # WHO guideline value
        
        # Select final columns
        air_quality_fact = air_quality_fact.select(
            "time_key",
            "location_key",
            "status_key",
            "timestamp",
            "pm2_5",
            "pm10",
            "carbon_monoxide",
            "nitrogen_dioxide",
            "sulphur_dioxide",
            "ozone",
            "us_aqi",
            "aerosol_optical_depth",
            "dust",
            "uv_index",
            *([
                "aqi_change_1h",
                "aqi_change_24h",
                "pm_ratio",
                "pollution_index",
                "pm25_normalized",
                "pm10_normalized"
            ] if include_derived_metrics else [])
        )
        
        # Add processing metadata
        air_quality_fact = air_quality_fact.withColumn(
            "processing_timestamp",
            current_timestamp()
        ).withColumn(
            "etl_batch_id",
            lit(datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
        )
        
        # Validate fact table
        required_cols = ["time_key", "location_key", "status_key", "us_aqi"]
        validate_data(air_quality_fact, "AirQualityFact", 
                     required_columns=required_cols)
        
        return air_quality_fact
        
    except Exception as e:
        logger.error(f"Error creating air quality facts: {str(e)}")
        raise

def create_weather_facts(
    df: DataFrame,
    latitude: float,
    longitude: float,
    include_derived_metrics: bool = True
) -> DataFrame:
    """
    Create enhanced weather fact table with derived metrics and severity assessments.
    
    Args:
        df: Input DataFrame with weather measurements
        latitude: Location latitude for key generation
        longitude: Location longitude for key generation
        include_derived_metrics: Whether to calculate additional metrics
        
    Returns:
        Weather fact table DataFrame
    """
    try:
        # Generate location key
        location_key = hash(f"{latitude}_{longitude}")
        
        # Create initial fact table
        weather_fact = df.withColumn("location_key", lit(location_key))
        
        # Add dimension keys
        weather_fact = (weather_fact
            .withColumn("condition_key", 
                col("weather_code").cast("integer"))
            .withColumn("time_key",
                expr("cast(date_format(timestamp, 'yyyyMMddHH') as int)"))
            .withColumn("severity_key",
                when(col("weather_code").isin([95, 96, 99]), 5)  # Extreme
                .when(col("weather_code").isin([65, 67, 75, 82, 86]), 4)  # Severe
                .when(col("weather_code").isin([63, 73, 81]), 3)  # Significant
                .when(col("weather_code").isin([51, 53, 61, 71, 80, 85]), 2)  # Moderate
                .otherwise(1))  # Minor
            .withColumn("season_key",
                when(month(col("timestamp")).isin(12, 1, 2), 1)  # Winter
                .when(month(col("timestamp")).isin(3, 4, 5), 2)  # Spring
                .when(month(col("timestamp")).isin(6, 7, 8), 3)  # Summer
                .otherwise(4)))  # Fall
        
        # Calculate derived metrics if requested
        if include_derived_metrics:
            weather_fact = (weather_fact
                # Temperature metrics
                .withColumn("temp_difference",
                    col("temperature_2m") - col("apparent_temperature"))
                .withColumn("temp_change_1h",
                    col("temperature_2m") - lag("temperature_2m", 1).over(
                        Window.partitionBy("location_key")
                        .orderBy("timestamp")))
                .withColumn("temp_change_24h",
                    col("temperature_2m") - lag("temperature_2m", 24).over(
                        Window.partitionBy("location_key")
                        .orderBy("timestamp")))
                
                # Wind chill (when temperature < 10°C and wind speed > 4.8 km/h)
                .withColumn("wind_chill",
                    when((col("temperature_2m") < 10) & (col("wind_speed_10m") > 4.8),
                        13.12 + 0.6215 * col("temperature_2m") -
                        11.37 * pow(col("wind_speed_10m"), 0.16) +
                        0.3965 * col("temperature_2m") * 
                        pow(col("wind_speed_10m"), 0.16))
                    .otherwise(null()))
                
                # Heat index (when temperature > 27°C)
                .withColumn("heat_index",
                    when(col("temperature_2m") > 27,
                        -42.379 + 2.04901523 * col("temperature_2m") +
                        10.14333127 * col("relative_humidity_2m") -
                        0.22475541 * col("temperature_2m") * 
                        col("relative_humidity_2m") -
                        6.83783 * pow(10, -3) * pow(col("temperature_2m"), 2) -
                        5.481717 * pow(10, -2) * pow(col("relative_humidity_2m"), 2) +
                        1.22874 * pow(10, -3) * pow(col("temperature_2m"), 2) * 
                        col("relative_humidity_2m") +
                        8.5282 * pow(10, -4) * col("temperature_2m") * 
                        pow(col("relative_humidity_2m"), 2) -
                        1.99 * pow(10, -6) * pow(col("temperature_2m"), 2) * 
                        pow(col("relative_humidity_2m"), 2))
                    .otherwise(null()))
                
                # Severe weather indicators
                .withColumn("is_severe_weather",
                    col("severity_key") >= 4)
                .withColumn("visibility_category",
                    when(col("visibility") < 1000, "Very Poor")
                    .when(col("visibility") < 4000, "Poor")
                    .when(col("visibility") < 10000, "Moderate")
                    .otherwise("Good")))
        
        # Select final columns
        weather_fact = weather_fact.select(
            "time_key",
            "location_key",
            "condition_key",
            "severity_key",
            "season_key",
            "timestamp",
            "temperature_2m",
            "apparent_temperature",
            "precipitation",
            "snowfall",
            "wind_speed_10m",
            "wind_direction_10m",
            "wind_gusts_10m",
            "relative_humidity_2m",
            "surface_pressure",
            "cloud_cover",
            "cloud_cover_low",
            "cloud_cover_mid",
            "cloud_cover_high",
            "visibility",
            "precipitation_probability",
            *([
                "temp_difference",
                "temp_change_1h",
                "temp_change_24h",
                "wind_chill",
                "heat_index",
                "is_severe_weather",
                "visibility_category"
            ] if include_derived_metrics else [])
        )
        
        # Add processing metadata
        weather_fact = weather_fact.withColumn(
            "processing_timestamp",
            current_timestamp()
        ).withColumn(
            "etl_batch_id",
            lit(datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
        )
        
        # Validate fact table
        required_cols = [
            "time_key", "location_key", "condition_key",
            "severity_key", "season_key"
        ]
        validate_data(weather_fact, "WeatherFact", required_columns=required_cols)
        
        return weather_fact
        
    except Exception as e:
        logger.error(f"Error creating weather facts: {str(e)}")
        raise

def write_fact_tables(
    air_quality_fact_df: DataFrame,
    weather_fact_df: DataFrame,
    write_mode: str = "overwrite",
    partition_cols: Optional[List[str]] = None
) -> None:
    """
    Write fact tables to BigQuery with enhanced error handling and partitioning.
    
    Args:
        air_quality_fact_df: Air quality fact table DataFrame
        weather_fact_df: Weather fact table DataFrame
        write_mode: Write mode for BigQuery (overwrite/append)
        partition_cols: Optional list of columns to partition by
    """
    try:
        logger.info("Writing fact tables to BigQuery")
        
        # Default partition columns if none provided
        if partition_cols is None:
            partition_cols = ["year", "month"]
        
        # Add partition columns if not present
        for df in [air_quality_fact_df, weather_fact_df]:
            for col in partition_cols:
                if col not in df.columns:
                    if col == "year":
                        df = df.withColumn(col, year("timestamp"))
                    elif col == "month":
                        df = df.withColumn(col, month("timestamp"))
        
        # Write air quality facts
        logger.info("Writing air quality facts...")
        write_to_bigquery(
            air_quality_fact_df,
            AIR_QUALITY_FACT_TABLE,
            write_mode,
            partition_cols
        )
        
        # Write weather facts
        logger.info("Writing weather facts...")
        write_to_bigquery(
            weather_fact_df,
            WEATHER_FACT_TABLE,
            write_mode,
            partition_cols
        )
        
        logger.info("Successfully wrote fact tables to BigQuery")
        
    except Exception as e:
        logger.error(f"Error writing fact tables: {str(e)}")
        raise
    
    
    
def read_historical_data(
    spark: SparkSession,
    config: Dict[str, str]
) -> Tuple[DataFrame, DataFrame]:
    """
    Read and validate historical data from Cloud Storage.
    
    Args:
        spark: Active SparkSession
        config: Configuration dictionary with bucket and prefix information
        
    Returns:
        Tuple of (air_quality_df, weather_df)
    """
    try:
        # Read air quality data with schema and validation
        air_quality_path = f"gs://{config['HISTORICAL_BUCKET']}/{config['AIR_QUALITY_PREFIX']}/*.csv"
        logger.info(f"Reading air quality data from: {air_quality_path}")
        
        air_quality_df = (spark.read
            .option("header", True)
            .option("mode", "FAILFAST")  # Fail immediately on schema mismatch
            .schema(AIR_QUALITY_SCHEMA)
            .csv(air_quality_path)
            .withColumn("timestamp", to_timestamp(col("time")))
            .drop("time"))
            
        # Read weather data with schema and validation
        weather_path = f"gs://{config['HISTORICAL_BUCKET']}/{config['WEATHER_PREFIX']}/*.csv"
        logger.info(f"Reading weather data from: {weather_path}")
        
        weather_df = (spark.read
            .option("header", True)
            .option("mode", "FAILFAST")
            .schema(WEATHER_SCHEMA)
            .csv(weather_path)
            .withColumn("timestamp", to_timestamp(col("time")))
            .drop("time"))
            
        # Validate datasets
        validate_data(air_quality_df, "raw_air_quality")
        validate_data(weather_df, "raw_weather")
        
        return air_quality_df, weather_df
        
    except Exception as e:
        logger.error(f"Error reading historical data: {str(e)}")
        raise

def process_batch_data(
    spark: SparkSession,
    config: Dict[str, str],
    start_date: Optional[datetime.datetime] = None,
    end_date: Optional[datetime.datetime] = None
) -> None:
    """
    Process historical batch data with comprehensive error handling and monitoring.
    
    Args:
        spark: Active SparkSession
        config: Configuration dictionary
        start_date: Optional start date for processing
        end_date: Optional end date for processing
    """
    try:
        batch_start_time = datetime.datetime.now()
        logger.info("Starting batch data processing")
        
        # Read historical data
        air_quality_batch, weather_batch = read_historical_data(spark, config)
        
        # Filter by date range if specified
        if start_date and end_date:
            air_quality_batch = air_quality_batch.filter(
                (col("timestamp") >= start_date) & 
                (col("timestamp") <= end_date)
            )
            weather_batch = weather_batch.filter(
                (col("timestamp") >= start_date) & 
                (col("timestamp") <= end_date)
            )
        
        # Process dimensions with monitoring
        start_time = datetime.datetime.now()
        logger.info("Creating dimension tables...")
        
        time_dim_df = create_time_dimension(
            air_quality_batch, 
            weather_batch,
            start_date,
            end_date
        )
        
        location_dim_df = create_location_dimension(
            spark,
            config['LATITUDE'],
            config['LONGITUDE']
        )
        
        weather_condition_dim_df = create_weather_condition_dimension(
            weather_batch
        )
        
        air_quality_status_dim_df = create_air_quality_status_dimension(
            air_quality_batch
        )
        
        season_dim_df = create_season_dimension(spark)
        severity_dim_df = create_severity_dimension(spark)
        
        logger.info(f"Dimension creation took: "
                   f"{(datetime.datetime.now() - start_time).total_seconds():.2f} seconds")
        
        # Process facts with monitoring
        start_time = datetime.datetime.now()
        logger.info("Creating fact tables...")
        
        air_quality_fact_df = create_air_quality_facts(
            air_quality_batch,
            config['LATITUDE'],
            config['LONGITUDE']
        )
        
        weather_fact_df = create_weather_facts(
            weather_batch,
            config['LATITUDE'],
            config['LONGITUDE']
        )
        
        logger.info(f"Fact creation took: "
                   f"{(datetime.datetime.now() - start_time).total_seconds():.2f} seconds")
        
        # Write dimensions with monitoring
        start_time = datetime.datetime.now()
        logger.info("Writing dimension tables...")
        
        write_dimension_tables(
            time_dim_df,
            location_dim_df,
            weather_condition_dim_df,
            air_quality_status_dim_df,
            season_dim_df,
            severity_dim_df
        )
        
        logger.info(f"Dimension writing took: "
                   f"{(datetime.datetime.now() - start_time).total_seconds():.2f} seconds")
        
        # Write facts with monitoring
        start_time = datetime.datetime.now()
        logger.info("Writing fact tables...")
        
        write_fact_tables(
            air_quality_fact_df,
            weather_fact_df,
            partition_cols=["year", "month"]
        )
        
        logger.info(f"Fact writing took: "
                   f"{(datetime.datetime.now() - start_time).total_seconds():.2f} seconds")
        
        # Create and write harmonized data
        start_time = datetime.datetime.now()
        logger.info("Creating and writing harmonized data...")
        
        harmonized_df = create_harmonized_data(
            air_quality_batch,
            weather_batch,
            time_dim_df,
            location_dim_df
        )
        
        write_harmonized_data(harmonized_df)
        
        logger.info(f"Harmonization took: "
                   f"{(datetime.datetime.now() - start_time).total_seconds():.2f} seconds")
        
        # Log final statistics
        total_duration = (datetime.datetime.now() - batch_start_time).total_seconds()
        logger.info(f"Batch processing completed in {total_duration:.2f} seconds")
        logger.info(f"Processed {air_quality_batch.count()} air quality records")
        logger.info(f"Processed {weather_batch.count()} weather records")
        
    except Exception as e:
        logger.error(f"Error in batch processing: {str(e)}")
        raise

def process_streaming_data(
    spark: SparkSession,
    config: Dict[str, str]
) -> None:
    """
    Process streaming data with enhanced error handling and monitoring.
    
    Args:
        spark: Active SparkSession
        config: Configuration dictionary
    """
    try:
        logger.info("Starting streaming data processing")
        
        # Create streaming DataFrames
        air_quality_stream = read_stream_from_pubsub(
            spark,
            config['AIR_QUALITY_TOPIC']
        )
        
        weather_stream = read_stream_from_pubsub(
            spark,
            config['WEATHER_TOPIC']
        )
        
        # Configure checkpointing
        air_quality_checkpoint = f"{config['TEMP_BUCKET']}/checkpoints/air_quality"
        weather_checkpoint = f"{config['TEMP_BUCKET']}/checkpoints/weather"
        
        # Start streaming queries with monitoring
        logger.info("Starting streaming queries...")
        
        air_quality_query = process_stream(
            air_quality_stream,
            air_quality_checkpoint,
            "air_quality",
            spark
        )
        
        weather_query = process_stream(
            weather_stream,
            weather_checkpoint,
            "weather",
            spark
        )
        
        # Monitor streaming queries
        create_streaming_triggers(
            air_quality_query,
            weather_query,
            monitoring_interval=60
        )
        
    except Exception as e:
        logger.error(f"Error in streaming process: {str(e)}")
        raise

def create_harmonized_data(
    air_quality_df: DataFrame,
    weather_df: DataFrame,
    time_dim_df: DataFrame,
    location_dim_df: DataFrame
) -> DataFrame:
    """
    Create enhanced harmonized data combining weather and air quality metrics.
    
    Args:
        air_quality_df: Air quality DataFrame
        weather_df: Weather DataFrame
        time_dim_df: Time dimension DataFrame
        location_dim_df: Location dimension DataFrame
        
    Returns:
        Harmonized DataFrame
    """
    try:
        logger.info("Creating harmonized data view")
        
        # Calculate location key
        location_key = hash(f"{config['LATITUDE']}_{config['LONGITUDE']}")
        
        # Add location_key to weather features
        weather_features = (weather_df
            .join(time_dim_df, "timestamp")
            .withColumn("location_key", lit(location_key))
            .select(
                "timestamp",
                "location_key",
                "temperature_2m",
                "apparent_temperature",
                "precipitation",
                "wind_speed_10m",
                "wind_direction_10m",
                "relative_humidity_2m",
                "surface_pressure",
                "cloud_cover",
                "visibility"
            ))
        
        # Add location_key to air quality features
        air_quality_features = (air_quality_df
            .join(time_dim_df, "timestamp")
            .withColumn("location_key", lit(location_key))
            .select(
                "timestamp",
                "location_key",
                "pm2_5",
                "pm10",
                "carbon_monoxide",
                "nitrogen_dioxide",
                "sulphur_dioxide",
                "ozone",
                "us_aqi",
                "aerosol_optical_depth",
                "dust",
                "uv_index"
            ))
        
        # Get time features
        time_features = time_dim_df.select(
            "timestamp",
            "year",
            "month",
            "hour",
            "day_of_week",
            "is_weekend",
            "season",
            "is_holiday"
        )
        
        # Get location features
        location_features = location_dim_df.select(
            "location_key",
            "latitude",
            "longitude",
            "elevation",
            "climate_zone"
        )
        
        # Combine all features
        harmonized_df = (weather_features
            .join(air_quality_features, ["timestamp", "location_key"], "outer")
            .join(time_features, "timestamp")
            .join(location_features, "location_key")
            .repartition("year", "month"))
            
        # Add derived features
        harmonized_df = harmonized_df.withColumn(
            "air_density",
            col("surface_pressure") / (287.05 * (col("temperature_2m") + 273.15))
        ).withColumn(
            "dew_point",
            col("temperature_2m") - ((100 - col("relative_humidity_2m")) / 5)
        )
        
        # Validate harmonized data
        validate_data(harmonized_df, "HarmonizedData")
        
        return harmonized_df
        
    except Exception as e:
        logger.error(f"Error creating harmonized data: {str(e)}")
        raise

def write_harmonized_data(
    harmonized_df: DataFrame,
    write_mode: str = "overwrite"
) -> None:
    """
    Write harmonized data to both Cloud Storage and BigQuery.
    
    Args:
        harmonized_df: Harmonized DataFrame to write
        write_mode: Write mode for outputs
    """
    try:
        logger.info("Writing harmonized data")
        
        # Write to Cloud Storage with partitioning
        start_time = datetime.datetime.now()
        
        storage_path = f"gs://{config['TEMP_BUCKET']}/harmonized_data"
        logger.info(f"Writing to Cloud Storage: {storage_path}")
        
        (harmonized_df.write
            .mode(write_mode)
            .partitionBy("year", "month")
            .parquet(storage_path))
            
        logger.info(f"Cloud Storage write took: "
                   f"{(datetime.datetime.now() - start_time).total_seconds():.2f} seconds")
        
        # Write to BigQuery
        start_time = datetime.datetime.now()
        logger.info("Writing to BigQuery")
        
        write_to_bigquery(
            harmonized_df,
            config['HARMONIZED_DATA_TABLE'],
            write_mode,
            ["year", "month"]
        )
        
        logger.info(f"BigQuery write took: "
                   f"{(datetime.datetime.now() - start_time).total_seconds():.2f} seconds")
        
    except Exception as e:
        logger.error(f"Error writing harmonized data: {str(e)}")
        raise
    

def get_dag_run_id() -> Optional[str]:
    """Get the current Airflow DAG run ID from environment."""
    return os.getenv('AIRFLOW_DAG_RUN_ID')

def update_execution_status(
    status: str,
    start_time: datetime.datetime,
    error: Optional[str] = None
) -> None:
    """
    Update execution status for monitoring.
    
    Args:
        status: Current execution status
        start_time: Process start time
        error: Optional error message
    """
    try:
        duration = (datetime.datetime.now() - start_time).total_seconds()
        dag_id = get_dag_run_id()
        
        status_data = {
            'dag_id': dag_id,
            'status': status,
            'start_time': start_time.isoformat(),
            'duration_seconds': duration,
            'error_message': error
        }
        
        logger.info(f"Execution status: {status_data}")
        
    except Exception as e:
        logger.error(f"Error updating execution status: {str(e)}")

def cleanup_resources(
    spark: Optional[SparkSession] = None,
    streams: Optional[List[StreamingQuery]] = None
) -> None:
    """
    Cleanup resources with proper error handling.
    
    Args:
        spark: Optional SparkSession to stop
        streams: Optional list of streaming queries to stop
    """
    try:
        # Stop streaming queries
        if streams:
            for stream in streams:
                if stream and stream.isActive:
                    try:
                        logger.info(f"Stopping stream: {stream.name}")
                        stream.stop()
                        stream.awaitTermination(timeout=30)
                    except Exception as e:
                        logger.error(f"Error stopping stream {stream.name}: {str(e)}")
        
        # Stop Spark session
        if spark:
            try:
                logger.info("Stopping Spark session")
                spark.stop()
            except Exception as e:
                logger.error(f"Error stopping Spark session: {str(e)}")
                
    except Exception as e:
        logger.error(f"Error in cleanup: {str(e)}")

def initialize_processing(
    app_name: str = "WeatherAirQualityProcessor"
) -> Tuple[SparkSession, Dict[str, str]]:
    """
    Initialize processing environment with proper configuration.
    
    Args:
        app_name: Name for the Spark application
        
    Returns:
        Tuple of (SparkSession, config dictionary)
    """
    try:
        logger.info(f"Initializing {app_name}")
        start_time = datetime.datetime.now()
        
        # Create Spark session
        spark = create_spark_session()
        
        # Get configuration
        config = get_config_from_env(spark)
        
        # Validate configuration
        missing_configs = [key for key, value in config.items() if not value]
        if missing_configs:
            raise ValueError(f"Missing required configurations: {missing_configs}")
        
        logger.info(f"Initialization completed in "
                   f"{(datetime.datetime.now() - start_time).total_seconds():.2f} seconds")
        
        return spark, config
        
    except Exception as e:
        logger.error(f"Error in initialization: {str(e)}")
        raise

def process_data(
    mode: str = "both",
    start_date: Optional[datetime.datetime] = None,
    end_date: Optional[datetime.datetime] = None
) -> None:
    """
    Main data processing function with comprehensive error handling.
    
    Args:
        mode: Processing mode ('batch', 'stream', or 'both')
        start_date: Optional start date for batch processing
        end_date: Optional end date for batch processing
    """
    spark = None
    streams = []
    start_time = datetime.datetime.now()
    
    try:
        # Initialize processing
        update_execution_status("INITIALIZING", start_time)
        spark, config = initialize_processing()
        
        # Process batch data if requested
        if mode in ["batch", "both"]:
            update_execution_status("PROCESSING_BATCH", start_time)
            process_batch_data(spark, config, start_date, end_date)
        
        # Process streaming data if requested
        if mode in ["stream", "both"]:
            update_execution_status("PROCESSING_STREAM", start_time)
            
            # Start air quality stream
            air_quality_stream = read_stream_from_pubsub(
                spark,
                config['AIR_QUALITY_TOPIC']
            )
            air_quality_query = process_stream(
                air_quality_stream,
                f"{config['TEMP_BUCKET']}/checkpoints/air_quality",
                "air_quality",
                spark
            )
            streams.append(air_quality_query)
            
            # Start weather stream
            weather_stream = read_stream_from_pubsub(
                spark,
                config['WEATHER_TOPIC']
            )
            weather_query = process_stream(
                weather_stream,
                f"{config['TEMP_BUCKET']}/checkpoints/weather",
                "weather",
                spark
            )
            streams.append(weather_query)
            
            # Monitor streams
            create_streaming_triggers(air_quality_query, weather_query)
        
        update_execution_status("COMPLETED", start_time)
        
    except Exception as e:
        logger.error("Error in data processing", exc_info=True)
        update_execution_status("FAILED", start_time, str(e))
        raise
        
    finally:
        cleanup_resources(spark, streams)

def main():
    """Main entry point with argument parsing and error handling."""
    try:
        parser = argparse.ArgumentParser(
            description='Weather and Air Quality Data Processor'
        )
        parser.add_argument(
            '--mode',
            choices=['batch', 'stream', 'both'],
            default='both',
            help='Processing mode'
        )
        parser.add_argument(
            '--start-date',
            help='Start date for batch processing (YYYY-MM-DD)'
        )
        parser.add_argument(
            '--end-date',
            help='End date for batch processing (YYYY-MM-DD)'
        )
        
        args = parser.parse_args()
        
        # Parse dates if provided
        start_date = None
        end_date = None
        if args.start_date:
            start_date = datetime.datetime.strptime(args.start_date, '%Y-%m-%d')
        if args.end_date:
            end_date = datetime.datetime.strptime(args.end_date, '%Y-%m-%d')
        
        # Process data
        process_data(args.mode, start_date, end_date)
        
    except Exception as e:
        logger.error("Fatal error in main execution", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()