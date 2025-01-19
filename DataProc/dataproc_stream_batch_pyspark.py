from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, from_unixtime, unix_timestamp, 
    year, month, dayofmonth, hour, dayofweek, 
    when, lit, expr, udf, sum, current_timestamp,
    window
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    FloatType, BooleanType, TimestampType, MapType
)
from pyspark.sql.streaming import StreamingQuery
from tenacity import retry, stop_after_attempt, wait_exponential
import datetime
import json
import os
import logging
from typing import Dict, List, Optional
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Configuration from environment variables
PROJECT_ID = os.getenv('PROJECT_ID')
REGION = os.getenv('REGION')
DATASET_ID = os.getenv('DATASET_ID')
LATITUDE = float(os.getenv('LATITUDE'))
LONGITUDE = float(os.getenv('LONGITUDE'))

# Pub/Sub Topics
AIR_QUALITY_TOPIC = os.getenv('AIR_QUALITY_TOPIC')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC')
DEAD_LETTER_TOPIC = os.getenv('DEAD_LETTER_TOPIC')

# GCS Configuration
HISTORICAL_BUCKET = os.getenv('HISTORICAL_BUCKET')
TEMP_BUCKET = os.getenv('TEMP_BUCKET')
AIR_QUALITY_PREFIX = os.getenv('AIR_QUALITY_PREFIX')
WEATHER_PREFIX = os.getenv('WEATHER_PREFIX')

# BigQuery Tables
AIR_QUALITY_FACT_TABLE = os.getenv('AIR_QUALITY_FACT_TABLE')
WEATHER_FACT_TABLE = os.getenv('WEATHER_FACT_TABLE')
AIR_QUALITY_STATUS_DIM_TABLE = os.getenv('AIR_QUALITY_STATUS_DIM_TABLE')
LOCATION_DIM_TABLE = os.getenv('LOCATION_DIM_TABLE')
TIME_DIM_TABLE = os.getenv('TIME_DIM_TABLE')
WEATHER_CONDITION_DIM_TABLE = os.getenv('WEATHER_CONDITION_DIM_TABLE')
HARMONIZED_DATA_TABLE = os.getenv('HARMONIZED_DATA_TABLE')

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

def create_spark_session() -> SparkSession:
    """Create and configure Spark session for both streaming and batch."""
    return (SparkSession.builder
            .appName("WeatherAirQualityIntegrated")
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
            .config("spark.sql.streaming.checkpointLocation", f"gs://{TEMP_BUCKET}/checkpoints")
            .config("spark.jars.packages", "org.apache.spark:spark-streaming-pubsub_2.12:3.4.0")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.sql.broadcastTimeout", "600")
            .getOrCreate())

# Schema definitions for streaming data
def create_air_quality_schema():
    """Create schema for air quality data."""
    return StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("pm2_5", FloatType(), True),
        StructField("pm10", FloatType(), True),
        StructField("carbon_monoxide", FloatType(), True),
        StructField("nitrogen_dioxide", FloatType(), True),
        StructField("sulphur_dioxide", FloatType(), True),
        StructField("ozone", FloatType(), True),
        StructField("us_aqi", IntegerType(), True),
        StructField("aerosol_optical_depth", FloatType(), True),
        StructField("dust", FloatType(), True),
        StructField("uv_index", FloatType(), True)
    ])

def create_weather_schema():
    """Create schema for weather data."""
    return StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("temperature_2m", FloatType(), True),
        StructField("apparent_temperature", FloatType(), True),
        StructField("precipitation", FloatType(), True),
        StructField("wind_speed_10m", FloatType(), True),
        StructField("wind_direction_10m", FloatType(), True),
        StructField("relative_humidity_2m", FloatType(), True),
        StructField("surface_pressure", FloatType(), True),
        StructField("cloud_cover", IntegerType(), True),
        StructField("visibility", FloatType(), True),
        StructField("weather_code", IntegerType(), True)
    ])


def get_season(date: datetime.datetime) -> str:
    """Determine season based on month."""
    month = date.month
    if month in (12, 1, 2):
        return 'Winter'
    elif month in (3, 4, 5):
        return 'Spring'
    elif month in (6, 7, 8):
        return 'Summer'
    else:
        return 'Fall'

def is_holiday(date: datetime.datetime) -> bool:
    """Check if given date is a US holiday in 2024 or 2025."""
    if date.year not in HOLIDAYS:
        return False
    return (date.month, date.day) in HOLIDAYS[date.year]

def get_weather_condition(code: int) -> Dict:
    """Map weather code to condition details."""
    conditions = {
        0: {'description': 'Clear sky', 'category': 'Clear', 'severity_level': 0},
        1: {'description': 'Mainly clear', 'category': 'Clear', 'severity_level': 0},
        2: {'description': 'Partly cloudy', 'category': 'Clouds', 'severity_level': 1},
        3: {'description': 'Overcast', 'category': 'Clouds', 'severity_level': 2},
        45: {'description': 'Fog', 'category': 'Fog', 'severity_level': 2},
        48: {'description': 'Depositing rime fog', 'category': 'Fog', 'severity_level': 3},
        51: {'description': 'Light drizzle', 'category': 'Drizzle', 'severity_level': 1},
        53: {'description': 'Moderate drizzle', 'category': 'Drizzle', 'severity_level': 2},
        55: {'description': 'Dense drizzle', 'category': 'Drizzle', 'severity_level': 3},
        56: {'description': 'Light freezing drizzle', 'category': 'Freezing Drizzle', 'severity_level': 2},
        57: {'description': 'Dense freezing drizzle', 'category': 'Freezing Drizzle', 'severity_level': 3},
        61: {'description': 'Slight rain', 'category': 'Rain', 'severity_level': 1},
        63: {'description': 'Moderate rain', 'category': 'Rain', 'severity_level': 2},
        65: {'description': 'Heavy rain', 'category': 'Rain', 'severity_level': 3},
        66: {'description': 'Light freezing rain', 'category': 'Freezing Rain', 'severity_level': 2},
        67: {'description': 'Heavy freezing rain', 'category': 'Freezing Rain', 'severity_level': 3},
        71: {'description': 'Slight snow fall', 'category': 'Snow', 'severity_level': 1},
        73: {'description': 'Moderate snow fall', 'category': 'Snow', 'severity_level': 2},
        75: {'description': 'Heavy snow fall', 'category': 'Snow', 'severity_level': 3},
        77: {'description': 'Snow grains', 'category': 'Snow', 'severity_level': 1},
        80: {'description': 'Slight rain showers', 'category': 'Rain', 'severity_level': 1},
        81: {'description': 'Moderate rain showers', 'category': 'Rain', 'severity_level': 2},
        82: {'description': 'Violent rain showers', 'category': 'Rain', 'severity_level': 3},
        85: {'description': 'Slight snow showers', 'category': 'Snow', 'severity_level': 1},
        86: {'description': 'Heavy snow showers', 'category': 'Snow', 'severity_level': 3},
        95: {'description': 'Thunderstorm', 'category': 'Storm', 'severity_level': 3},
        96: {'description': 'Thunderstorm with slight hail', 'category': 'Storm', 'severity_level': 4},
        99: {'description': 'Thunderstorm with heavy hail', 'category': 'Storm', 'severity_level': 5}
    }
    
    condition = conditions.get(code, {
        'description': 'Unknown',
        'category': 'Unknown',
        'severity_level': -1
    })
    
    return {
        'condition_key': code,
        'weather_code': code,
        'description': condition['description'],
        'category': condition['category'],
        'severity_level': condition['severity_level']
    }

def get_aqi_status(aqi: int) -> Dict:
    """Determine AQI status and related information."""
    if aqi <= 50:
        return {
            'status_key': hash('Good'),
            'aqi_category': 'Good',
            'health_implications': 'Air quality is satisfactory',
            'cautionary_statement': 'None',
            'color_code': 'green'
        }
    elif aqi <= 100:
        return {
            'status_key': hash('Moderate'),
            'aqi_category': 'Moderate',
            'health_implications': 'Acceptable air quality',
            'cautionary_statement': 'Unusually sensitive people should consider reducing prolonged outdoor exposure',
            'color_code': 'yellow'
        }
    elif aqi <= 150:
        return {
            'status_key': hash('Unhealthy for Sensitive Groups'),
            'aqi_category': 'Unhealthy for Sensitive Groups',
            'health_implications': 'Members of sensitive groups may experience health effects',
            'cautionary_statement': 'Active children and adults should limit prolonged outdoor exposure',
            'color_code': 'orange'
        }
    elif aqi <= 200:
        return {
            'status_key': hash('Unhealthy'),
            'aqi_category': 'Unhealthy',
            'health_implications': 'Everyone may begin to experience health effects',
            'cautionary_statement': 'Everyone should reduce outdoor activities',
            'color_code': 'red'
        }
    elif aqi <= 300:
        return {
            'status_key': hash('Very Unhealthy'),
            'aqi_category': 'Very Unhealthy',
            'health_implications': 'Health warnings of emergency conditions',
            'cautionary_statement': 'Everyone should avoid outdoor activities',
            'color_code': 'purple'
        }
    else:
        return {
            'status_key': hash('Hazardous'),
            'aqi_category': 'Hazardous',
            'health_implications': 'Health alert: everyone may experience serious health effects',
            'cautionary_statement': 'Everyone should avoid all outdoor activities',
            'color_code': 'maroon'
        }


# Streaming processing functions
def process_streaming_data(spark: SparkSession):
    """Process streaming data from Pub/Sub topics."""
    # Read from Air Quality Pub/Sub topic
    air_quality_stream = (spark.readStream
        .format("pubsub")
        .option("pubsub.project", PROJECT_ID)
        .option("pubsub.topic", AIR_QUALITY_TOPIC)
        .option("pubsub.subscription", f"{AIR_QUALITY_TOPIC}-sub")
        .option("failOnDataLoss", "false")
        .load()
        .select(from_json(col("value").cast("string"), create_air_quality_schema()).alias("data"))
        .select("data.*"))

    # Read from Weather Pub/Sub topic
    weather_stream = (spark.readStream
        .format("pubsub")
        .option("pubsub.project", PROJECT_ID)
        .option("pubsub.topic", WEATHER_TOPIC)
        .option("pubsub.subscription", f"{WEATHER_TOPIC}-sub")
        .option("failOnDataLoss", "false")
        .load()
        .select(from_json(col("value").cast("string"), create_weather_schema()).alias("data"))
        .select("data.*"))

    # Apply transformations and write streams 
    queries = []
    queries.append(process_air_quality_stream(air_quality_stream))
    queries.append(process_weather_stream(weather_stream))
    handle_dead_letter_queue(spark)
    
    return queries

def process_air_quality_stream(stream_df):
    """Process and write air quality streaming data."""
    location_key = hash(f"{LATITUDE}_{LONGITUDE}")
    
    transformed_stream = (stream_df
        .withWatermark("timestamp", "1 hour")
        .withColumn("location_key", lit(location_key))
        .withColumn("status_key", 
            udf(lambda aqi: get_aqi_status(aqi)['status_key'])("us_aqi")))
    
    # Write stream to temporary storage
    query = (transformed_stream
        .writeStream
        .outputMode("append")
        .format("parquet")
        .partitionBy("year", "month")
        .option("path", f"gs://{TEMP_BUCKET}/streaming/air_quality")
        .start())

    return query

def process_weather_stream(stream_df):
    """Process and write weather streaming data."""
    location_key = hash(f"{LATITUDE}_{LONGITUDE}")
    
    transformed_stream = (stream_df
        .withWatermark("timestamp", "1 hour")
        .withColumn("location_key", lit(location_key))
        .withColumn("condition_key", col("weather_code")))
    
    # Write stream to temporary storage
    query = (transformed_stream
        .writeStream
        .outputMode("append")
        .format("parquet")
        .partitionBy("year", "month")
        .option("path", f"gs://{TEMP_BUCKET}/streaming/weather")
        .start())

    return query

def handle_dead_letter_queue(spark: SparkSession):
    """Handle failed messages using dead letter queue."""
    def process_batch(batch_df, batch_id):
        (batch_df.write
            .format("pubsub")
            .option("pubsub.project", PROJECT_ID)
            .option("pubsub.topic", DEAD_LETTER_TOPIC)
            .save())

    failed_messages = (spark.readStream
        .format("pubsub")
        .option("pubsub.project", PROJECT_ID)
        .option("pubsub.subscription", f"{DEAD_LETTER_TOPIC}-sub")
        .load())

    (failed_messages.writeStream
        .foreachBatch(process_batch)
        .start())


def validate_data(df, table_name: str) -> None:
    """
    Validate data quality and log statistics.
    """
    total_rows = df.count()
    null_counts = df.select([sum(col(c).isNull().cast("int")).alias(c) 
                           for c in df.columns]).collect()[0]
    
    logger.info(f"Data validation for {table_name}:")
    logger.info(f"Total rows: {total_rows}")
    logger.info("Null counts by column:")
    
    for col_name in df.columns:
        null_pct = (null_counts[col_name] / total_rows) * 100
        logger.info(f"{col_name}: {null_pct:.2f}%")
        
        # Raise warning if null percentage is high
        if null_pct > 10:
            logger.warning(f"High null percentage ({null_pct:.2f}%) in column {col_name} of {table_name}")

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def write_to_bigquery(df, table_id: str, write_mode: str = "overwrite") -> None:
    """
    Write DataFrame to BigQuery table with retry logic.
    """
    try:
        full_table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"
        logger.info(f"Writing data to BigQuery table: {full_table_id}")
        
        df.write.format('bigquery') \
            .option('table', full_table_id) \
            .mode(write_mode) \
            .save()
        
        logger.info(f"Successfully wrote data to {full_table_id}")
        
    except Exception as e:
        logger.error(f"Error writing to BigQuery table {table_id}: {str(e)}")
        raise

def write_dimension_tables(time_dim_df, location_dim_df, 
                         weather_condition_dim_df, air_quality_status_dim_df):
    """Write all dimension tables to BigQuery."""
    logger.info("Writing dimension tables to BigQuery")
    
    write_to_bigquery(time_dim_df, TIME_DIM_TABLE)
    write_to_bigquery(location_dim_df, LOCATION_DIM_TABLE)
    write_to_bigquery(weather_condition_dim_df, WEATHER_CONDITION_DIM_TABLE)
    write_to_bigquery(air_quality_status_dim_df, AIR_QUALITY_STATUS_DIM_TABLE)

def write_fact_tables(air_quality_fact_df, weather_fact_df):
    """Write fact tables to BigQuery."""
    logger.info("Writing fact tables to BigQuery")
    
    write_to_bigquery(air_quality_fact_df, AIR_QUALITY_FACT_TABLE)
    write_to_bigquery(weather_fact_df, WEATHER_FACT_TABLE)
    
def write_harmonized_data(harmonized_df):
    """Write harmonized data to both GCS and BigQuery."""
    logger.info("Writing harmonized data to GCS and BigQuery")
    
    # Write to GCS with partitioning
    harmonized_df.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(f"gs://{TEMP_BUCKET}/harmonized_data")
    
    # Write to BigQuery
    write_to_bigquery(harmonized_df, HARMONIZED_DATA_TABLE)

def create_time_dimension(df):
    """Create time dimension from timestamp column."""
    time_dim = (df.select(col("timestamp"))
            .distinct()
            .withColumn("time_key", 
                expr("cast(date_format(timestamp, 'yyyyMMddHH') as int)"))
            .withColumn("full_date", col("timestamp").cast("date"))
            .withColumn("year", year("timestamp"))
            .withColumn("quarter", expr("ceil(month(timestamp)/3)"))
            .withColumn("month", month("timestamp"))
            .withColumn("day", dayofmonth("timestamp"))
            .withColumn("hour", hour("timestamp"))
            .withColumn("day_of_week", dayofweek("timestamp"))
            .withColumn("is_weekend", 
                expr("dayofweek(timestamp) in (1,7)"))
            .withColumn("season", expr("case " +
                "when month in (12,1,2) then 'Winter' " +
                "when month in (3,4,5) then 'Spring' " +
                "when month in (6,7,8) then 'Summer' " +
                "else 'Fall' end"))
            .withColumn("is_holiday", 
                udf(is_holiday, BooleanType())("timestamp")))
    
    validate_data(time_dim, "TimeDim")
    return time_dim

def create_location_dimension(spark):
    """Create location dimension with fixed location data."""
    location_dim = spark.createDataFrame([
        {
            'location_key': hash(f"{LATITUDE}_{LONGITUDE}"),
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'city': 'New York',
            'region': 'Northeast',
            'climate_zone': 'Humid subtropical',
            'elevation': 10.0
        }
    ])
    
    validate_data(location_dim, "LocationDim")
    return location_dim

def create_weather_condition_dimension(weather_df):
    """Create weather condition dimension from weather codes."""
    weather_codes = weather_df.select("weather_code").distinct()
    get_condition_udf = udf(get_weather_condition, 
        MapType(StringType(), StringType()))
    
    weather_condition_dim = (weather_codes
            .withColumn("condition_info", get_condition_udf("weather_code"))
            .select(
                col("condition_info.condition_key"),
                col("weather_code"),
                col("condition_info.description"),
                col("condition_info.category"),
                col("condition_info.severity_level")
            ))
    
    validate_data(weather_condition_dim, "WeatherConditionDim")
    return weather_condition_dim

def create_air_quality_status_dimension(df):
    """Create air quality status dimension from AQI values."""
    aqi_values = df.select("us_aqi").distinct()
    get_status_udf = udf(get_aqi_status, MapType(StringType(), StringType()))
    
    air_quality_status_dim = (aqi_values
            .withColumn("status_info", get_status_udf("us_aqi"))
            .select(
                col("status_info.status_key"),
                col("status_info.aqi_category"),
                col("status_info.health_implications"),
                col("status_info.cautionary_statement"),
                col("status_info.color_code")
            ))
    
    validate_data(air_quality_status_dim, "AirQualityStatusDim")
    return air_quality_status_dim

def create_air_quality_facts(df):
    """Create air quality fact table."""
    location_key = hash(f"{LATITUDE}_{LONGITUDE}")
    
    air_quality_fact = (df
            .withColumn("location_key", lit(location_key))
            .withColumn("status_key", 
                udf(lambda aqi: get_aqi_status(aqi)['status_key'])("us_aqi"))
            .select(
                "timestamp", "location_key", "status_key",
                "pm2_5", "pm10", "carbon_monoxide", "nitrogen_dioxide",
                "sulphur_dioxide", "ozone", "us_aqi", "aerosol_optical_depth",
                "dust", "uv_index"
            ))
    
    validate_data(air_quality_fact, "AirQualityFact")
    return air_quality_fact

def create_weather_facts(df):
    """Create weather fact table."""
    location_key = hash(f"{LATITUDE}_{LONGITUDE}")
    
    weather_fact = (df
            .withColumn("location_key", lit(location_key))
            .withColumn("condition_key", col("weather_code"))
            .select(
                "timestamp", "location_key", "condition_key",
                "temperature_2m", "apparent_temperature", "precipitation",
                "wind_speed_10m", "wind_direction_10m", "relative_humidity_2m",
                "surface_pressure", "cloud_cover", "visibility"
            ))
    
    validate_data(weather_fact, "WeatherFact")
    return weather_fact

def create_harmonized_data(air_quality_df, weather_df, time_dim_df, location_dim_df):
    """Create harmonized data combining weather and air quality data."""
    # Join with dimension tables
    weather_features = (weather_df
        .join(time_dim_df, "timestamp")
        .join(location_dim_df, "location_key"))
    
    air_quality_features = (air_quality_df
        .join(time_dim_df, "timestamp")
        .join(location_dim_df, "location_key"))
    
    # Combine features and partition by year and month
    harmonized_df = (weather_features
        .join(air_quality_features, ["timestamp", "location_key"], "outer")
        .select(
            "timestamp", "location_key",
            # Weather features
            "temperature_2m", "apparent_temperature", "precipitation",
            "wind_speed_10m", "wind_direction_10m", "relative_humidity_2m",
            "surface_pressure", "cloud_cover", "visibility",
            # Air quality features
            "pm2_5", "pm10", "carbon_monoxide", "nitrogen_dioxide",
            "sulphur_dioxide", "ozone", "us_aqi", "aerosol_optical_depth",
            "dust", "uv_index",
            # Time features
            "year", "month", "hour", "day_of_week", "is_weekend", "season",
            # Location features
            "latitude", "longitude", "elevation"
        )
        .repartition("year", "month"))
    
    validate_data(harmonized_df, "HarmonizedData")
    return harmonized_df


def process_batch_data(spark: SparkSession):
    """Process historical batch data from GCS."""
    try:
        # Read historical data and streaming data
        air_quality_batch = read_air_quality_data(spark)
        weather_batch = read_weather_data(spark)
        
        # Process dimensions
        time_dim_df = create_time_dimension(air_quality_batch.union(weather_batch))
        location_dim_df = create_location_dimension(spark)
        weather_condition_dim_df = create_weather_condition_dimension(weather_batch)
        air_quality_status_dim_df = create_air_quality_status_dimension(air_quality_batch)
        
        # Process facts
        air_quality_fact_df = create_air_quality_facts(air_quality_batch)
        weather_fact_df = create_weather_facts(weather_batch)
        
        # Create harmonized data
        harmonized_df = create_harmonized_data(
            air_quality_batch, weather_batch, 
            time_dim_df, location_dim_df
        )
        
        # Write to BigQuery
        write_dimension_tables(
            time_dim_df, location_dim_df,
            weather_condition_dim_df, air_quality_status_dim_df
        )
        write_fact_tables(air_quality_fact_df, weather_fact_df)
        write_harmonized_data(harmonized_df)
        
    except Exception as e:
        logger.error(f"Error in batch processing: {str(e)}")
        raise

def read_air_quality_data(spark: SparkSession):
    """Read air quality data from both batch and streaming sources."""
    batch_path = f"gs://{HISTORICAL_BUCKET}/{AIR_QUALITY_PREFIX}/*.csv"
    streaming_path = f"gs://{TEMP_BUCKET}/streaming/air_quality"
    
    batch_data = spark.read.csv(batch_path, header=True)
    streaming_data = spark.read.parquet(streaming_path)
    
    return batch_data.union(streaming_data)

def read_weather_data(spark: SparkSession):
    """Read weather data from both batch and streaming sources."""
    batch_path = f"gs://{HISTORICAL_BUCKET}/{WEATHER_PREFIX}/*.csv"
    streaming_path = f"gs://{TEMP_BUCKET}/streaming/weather"
    
    batch_data = spark.read.csv(batch_path, header=True)
    streaming_data = spark.read.parquet(streaming_path)
    
    return batch_data.union(streaming_data)

def process_data(spark: SparkSession):
    """Main data processing function combining streaming and batch."""
    try:
        # Start streaming process
        streaming_queries = []
        
        # Process streaming data
        logger.info("Starting streaming processing")
        streaming_queries.extend(process_streaming_data(spark))
        
        # Process batch data periodically
        logger.info("Starting batch processing")
        process_batch_data(spark)
        
        # Wait for streaming queries to terminate
        for query in streaming_queries:
            if query:
                query.awaitTermination()
                
    except Exception as e:
        logger.error(f"Error in data processing: {str(e)}")
        raise
    finally:
        # Stop all streaming queries
        for query in streaming_queries:
            if query and query.isActive:
                query.stop()

def main():
    """Main entry point for the integrated job."""
    logger.info("Starting Weather and Air Quality data integration job")
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Process both streaming and batch data
        process_data(spark)
        
        logger.info("Data processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
        raise
        
    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    main()