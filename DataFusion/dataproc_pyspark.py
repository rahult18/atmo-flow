from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, from_unixtime, unix_timestamp, 
    year, month, dayofmonth, hour, dayofweek, 
    when, lit, expr, udf, sum
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    FloatType, BooleanType, TimestampType, MapType
)
from pyspark.sql.utils import AnalysisException
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

# GCP Project Configuration
PROJECT_ID = os.getenv('PROJECT_ID')
REGION = os.getenv('REGION')
DATASET_ID = os.getenv('DATASET_ID')

# Location Configuration
LATITUDE = float(os.getenv('LATITUDE'))
LONGITUDE = float(os.getenv('LONGITUDE'))

# GCS Configuration
HISTORICAL_BUCKET = os.getenv('HISTORICAL_BUCKET')
TEMP_BUCKET = os.getenv('TEMP_BUCKET')
AIR_QUALITY_PREFIX = os.getenv('AIR_QUALITY_PREFIX')
WEATHER_PREFIX = os.getenv('WEATHER_PREFIX')

# BigQuery Table Configuration
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
    """Create and configure Spark session with necessary settings."""
    return (SparkSession.builder
            .appName("WeatherAirQualityHarmonization")
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.sql.broadcastTimeout", "600")
            .getOrCreate())

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

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def process_batch_data(spark: SparkSession):
    """Process historical batch data from GCS."""
    try:
        # Read historical data
        air_quality_path = f"gs://{HISTORICAL_BUCKET}/{AIR_QUALITY_PREFIX}/*.csv"
        weather_path = f"gs://{HISTORICAL_BUCKET}/{WEATHER_PREFIX}/*.csv"
        
        logger.info("Reading air quality data from GCS")
        air_quality_df = spark.read.csv(air_quality_path, header=True)
        
        logger.info("Reading weather data from GCS")
        weather_df = spark.read.csv(weather_path, header=True)
        
        # Process dimensions
        logger.info("Creating dimension tables")
        time_dim_df = create_time_dimension(air_quality_df.union(weather_df))
        location_dim_df = create_location_dimension(spark)
        weather_condition_dim_df = create_weather_condition_dimension(weather_df)
        air_quality_status_dim_df = create_air_quality_status_dimension(air_quality_df)
        
        # Process facts
        logger.info("Creating fact tables")
        air_quality_fact_df = create_air_quality_facts(air_quality_df)
        weather_fact_df = create_weather_facts(weather_df)
        
        # Create harmonized data
        logger.info("Creating harmonized data")
        harmonized_df = create_harmonized_data(
            air_quality_df, weather_df, 
            time_dim_df, location_dim_df
        )
        
        # Write dimension tables to BigQuery
        logger.info("Writing dimension tables to BigQuery")
        write_to_bigquery(time_dim_df, TIME_DIM_TABLE)
        write_to_bigquery(location_dim_df, LOCATION_DIM_TABLE)
        write_to_bigquery(weather_condition_dim_df, WEATHER_CONDITION_DIM_TABLE)
        write_to_bigquery(air_quality_status_dim_df, AIR_QUALITY_STATUS_DIM_TABLE)
        
        # Write fact tables to BigQuery
        logger.info("Writing fact tables to BigQuery")
        write_to_bigquery(air_quality_fact_df, AIR_QUALITY_FACT_TABLE)
        write_to_bigquery(weather_fact_df, WEATHER_FACT_TABLE)
        
        # Write harmonized data to both GCS and BigQuery
        logger.info("Writing harmonized data to GCS and BigQuery")
        harmonized_df.write \
            .mode("overwrite") \
            .partitionBy("year", "month") \
            .parquet(f"gs://{TEMP_BUCKET}/harmonized_data")
        
        write_to_bigquery(harmonized_df, HARMONIZED_DATA_TABLE)
        
        logger.info("Batch processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in batch processing: {str(e)}")
        raise

def main():
    """Main entry point for the Spark job."""
    logger.info("Starting Weather and Air Quality data harmonization job")
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Process batch data
        process_batch_data(spark)
        
        logger.info("Data processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
        raise
        
    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    main()