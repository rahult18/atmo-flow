from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, dayofmonth, hour, dayofweek, 
    when, lit, expr, udf, sum, date_format, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    FloatType, BooleanType, TimestampType, MapType
)
import datetime
import logging
from typing import Dict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

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

# GCP and Location Configuration
def get_config_from_env(spark: SparkSession):
    """Get configuration values from Spark environment variables."""
    config = {
        'PROJECT_ID': spark.conf.get("spark.project.id"),
        'REGION': spark.conf.get("spark.region"),
        'DATASET_ID': spark.conf.get("spark.dataset.id"),
        'LATITUDE': float(spark.conf.get("spark.location.latitude")),
        'LONGITUDE': float(spark.conf.get("spark.location.longitude")),
        
        # GCS Configuration
        'HISTORICAL_BUCKET': spark.conf.get("spark.bucket.historical"),
        'TEMP_BUCKET': spark.conf.get("spark.bucket.temp"),
        'AIR_QUALITY_PREFIX': spark.conf.get("spark.prefix.air_quality"),
        'WEATHER_PREFIX': spark.conf.get("spark.prefix.weather"),
        
        # BigQuery Tables
        'AIR_QUALITY_FACT_TABLE': spark.conf.get("spark.table.air_quality_fact"),
        'WEATHER_FACT_TABLE': spark.conf.get("spark.table.weather_fact"),
        'AIR_QUALITY_STATUS_DIM_TABLE': spark.conf.get("spark.table.air_quality_status_dim"),
        'LOCATION_DIM_TABLE': spark.conf.get("spark.table.location_dim"),
        'TIME_DIM_TABLE': spark.conf.get("spark.table.time_dim"),
        'WEATHER_CONDITION_DIM_TABLE': spark.conf.get("spark.table.weather_condition_dim"),
        'HARMONIZED_DATA_TABLE': spark.conf.get("spark.table.harmonized_data")
    }
    return config

def create_spark_session() -> SparkSession:
    """Create and configure Spark session for batch processing."""
    return (SparkSession.builder
            .appName("WeatherAirQualityBatch")
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

def is_holiday(date_str: str) -> bool:
    """Check if given date string is a US holiday in 2024 or 2025."""
    try:
        # Convert string to datetime object
        date = datetime.datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
        if date.year not in HOLIDAYS:
            return False
        return (date.month, date.day) in HOLIDAYS[date.year]
    except (ValueError, TypeError):
        return False

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

def get_aqi_status(aqi) -> Dict:
    """
    Determine AQI status and related information.
    
    Args:
        aqi: Air Quality Index value (can be string or int)
    Returns:
        Dict containing AQI status information
    """
    try:
        # Convert aqi to integer if it's a string
        aqi_value = int(float(aqi)) if isinstance(aqi, str) else aqi
        
        if aqi_value <= 50:
            return {
                'status_key': hash('Good'),
                'aqi_category': 'Good',
                'health_implications': 'Air quality is satisfactory',
                'cautionary_statement': 'None',
                'color_code': 'green'
            }
        elif aqi_value <= 100:
            return {
                'status_key': hash('Moderate'),
                'aqi_category': 'Moderate',
                'health_implications': 'Acceptable air quality',
                'cautionary_statement': 'Unusually sensitive people should consider reducing prolonged outdoor exposure',
                'color_code': 'yellow'
            }
        elif aqi_value <= 150:
            return {
                'status_key': hash('Unhealthy for Sensitive Groups'),
                'aqi_category': 'Unhealthy for Sensitive Groups',
                'health_implications': 'Members of sensitive groups may experience health effects',
                'cautionary_statement': 'Active children and adults should limit prolonged outdoor exposure',
                'color_code': 'orange'
            }
        elif aqi_value <= 200:
            return {
                'status_key': hash('Unhealthy'),
                'aqi_category': 'Unhealthy',
                'health_implications': 'Everyone may begin to experience health effects',
                'cautionary_statement': 'Everyone should reduce outdoor activities',
                'color_code': 'red'
            }
        elif aqi_value <= 300:
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
    except (ValueError, TypeError) as e:
        logger.warning(f"Invalid AQI value: {aqi}. Error: {str(e)}")
        return {
            'status_key': hash('Unknown'),
            'aqi_category': 'Unknown',
            'health_implications': 'Unable to determine health implications',
            'cautionary_statement': 'Unable to provide cautionary statement',
            'color_code': 'gray'
        }

def validate_data(df, table_name: str) -> None:
    """Validate data quality and log statistics."""
    total_rows = df.count()
    null_counts = df.select([sum(col(c).isNull().cast("int")).alias(c) 
                           for c in df.columns]).collect()[0]
    
    logger.info(f"Data validation for {table_name}:")
    logger.info(f"Total rows: {total_rows}")
    logger.info("Null counts by column:")
    
    for col_name in df.columns:
        null_pct = (null_counts[col_name] / total_rows) * 100
        logger.info(f"{col_name}: {null_pct:.2f}%")
        
        if null_pct > 10:
            logger.warning(f"High null percentage ({null_pct:.2f}%) in column {col_name} of {table_name}")


def write_to_bigquery(df, table_id: str, write_mode: str = "overwrite") -> None:
    """Write DataFrame to BigQuery table with retry logic."""
    try:
        full_table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_id}"
        logger.info(f"Writing data to BigQuery table: {full_table_id}")
        
        df.write.format('bigquery') \
            .option('table', full_table_id) \
            .option('temporaryGcsBucket', TEMP_BUCKET) \
            .mode(write_mode) \
            .save()
        
        logger.info(f"Successfully wrote data to {full_table_id}")
        
    except Exception as e:
        logger.error(f"Error writing to BigQuery table {table_id}: {str(e)}")
        raise

def write_dimension_tables(time_dim_df, location_dim_df, 
                         weather_condition_dim_df, air_quality_status_dim_df,
                         season_dim_df, severity_dim_df):
    """Write all dimension tables to BigQuery."""
    logger.info("Writing dimension tables to BigQuery")
    
    write_to_bigquery(time_dim_df, TIME_DIM_TABLE)
    write_to_bigquery(location_dim_df, LOCATION_DIM_TABLE)
    write_to_bigquery(weather_condition_dim_df, WEATHER_CONDITION_DIM_TABLE)
    write_to_bigquery(air_quality_status_dim_df, AIR_QUALITY_STATUS_DIM_TABLE)
    write_to_bigquery(season_dim_df, "SeasonDim")
    write_to_bigquery(severity_dim_df, "SeverityDim")

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

def create_season_dimension(spark):
    """Create season dimension with detailed characteristics."""
    seasons_data = [
        (1, "Winter", 12, 2, 9.5, "-5°C to 5°C", "Snow, frost, cold winds"),
        (2, "Spring", 3, 5, 12.5, "5°C to 20°C", "Rain showers, mild temperatures"),
        (3, "Summer", 6, 8, 14.5, "20°C to 30°C", "Warm, thunderstorms, humid"),
        (4, "Fall", 9, 11, 10.5, "5°C to 20°C", "Cool, windy, rain")
    ]
    
    season_schema = StructType([
        StructField("season_key", IntegerType(), False),
        StructField("season_name", StringType(), False),
        StructField("start_month", IntegerType(), False),
        StructField("end_month", IntegerType(), False),
        StructField("avg_daylight_hours", FloatType(), True),
        StructField("typical_temp_range", StringType(), True),
        StructField("characteristic_weather", StringType(), True)
    ])
    
    season_dim = spark.createDataFrame(seasons_data, season_schema)
    validate_data(season_dim, "SeasonDim")
    return season_dim

def create_severity_dimension(spark):
    """Create shared severity dimension for weather and air quality."""
    severity_data = [
        (1, 1, "Minor", "Minimal impact", "No specific actions needed", "Info"),
        (2, 2, "Moderate", "Some impact", "Be aware and monitor", "Watch"),
        (3, 3, "Significant", "Notable impact", "Take precautions", "Advisory"),
        (4, 4, "Severe", "Major impact", "Take protective actions", "Warning"),
        (5, 5, "Extreme", "Critical impact", "Immediate action required", "Emergency")
    ]
    
    severity_schema = StructType([
        StructField("severity_key", IntegerType(), False),
        StructField("severity_level", IntegerType(), False),
        StructField("severity_name", StringType(), False),
        StructField("description", StringType(), False),
        StructField("recommended_actions", StringType(), False),
        StructField("alert_level", StringType(), False)
    ])
    
    severity_dim = spark.createDataFrame(severity_data, severity_schema)
    validate_data(severity_dim, "SeverityDim")
    return severity_dim

def create_time_dimension(air_quality_df, weather_df):
    """Create time dimension from timestamp column of both dataframes."""
    # Select only timestamp columns
    air_quality_times = air_quality_df.select("timestamp")
    weather_times = weather_df.select("timestamp")
    
    # Union the timestamp columns and continue with the rest of the processing
    time_dim = (air_quality_times.union(weather_times)
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
            # Format timestamp as string for is_holiday UDF
            .withColumn("date_str", 
                date_format(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
            .withColumn("is_holiday", 
                udf(is_holiday, BooleanType())("date_str"))
            .drop("date_str"))  # Remove temporary column
    
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
    # First cast us_aqi column to double to handle potential decimals
    df = df.withColumn("us_aqi", col("us_aqi").cast("double"))
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
    """Create enhanced weather fact table."""
    location_key = hash(f"{LATITUDE}_{LONGITUDE}")
    
    # Get season key based on month
    def get_season_key(month):
        if month in (12, 1, 2):
            return 1  # Winter
        elif month in (3, 4, 5):
            return 2  # Spring
        elif month in (6, 7, 8):
            return 3  # Summer
        else:
            return 4  # Fall
    
    get_season_key_udf = udf(get_season_key, IntegerType())
    
    weather_fact = (df
        .withColumn("location_key", lit(location_key))
        .withColumn("condition_key", col("weather_code").cast("integer"))
        .withColumn("severity_key", 
            when(col("weather_code").isin([95, 96, 99]), 5)  # Extreme
            .when(col("weather_code").isin([65, 67, 75, 82, 86]), 4)  # Severe
            .when(col("weather_code").isin([63, 73, 81]), 3)  # Significant
            .when(col("weather_code").isin([51, 53, 61, 71, 80, 85]), 2)  # Moderate
            .otherwise(1))  # Minor
        .withColumn("season_key", 
            get_season_key_udf(month("timestamp")))
        .select(
            "timestamp", "location_key", "condition_key", "severity_key", "season_key",
            "temperature_2m", "apparent_temperature", "precipitation", "snowfall",
            "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m",
            "relative_humidity_2m", "surface_pressure",
            "cloud_cover", "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high",
            "visibility", "precipitation_probability"
        ))
    
    validate_data(weather_fact, "WeatherFact")
    return weather_fact

def create_harmonized_data(air_quality_df, weather_df, time_dim_df, location_dim_df):
    """Create harmonized data combining weather and air quality data."""
    # Calculate location key
    location_key = hash(f"{LATITUDE}_{LONGITUDE}")
    
    # Add location_key to weather features and select specific columns
    weather_features = (weather_df
        .join(time_dim_df, "timestamp")
        .withColumn("location_key", lit(location_key))
        .select(
            "timestamp", "location_key",
            "temperature_2m", "apparent_temperature", "precipitation",
            "wind_speed_10m", "wind_direction_10m", "relative_humidity_2m",
            "surface_pressure", "cloud_cover", "visibility"
        ))
    
    # Add location_key to air quality features and select specific columns
    air_quality_features = (air_quality_df
        .join(time_dim_df, "timestamp")
        .withColumn("location_key", lit(location_key))
        .select(
            "timestamp", "location_key",
            "pm2_5", "pm10", "carbon_monoxide", "nitrogen_dioxide",
            "sulphur_dioxide", "ozone", "us_aqi", "aerosol_optical_depth",
            "dust", "uv_index"
        ))
    
    # Get time features separately with unique column names
    time_features = time_dim_df.select(
        "timestamp",
        "year", "month", "hour", "day_of_week",
        "is_weekend", "season"
    )
    
    # Get location features
    location_features = location_dim_df.select(
        "location_key",
        "latitude", "longitude", "elevation"
    )
    
    # Combine all features
    harmonized_df = (weather_features
        .join(air_quality_features, ["timestamp", "location_key"], "outer")
        .join(time_features, "timestamp")
        .join(location_features, "location_key")
        .repartition("year", "month"))
    
    validate_data(harmonized_df, "HarmonizedData")
    return harmonized_df

def read_historical_data(spark: SparkSession):
    """Read historical data from GCS and standardize timestamp format."""
    # Define schema for air quality data
    air_quality_schema = StructType([
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

    # Define schema for weather data
    weather_schema = StructType([
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

    # Read air quality data with schema
    air_quality_path = f"gs://{HISTORICAL_BUCKET}/{AIR_QUALITY_PREFIX}/*.csv"
    air_quality_df = (spark.read
        .option("header", True)
        .schema(air_quality_schema)
        .csv(air_quality_path)
        .withColumn("timestamp", to_timestamp(col("time")))
        .drop("time"))

    # Read weather data with schema
    weather_path = f"gs://{HISTORICAL_BUCKET}/{WEATHER_PREFIX}/*.csv"
    weather_df = (spark.read
        .option("header", True)
        .schema(weather_schema)
        .csv(weather_path)
        .withColumn("timestamp", to_timestamp(col("time")))
        .drop("time"))

    return air_quality_df, weather_df

def process_batch_data(spark: SparkSession):
    """Process historical batch data from GCS."""
    try:
        # Read historical data
        air_quality_batch, weather_batch = read_historical_data(spark)
        
        # Process dimensions
        time_dim_df = create_time_dimension(air_quality_batch, weather_batch)
        location_dim_df = create_location_dimension(spark)
        weather_condition_dim_df = create_weather_condition_dimension(weather_batch)
        air_quality_status_dim_df = create_air_quality_status_dimension(air_quality_batch)
        season_dim_df = create_season_dimension(spark)
        severity_dim_df = create_severity_dimension(spark)
        
        # Process facts
        air_quality_fact_df = create_air_quality_facts(air_quality_batch)
        weather_fact_df = create_weather_facts(weather_batch)
        
        # Write dimensions
        write_dimension_tables(
            time_dim_df, location_dim_df,
            weather_condition_dim_df, air_quality_status_dim_df,
            season_dim_df, severity_dim_df
        )
        
        # Write facts
        write_fact_tables(air_quality_fact_df, weather_fact_df)
        
        # Create and write harmonized data
        harmonized_df = create_harmonized_data(
            air_quality_batch, weather_batch, 
            time_dim_df, location_dim_df
        )
        write_harmonized_data(harmonized_df)
        
    except Exception as e:
        logger.error(f"Error in batch processing: {str(e)}")
        raise

def main():
    """Main entry point for the batch job."""
    logger.info("Starting Weather and Air Quality data integration batch job")
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Get configuration from environment
        config = get_config_from_env(spark)
        
        # Update global variables with config values
        globals().update(config)
        
        # Process batch data
        process_batch_data(spark)
        logger.info("Batch processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
        raise
        
    finally:
        # Stop Spark session
        spark.stop()

if __name__ == "__main__":
    main()