import os
import json
from datetime import datetime, timedelta
import functions_framework
from google.cloud import storage
import requests_cache
import openmeteo_requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from openmeteo_sdk.Variable import Variable

# Load environment variables
LATITUDE = float(os.getenv('LATITUDE'))
LONGITUDE = float(os.getenv('LONGITUDE'))
BUCKET_NAME = os.getenv('BUCKET_NAME')
AIR_QUALITY_PREFIX = os.getenv('AIR_QUALITY_PREFIX')
WEATHER_PREFIX = os.getenv('WEATHER_PREFIX')

# Setup the storage client
storage_client = storage.Client()

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('/tmp/.cache', expire_after=-1)

# Configure retry strategy
retry_strategy = Retry(
    total=5,
    backoff_factor=0.5,
    status_forcelist=[429, 500, 502, 503, 504],
)

# Mount adapter with retry strategy to session
adapter = HTTPAdapter(max_retries=retry_strategy)
cache_session.mount("http://", adapter)
cache_session.mount("https://", adapter)

# Initialize OpenMeteo client
om = openmeteo_requests.Client(session=cache_session)

def upload_to_gcs(data_df, bucket_name, prefix, filename):
    """Upload DataFrame to Google Cloud Storage"""
    try:
        bucket = storage_client.bucket(bucket_name)
        blob_path = f"{prefix}/{filename}"
        blob = bucket.blob(blob_path)
        
        # Convert DataFrame to CSV string and upload
        blob.upload_from_string(
            data_df.to_csv(index=False),
            content_type='text/csv'
        )
        
        print(f"Successfully uploaded: gs://{bucket_name}/{blob_path}")
        return True
    except Exception as e:
        print(f"Error uploading to GCS: {str(e)}")
        raise

def fetch_air_quality_data(start_date, end_date):
    """Fetch historical air quality data"""
    try:
        print(f"Fetching air quality data from {start_date} to {end_date}")
        
        air_quality_params = {
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "hourly": [
                "pm10",
                "pm2_5",
                "carbon_monoxide",
                "nitrogen_dioxide",
                "sulphur_dioxide",
                "ozone",
                "aerosol_optical_depth",
                "dust",
                "uv_index",
                "us_aqi"
            ]
        }
        
        # Make API request
        url = "https://air-quality-api.open-meteo.com/v1/air-quality"
        response = cache_session.get(url, params=air_quality_params)
        response.raise_for_status()
        data = response.json()
        
        # Create DataFrame
        air_quality_df = pd.DataFrame({
            "time": pd.to_datetime(data["hourly"]["time"])
        })
        
        # Add all available metrics
        for var in air_quality_params["hourly"]:
            if var in data["hourly"]:
                air_quality_df[var] = data["hourly"][var]
        
        # Ensure proper data types
        if 'us_aqi' in air_quality_df.columns:
            air_quality_df['us_aqi'] = air_quality_df['us_aqi'].astype(int)
        
        print("Successfully fetched air quality data")
        return air_quality_df
        
    except Exception as e:
        print(f"Error fetching air quality data: {str(e)}")
        raise

def fetch_weather_data(start_date, end_date):
    """Fetch historical weather data"""
    try:
        print(f"Fetching weather data from {start_date} to {end_date}")
        
        weather_params = {
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "daily": [
                "temperature_2m_max",
                "temperature_2m_min",
                "apparent_temperature_max",
                "apparent_temperature_min",
                "precipitation_sum",
                "rain_sum",
                "snowfall_sum",
                "wind_speed_10m_max",
                "wind_gusts_10m_max",
                "wind_direction_10m_dominant",
                "daylight_duration",
                "weather_code"
            ]
        }
        
        weather_responses = om.weather_api(
            "https://archive-api.open-meteo.com/v1/archive",
            params=weather_params
        )
        weather_response = weather_responses[0]
        weather_daily = weather_response.Daily()
        
        # Create DataFrame
        weather_df = pd.DataFrame({
            "time": pd.date_range(
                start=pd.to_datetime(weather_daily.Time(), unit="s"),
                end=pd.to_datetime(weather_daily.TimeEnd(), unit="s"),
                freq=pd.Timedelta(days=1),
                inclusive="left"
            )
        })
        
        # Add all available metrics
        for i in range(weather_daily.VariablesLength()):
            var = weather_daily.Variables(i)
            var_name = weather_params["daily"][i]
            weather_df[var_name] = var.ValuesAsNumpy()
        
        # Convert data types
        if 'weather_code' in weather_df.columns:
            weather_df['weather_code'] = weather_df['weather_code'].astype(int)
        if 'daylight_duration' in weather_df.columns:
            weather_df['daylight_duration'] = weather_df['daylight_duration'].round(2)
        
        print("Successfully fetched weather data")
        return weather_df
        
    except Exception as e:
        print(f"Error fetching weather data: {str(e)}")
        raise

@functions_framework.http
def process_data(request):
    """Cloud Function entry point for processing historical data"""
    try:
        # Calculate date range (last 6 months)
        end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
        start_date = end_date - timedelta(days=180)
        date_suffix = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
        
        # Process air quality data
        air_quality_df = fetch_air_quality_data(start_date, end_date)
        air_quality_filename = f"nyc_air_quality_{date_suffix}.csv"
        upload_to_gcs(
            air_quality_df,
            BUCKET_NAME,
            AIR_QUALITY_PREFIX,
            air_quality_filename
        )
        
        # Process weather data
        weather_df = fetch_weather_data(start_date, end_date)
        weather_filename = f"nyc_weather_{date_suffix}.csv"
        upload_to_gcs(
            weather_df,
            BUCKET_NAME,
            WEATHER_PREFIX,
            weather_filename
        )
        
        return json.dumps({
            'status': 'success',
            'message': 'Historical data processed and uploaded successfully',
            'files': {
                'air_quality': f"gs://{BUCKET_NAME}/{AIR_QUALITY_PREFIX}/{air_quality_filename}",
                'weather': f"gs://{BUCKET_NAME}/{WEATHER_PREFIX}/{weather_filename}"
            }
        }), 200
        
    except Exception as e:
        error_message = str(e)
        print(f"Error processing historical data: {error_message}")
        import traceback
        traceback.print_exc()
        return json.dumps({
            'status': 'error',
            'message': error_message
        }), 500