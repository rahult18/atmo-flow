import openmeteo_requests
import requests_cache
import pandas as pd
from datetime import datetime, timedelta
from openmeteo_sdk.Variable import Variable
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Setup variable name mappings
WEATHER_VARIABLES = {
    "temperature_2m": 0,
    "relative_humidity_2m": 1,
    "precipitation": 2,
    "rain": 3,
    "showers": 4,
    "surface_pressure": 5,
    "cloud_cover": 6,
    "visibility": 7,
    "wind_speed_10m": 8,
    "wind_gusts_10m": 9
}

AIR_QUALITY_VARIABLES = {
    "pm10": 10,
    "pm2_5": 11,
    "carbon_monoxide": 12,
    "nitrogen_dioxide": 13,
    "ozone": 14
}

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after=-1)

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

om = openmeteo_requests.Client(session=cache_session)

# New York City coordinates
latitude = 40.7128
longitude = -74.0060

# Calculate dates for last 2 months
end_date = datetime.now()
start_date = end_date - timedelta(days=60)

# Weather API parameters
weather_params = {
    "latitude": latitude,
    "longitude": longitude,
    "start_date": start_date.strftime("%Y-%m-%d"),
    "end_date": end_date.strftime("%Y-%m-%d"),
    "hourly": [
        "temperature_2m",
        "relative_humidity_2m",
        "precipitation",
        "rain",
        "showers",
        "surface_pressure",
        "cloud_cover",
        "visibility",
        "wind_speed_10m",
        "wind_gusts_10m"
    ]
}

# Air Quality API parameters
air_quality_params = {
    "latitude": latitude,
    "longitude": longitude,
    "start_date": start_date.strftime("%Y-%m-%d"),
    "end_date": end_date.strftime("%Y-%m-%d"),
    "hourly": [
        "pm10",
        "pm2_5",
        "carbon_monoxide",
        "nitrogen_dioxide",
        "ozone"
    ]
}

def fetch_weather_data():
    """Fetch and process weather data"""
    print("Fetching weather data...")
    weather_responses = om.weather_api("https://archive-api.open-meteo.com/v1/archive", params=weather_params)
    weather_response = weather_responses[0]
    
    weather_hourly = weather_response.Hourly()
    weather_data = {
        "time": pd.date_range(
            start=pd.to_datetime(weather_hourly.Time(), unit="s"),
            end=pd.to_datetime(weather_hourly.TimeEnd(), unit="s"),
            freq=pd.Timedelta(seconds=weather_hourly.Interval()),
            inclusive="left"
        )
    }
    
    # Get variable IDs and their names
    variable_ids = {}
    for i in range(weather_hourly.VariablesLength()):
        var = weather_hourly.Variables(i)
        var_id = var.Variable()
        # Print for debugging
        print(f"Variable ID: {var_id}, Values: {var.ValuesAsNumpy()[:5]}")  # Show first 5 values
        variable_ids[var_id] = var.ValuesAsNumpy()

    # Map values to correct variable names based on order
    for name, values in zip(weather_params["hourly"], variable_ids.values()):
        weather_data[name] = values
    
    return pd.DataFrame(data=weather_data)

def fetch_air_quality_data():
    """Fetch and process air quality data"""
    print("\nFetching air quality data...")
    air_quality_responses = om.weather_api("https://air-quality-api.open-meteo.com/v1/air-quality", params=air_quality_params)
    air_quality_response = air_quality_responses[0]
    
    air_quality_hourly = air_quality_response.Hourly()
    air_quality_data = {
        "time": pd.date_range(
            start=pd.to_datetime(air_quality_hourly.Time(), unit="s"),
            end=pd.to_datetime(air_quality_hourly.TimeEnd(), unit="s"),
            freq=pd.Timedelta(seconds=air_quality_hourly.Interval()),
            inclusive="left"
        )
    }
    
    # Get variable IDs and their names
    variable_ids = {}
    for i in range(air_quality_hourly.VariablesLength()):
        var = air_quality_hourly.Variables(i)
        var_id = var.Variable()
        # Print for debugging
        print(f"Variable ID: {var_id}, Values: {var.ValuesAsNumpy()[:5]}")  # Show first 5 values
        variable_ids[var_id] = var.ValuesAsNumpy()

    # Map values to correct variable names based on order
    for name, values in zip(air_quality_params["hourly"], variable_ids.values()):
        air_quality_data[name] = values
    
    return pd.DataFrame(data=air_quality_data)

def main():
    # Fetch data and create separate dataframes
    weather_df = fetch_weather_data()
    air_quality_df = fetch_air_quality_data()
    
    # Print column names for verification
    print("\nWeather DataFrame columns:", weather_df.columns.tolist())
    print("Air Quality DataFrame columns:", air_quality_df.columns.tolist())
    
    # Save to separate CSV files
    date_suffix = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
    
    weather_filename = f"nyc_weather_{date_suffix}.csv"
    air_quality_filename = f"nyc_air_quality_{date_suffix}.csv"
    
    weather_df.to_csv(weather_filename, index=False)
    air_quality_df.to_csv(air_quality_filename, index=False)
    
    print(f"\nWeather data shape: {weather_df.shape}")
    print(f"Air quality data shape: {air_quality_df.shape}")
    print(f"\nWeather data saved to: {weather_filename}")
    print(f"Air quality data saved to: {air_quality_filename}")
    
    return weather_df, air_quality_df

if __name__ == "__main__":
    weather_df, air_quality_df = main()