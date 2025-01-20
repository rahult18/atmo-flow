import openmeteo_requests
import requests_cache
import pandas as pd
from datetime import datetime, timedelta
from openmeteo_sdk.Variable import Variable
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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

# Calculate dates for last 6 months (excluding today)
end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
start_date = end_date - timedelta(days=180)

# Weather API parameters
weather_params = {
    "latitude": latitude,
    "longitude": longitude,
    "start_date": start_date.strftime("%Y-%m-%d"),
    "end_date": end_date.strftime("%Y-%m-%d"),
    "hourly": [
        "temperature_2m",
        "relative_humidity_2m",
        "apparent_temperature",
        "precipitation",
        "rain",
        "snowfall",
        "cloud_cover",
        "cloud_cover_low",
        "cloud_cover_mid", 
        "cloud_cover_high",
        "surface_pressure",
        "visibility",
        "wind_speed_10m",
        "wind_direction_10m",
        "wind_gusts_10m",
        "weather_code",
        "precipitation_probability"
    ],
    "timezone": "America/New_York"
}

def fetch_weather_data():
    """Fetch and process historical weather data"""
    print("Fetching weather data...")
    weather_responses = om.weather_api("https://archive-api.open-meteo.com/v1/archive", params=weather_params)
    weather_response = weather_responses[0]
    weather_daily = weather_response.Hourly()
    
    # Create time range with hourly frequency
    weather_data = {
        "time": pd.date_range(
            start=pd.to_datetime(weather_daily.Time(), unit="s"),
            end=pd.to_datetime(weather_daily.TimeEnd(), unit="s"),
            freq=pd.Timedelta(hours=1),
            inclusive="left"
        )
    }
    
    # Get all available variables and their values
    for i in range(weather_daily.VariablesLength()):
        var = weather_daily.Variables(i)
        var_name = weather_params["hourly"][i]
        values = var.ValuesAsNumpy()
        weather_data[var_name] = values
    
    # Create DataFrame
    df = pd.DataFrame(data=weather_data)
    
    # Convert specific columns to appropriate types with NaN handling
    if 'weather_code' in df.columns:
        # Fill NaN values with -1 (or another suitable default value) before converting to int
        df['weather_code'] = df['weather_code'].fillna(-1).astype(int)
    
    if 'daylight_duration' in df.columns:
        df['daylight_duration'] = df['daylight_duration'].round(2)
    
    return df

def main():
    # Fetch weather data
    weather_df = fetch_weather_data()
    
    # Save to CSV file
    date_suffix = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
    weather_filename = f"Weather/nyc_weather_{date_suffix}.csv"
    
    weather_df.to_csv(weather_filename, index=False)
    
    print(f"Weather data saved to: {weather_filename}")
    
    return weather_df

if __name__ == "__main__":
    weather_df = main()