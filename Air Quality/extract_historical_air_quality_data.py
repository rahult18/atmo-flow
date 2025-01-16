import requests_cache
import pandas as pd
from datetime import datetime, timedelta
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

# New York City coordinates
latitude = 40.7128
longitude = -74.0060

# Calculate dates for historical data (last 6 months)
end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
start_date = end_date - timedelta(days=180)

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
        "sulphur_dioxide",
        "ozone",
        "aerosol_optical_depth",
        "dust",
        "uv_index",
        "us_aqi"
    ]
}

def fetch_air_quality_data():
    """Fetch and process historical air quality data"""
    print("Fetching historical air quality data...")
    
    # Make API request
    url = "https://air-quality-api.open-meteo.com/v1/air-quality"
    response = cache_session.get(url, params=air_quality_params)
    response.raise_for_status()
    data = response.json()
    
    # Create DataFrame with time series
    weather_data = {
        "time": pd.to_datetime(data["hourly"]["time"])
    }
    
    # Add all available variables
    for var in air_quality_params["hourly"]:
        if var in data["hourly"]:
            weather_data[var] = data["hourly"][var]
    
    # Create DataFrame
    df = pd.DataFrame(data=weather_data)
        
    # Ensure proper data types
    if 'us_aqi' in df.columns:
        df['us_aqi'] = df['us_aqi'].astype(int)
    
    return df

def main():
    try:
        # Fetch air quality data
        air_quality_df = fetch_air_quality_data()
        
        # Save to CSV file
        date_suffix = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
        air_quality_filename = f"Air Quality/nyc_air_quality_{date_suffix}.csv"
        
        air_quality_df.to_csv(air_quality_filename, index=False)
        
        print(f"Air quality data saved to: {air_quality_filename}")
        
        return air_quality_df
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        import traceback
        print("\nFull traceback:")
        traceback.print_exc()
        raise

if __name__ == "__main__":
    air_quality_df = main()