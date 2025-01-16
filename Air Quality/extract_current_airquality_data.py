import requests_cache
import json
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after=300)  # Cache for 5 minutes

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

# Get current timestamp for the data
current_timestamp = datetime.now()

# Air Quality API parameters - only request current conditions
air_quality_params = {
    "latitude": latitude,
    "longitude": longitude,
    "current": [
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
    """Fetch current air quality data"""
    print("Fetching current air quality data...")
    
    # Make API request
    url = "https://air-quality-api.open-meteo.com/v1/air-quality"
    response = cache_session.get(url, params=air_quality_params)
    response.raise_for_status()
    data = response.json()
    
    # Create the JSON structure
    air_quality_data = {
        "metadata": {
            "timestamp": current_timestamp.isoformat(),
            "location": {
                "latitude": latitude,
                "longitude": longitude,
                "city": "New York City"
            },
            "data_source": "Open-Meteo Air Quality API"
        },
        "current_conditions": {}
    }
    
    # Process current conditions
    if "current" in data:
        current_data = {
            "timestamp": current_timestamp.isoformat(),
            "values": {}
        }
        
        for var in air_quality_params["current"]:
            if var in data["current"]:
                current_data["values"][var] = data["current"][var]
        
        air_quality_data["current_conditions"] = current_data
    
    return air_quality_data

def save_air_quality_data(air_quality_data):
    """Save air quality data to JSON file with timestamp in filename"""
    timestamp = current_timestamp.strftime("%Y%m%d_%H%M%S")
    filename = f"Air Quality/nyc_air_quality_current_{timestamp}.json"
    
    with open(filename, 'w') as f:
        json.dump(air_quality_data, f, indent=2)
    
    print(f"Air quality data saved to: {filename}")
    return filename

def main():
    try:
        # Fetch current air quality data
        air_quality_data = fetch_air_quality_data()
        
        # Save data to JSON file
        saved_file = save_air_quality_data(air_quality_data)
        
        print(f"\nData successfully processed and saved to {saved_file}")
        return air_quality_data
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        import traceback
        print("\nFull traceback:")
        traceback.print_exc()
        raise

if __name__ == "__main__":
    air_quality_data = main()