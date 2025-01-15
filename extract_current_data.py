import openmeteo_requests
import requests_cache
import json
from datetime import datetime, timedelta
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

om = openmeteo_requests.Client(session=cache_session)

# New York City coordinates
latitude = 40.7128
longitude = -74.0060

# Get today's date
today = datetime.now().strftime("%Y-%m-%d")

# Weather API parameters (forecast for today)
weather_params = {
    "latitude": latitude,
    "longitude": longitude,
    "start_date": today,
    "end_date": today,
    "current": [  # Add current weather data
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
    ],
    "hourly": [  # Hourly forecast for today
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
    "start_date": today,
    "end_date": today,
    "current": [  # Add current air quality data
        "pm10",
        "pm2_5",
        "carbon_monoxide",
        "nitrogen_dioxide",
        "ozone"
    ],
    "hourly": [  # Hourly forecast for today
        "pm10",
        "pm2_5",
        "carbon_monoxide",
        "nitrogen_dioxide",
        "ozone"
    ]
}

def get_variable_name(var_id, variables_list):
    """Map variable ID to name based on order in variables list"""
    try:
        return variables_list[var_id]
    except IndexError:
        print(f"Warning: Variable ID {var_id} not found in variables list")
        return f"unknown_variable_{var_id}"

def fetch_weather_data():
    """Fetch today's weather data and forecast"""
    print("Fetching weather data...")
    weather_responses = om.weather_api("https://api.open-meteo.com/v1/forecast", params=weather_params)
    weather_response = weather_responses[0]
    
    weather_data = {
        "timestamp": datetime.now().isoformat(),
        "location": {
            "latitude": latitude,
            "longitude": longitude,
            "city": "New York City"
        },
        "current": {},
        "hourly": {
            "time": [],
            "variables": {}
        }
    }
    
    # Initialize variables in hourly data
    for var_name in weather_params["hourly"]:
        weather_data["hourly"]["variables"][var_name] = []
    
    # Process current weather if available
    if hasattr(weather_response, 'Current'):
        current = weather_response.Current()
        weather_data["current"] = {
            "time": datetime.fromtimestamp(current.Time()).isoformat(),
            "variables": {}
        }
        for i in range(current.VariablesLength()):
            var = current.Variables(i)
            var_name = get_variable_name(i, weather_params["current"])
            weather_data["current"]["variables"][var_name] = float(var.Value())
    
    # Process hourly data
    hourly = weather_response.Hourly()
    time_array = [datetime.fromtimestamp(t).isoformat() 
                 for t in range(hourly.Time(), 
                              hourly.TimeEnd(), 
                              hourly.Interval())]
    weather_data["hourly"]["time"] = time_array
    
    for i in range(hourly.VariablesLength()):
        var = hourly.Variables(i)
        var_name = get_variable_name(i, weather_params["hourly"])
        if var_name in weather_data["hourly"]["variables"]:
            weather_data["hourly"]["variables"][var_name] = var.ValuesAsNumpy().tolist()
    
    return weather_data

def fetch_air_quality_data():
    """Fetch today's air quality data and forecast"""
    print("Fetching air quality data...")
    air_quality_responses = om.weather_api("https://air-quality-api.open-meteo.com/v1/air-quality", params=air_quality_params)
    air_quality_response = air_quality_responses[0]
    
    air_quality_data = {
        "timestamp": datetime.now().isoformat(),
        "location": {
            "latitude": latitude,
            "longitude": longitude,
            "city": "New York City"
        },
        "current": {},
        "hourly": {
            "time": [],
            "variables": {}
        }
    }
    
    # Initialize variables in hourly data
    for var_name in air_quality_params["hourly"]:
        air_quality_data["hourly"]["variables"][var_name] = []
    
    # Process current air quality if available
    if hasattr(air_quality_response, 'Current'):
        current = air_quality_response.Current()
        air_quality_data["current"] = {
            "time": datetime.fromtimestamp(current.Time()).isoformat(),
            "variables": {}
        }
        for i in range(current.VariablesLength()):
            var = current.Variables(i)
            var_name = get_variable_name(i, air_quality_params["current"])
            air_quality_data["current"]["variables"][var_name] = float(var.Value())
    
    # Process hourly data
    hourly = air_quality_response.Hourly()
    time_array = [datetime.fromtimestamp(t).isoformat() 
                 for t in range(hourly.Time(), 
                              hourly.TimeEnd(), 
                              hourly.Interval())]
    air_quality_data["hourly"]["time"] = time_array
    
    for i in range(hourly.VariablesLength()):
        var = hourly.Variables(i)
        var_name = get_variable_name(i, air_quality_params["hourly"])
        if var_name in air_quality_data["hourly"]["variables"]:
            air_quality_data["hourly"]["variables"][var_name] = var.ValuesAsNumpy().tolist()
    
    return air_quality_data

def main():
    try:
        # Fetch current data and forecasts
        weather_data = fetch_weather_data()
        air_quality_data = fetch_air_quality_data()
        
        # Save to JSON files with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        weather_filename = f"nyc_weather_current_{timestamp}.json"
        air_quality_filename = f"nyc_air_quality_current_{timestamp}.json"
        
        # Save separate files for weather and air quality
        with open(weather_filename, 'w') as f:
            json.dump(weather_data, f, indent=2)
        
        with open(air_quality_filename, 'w') as f:
            json.dump(air_quality_data, f, indent=2)
        
        print(f"\nData saved to:")
        print(f"Weather: {weather_filename}")
        print(f"Air Quality: {air_quality_filename}")
        
        return weather_data, air_quality_data
    
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        # Print additional debug information
        import traceback
        print("\nFull traceback:")
        traceback.print_exc()
        raise

if __name__ == "__main__":
    weather_data, air_quality_data = main()