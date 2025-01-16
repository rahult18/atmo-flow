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

# Get current timestamp for the data
current_timestamp = datetime.now()
today = current_timestamp.strftime("%Y-%m-%d")

# Updated Weather API parameters
weather_params = {
    "latitude": latitude,
    "longitude": longitude,
    "start_date": today,
    "end_date": today,
    "current": [
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
    
    # Create the JSON structure
    weather_data = {
        "metadata": {
            "timestamp": current_timestamp.isoformat(),
            "location": {
                "latitude": latitude,
                "longitude": longitude,
            },
            "data_source": "Open-Meteo API"
        },
        "current_conditions": {},
        "hourly_forecast": {
            "time_series": []
        }
    }
    
    # Process current weather conditions
    if hasattr(weather_response, 'Current'):
        current = weather_response.Current()
        current_time = datetime.fromtimestamp(current.Time())
        
        current_data = {
            "timestamp": current_time.isoformat(),
            "values": {}
        }
        
        for i in range(current.VariablesLength()):
            var = current.Variables(i)
            var_name = get_variable_name(i, weather_params["current"])
            current_data["values"][var_name] = float(var.Value())
        
        weather_data["current_conditions"] = current_data
    
    # Process hourly forecast
    hourly = weather_response.Hourly()
    start_time = hourly.Time()
    end_time = hourly.TimeEnd()
    interval = hourly.Interval()
    
    # Get all variable data
    variable_data = {}
    for i in range(hourly.VariablesLength()):
        var = hourly.Variables(i)
        var_name = get_variable_name(i, weather_params["hourly"])
        variable_data[var_name] = var.ValuesAsNumpy()
    
    # Create hourly entries
    time_index = 0
    for timestamp in range(start_time, end_time, interval):
        current_time = datetime.fromtimestamp(timestamp)
        
        hourly_data = {
            "timestamp": current_time.isoformat(),
            "values": {}
        }
        
        for var_name, values in variable_data.items():
            try:
                hourly_data["values"][var_name] = float(values[time_index])
            except IndexError:
                print(f"Warning: Missing data for {var_name} at index {time_index}")
                hourly_data["values"][var_name] = None
        
        weather_data["hourly_forecast"]["time_series"].append(hourly_data)
        time_index += 1
    
    return weather_data

def save_weather_data(weather_data):
    """Save weather data to JSON file with timestamp in filename"""
    timestamp = current_timestamp.strftime("%Y%m%d_%H%M%S")
    filename = f"Weather/nyc_weather_current_{timestamp}.json"
    
    with open(filename, 'w') as f:
        json.dump(weather_data, f, indent=2)
    
    print(f"Weather data saved to: {filename}")
    return filename

def main():
    try:
        # Fetch current weather data
        weather_data = fetch_weather_data()
        
        # Save data to JSON file
        saved_file = save_weather_data(weather_data)
        
        print(f"\nData successfully processed and saved to {saved_file}")
        return weather_data
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        import traceback
        print("\nFull traceback:")
        traceback.print_exc()
        raise

if __name__ == "__main__":
    weather_data = main()