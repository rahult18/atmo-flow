import os
import json
from datetime import datetime
import functions_framework
from google.cloud import pubsub_v1
import requests_cache
import openmeteo_requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Load environment variables
LATITUDE = float(os.getenv('LATITUDE'))
LONGITUDE = float(os.getenv('LONGITUDE'))
AIR_QUALITY_TOPIC = os.getenv('AIR_QUALITY_TOPIC')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC')
DEAD_LETTER_TOPIC = os.getenv('DEAD_LETTER_TOPIC')

# Setup the publisher client
publisher = pubsub_v1.PublisherClient()

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after=300)

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

def publish_to_topic(topic_path, data):
    """Publish data to a Pub/Sub topic"""
    try:
        # Convert data to JSON string
        json_string = json.dumps(data)
        # Convert string to bytes
        data_bytes = json_string.encode('utf-8')
        # Publish message
        future = publisher.publish(topic_path, data=data_bytes)
        future.result()  # Wait for message to be published
        print(f"Published message to {topic_path}")
    except Exception as e:
        # Publish to dead letter queue
        error_data = {
            "original_data": data,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }
        dead_letter_future = publisher.publish(
            DEAD_LETTER_TOPIC, 
            data=json.dumps(error_data).encode('utf-8')
        )
        dead_letter_future.result()
        raise e

def fetch_air_quality_data():
    """Fetch current air quality data"""
    air_quality_params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
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
    
    # Make API request
    url = "https://air-quality-api.open-meteo.com/v1/air-quality"
    response = cache_session.get(url, params=air_quality_params)
    response.raise_for_status()
    data = response.json()
    
    current_timestamp = datetime.now()
    
    # Create the JSON structure
    air_quality_data = {
        "metadata": {
            "timestamp": current_timestamp.isoformat(),
            "location": {
                "latitude": LATITUDE,
                "longitude": LONGITUDE,
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

def fetch_weather_data():
    """Fetch current weather data"""
    current_timestamp = datetime.now()
    today = current_timestamp.strftime("%Y-%m-%d")
    
    weather_params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
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
    
    weather_responses = om.weather_api("https://api.open-meteo.com/v1/forecast", params=weather_params)
    weather_response = weather_responses[0]
    
    weather_data = {
        "metadata": {
            "timestamp": current_timestamp.isoformat(),
            "location": {
                "latitude": LATITUDE,
                "longitude": LONGITUDE,
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
            var_name = weather_params["current"][i]
            current_data["values"][var_name] = float(var.Value())
        
        weather_data["current_conditions"] = current_data
    
    return weather_data

@functions_framework.http
def process_data(request):
    """Cloud Function entry point"""
    try:
        # Fetch and publish air quality data
        air_quality_data = fetch_air_quality_data()
        publish_to_topic(AIR_QUALITY_TOPIC, air_quality_data)
        
        # Fetch and publish weather data
        weather_data = fetch_weather_data()
        publish_to_topic(WEATHER_TOPIC, weather_data)
        
        return 'Data successfully processed and published', 200
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        import traceback
        print("\nFull traceback:")
        traceback.print_exc()
        return f'Error: {str(e)}', 500
