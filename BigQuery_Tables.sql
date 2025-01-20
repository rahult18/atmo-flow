-- First we create the dataset

CREATE SCHEMA IF NOT EXISTS `weather_air_quality`
OPTIONS (
  location = "US"
);

-- TimeDim table
CREATE OR REPLACE TABLE `weather_air_quality.TimeDim`
(
  time_key INT64 NOT NULL,
  full_date DATE NOT NULL,
  year INT64 NOT NULL,
  quarter INT64 NOT NULL,
  month INT64 NOT NULL,
  day INT64 NOT NULL,
  hour INT64 NOT NULL,
  day_of_week INT64 NOT NULL,
  is_weekend BOOL NOT NULL,
  is_holiday BOOL NOT NULL
)
PARTITION BY full_date
CLUSTER BY hour
OPTIONS(
  description="Time dimension table for weather and air quality measurements"
);

-- LocationDim table
CREATE OR REPLACE TABLE `weather_air_quality.LocationDim`
(
  location_key INT64 NOT NULL,
  latitude FLOAT64 NOT NULL,
  longitude FLOAT64 NOT NULL,
  city STRING NOT NULL,
  region STRING NOT NULL,
  climate_zone STRING NOT NULL,
  elevation FLOAT64 NOT NULL
)
CLUSTER BY city, region
OPTIONS(
  description="Location dimension table for measurement locations"
);

-- AirQualityStatusDim table
CREATE OR REPLACE TABLE `weather_air_quality.AirQualityStatusDim`
(
  status_key INT64 NOT NULL,
  aqi_category STRING NOT NULL,
  health_implications STRING NOT NULL,
  cautionary_statement STRING NOT NULL,
  color_code STRING NOT NULL
)
OPTIONS(
  description="Air quality status classification and descriptions"
);

-- WeatherConditionDim table
CREATE OR REPLACE TABLE `weather_air_quality.WeatherConditionDim`
(
  condition_key INT64 NOT NULL,
  weather_code INT64 NOT NULL,
  description STRING NOT NULL,
  category STRING NOT NULL,
  severity_level INT64 NOT NULL
)
OPTIONS(
  description="Weather conditions classification and descriptions"
);

-- AirQualityFact table
CREATE OR REPLACE TABLE `weather_air_quality.AirQualityFact`
(
  timestamp DATETIME NOT NULL,
  location_key INT64 NOT NULL,
  status_key INT64 NOT NULL,
  pm2_5 FLOAT64,
  pm10 FLOAT64,
  carbon_monoxide FLOAT64,
  nitrogen_dioxide FLOAT64,
  sulphur_dioxide FLOAT64,
  ozone FLOAT64,
  us_aqi INT64,
  aerosol_optical_depth FLOAT64,
  dust FLOAT64,
  uv_index FLOAT64
)
PARTITION BY DATE(timestamp)
CLUSTER BY location_key, status_key
OPTIONS(
  description="Air quality measurements fact table"
);

-- WeatherFact table
CREATE OR REPLACE TABLE `weather_air_quality.WeatherFact`
(
  timestamp DATETIME NOT NULL,
  location_key INT64 NOT NULL,
  condition_key INT64 NOT NULL,
  severity_key INT64 NOT NULL,
  season_key INT64 NOT NULL,
  temperature_2m FLOAT64,
  apparent_temperature FLOAT64,
  precipitation FLOAT64,
  snowfall FLOAT64,
  wind_speed_10m FLOAT64,
  wind_direction_10m INT64,
  wind_gusts_10m FLOAT64,
  relative_humidity_2m FLOAT64,
  surface_pressure FLOAT64,
  cloud_cover INT64,
  cloud_cover_low INT64,
  cloud_cover_mid INT64,
  cloud_cover_high INT64,
  visibility FLOAT64,
  precipitation_probability FLOAT64
)
PARTITION BY DATE(timestamp)
CLUSTER BY location_key, severity_key
OPTIONS(
  description="Enhanced weather measurements fact table"
);

-- Create HarmonizedData table with partitioning and clustering
CREATE OR REPLACE TABLE `atmo-flow.weather_air_quality.HarmonizedData`
(
    -- Timestamp
    timestamp TIMESTAMP NOT NULL,
    location_key INT64 NOT NULL,
    
    -- Weather Features
    temperature_2m FLOAT64,
    apparent_temperature FLOAT64,
    precipitation FLOAT64,
    wind_speed_10m FLOAT64,
    wind_direction_10m INT64,
    relative_humidity_2m FLOAT64,
    surface_pressure FLOAT64,
    cloud_cover INT64,
    visibility FLOAT64,
    
    -- Air Quality Features
    pm2_5 FLOAT64,
    pm10 FLOAT64,
    carbon_monoxide FLOAT64,
    nitrogen_dioxide FLOAT64,
    sulphur_dioxide FLOAT64,
    ozone FLOAT64,
    us_aqi INT64,
    aerosol_optical_depth FLOAT64,
    dust FLOAT64,
    uv_index FLOAT64,
    
    -- Time Features
    year INT64 NOT NULL,
    month INT64 NOT NULL,
    hour INT64,
    day_of_week INT64,
    is_weekend BOOL,
    season STRING,
    
    -- Location Features
    latitude FLOAT64,
    longitude FLOAT64,
    elevation FLOAT64
)
PARTITION BY
    DATE_TRUNC(timestamp, MONTH)
CLUSTER BY
    location_key, year, month
OPTIONS(
    description="Harmonized weather and air quality data for machine learning",
    labels=[("domain", "environmental"), ("data_type", "harmonized")]
);

-- Create SeasonDim table
CREATE OR REPLACE TABLE `weather_air_quality.SeasonDim`
(
  season_key INT64 NOT NULL,
  season_name STRING NOT NULL,
  start_month INT64 NOT NULL,
  end_month INT64 NOT NULL,
  avg_daylight_hours FLOAT64,
  typical_temp_range STRING,
  characteristic_weather STRING
)
OPTIONS(
  description="Season dimension with detailed seasonal characteristics"
);

-- Create SeverityDim table
CREATE OR REPLACE TABLE `weather_air_quality.SeverityDim`
(
  severity_key INT64 NOT NULL,
  severity_level INT64 NOT NULL,
  severity_name STRING NOT NULL,
  description STRING NOT NULL,
  recommended_actions STRING NOT NULL,
  alert_level STRING NOT NULL
)
OPTIONS(
  description="Shared severity dimension for weather and air quality conditions"
);
