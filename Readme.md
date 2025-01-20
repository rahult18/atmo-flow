# AtmoFlow 🌤️

Breathing Life into Data - Real Time Weather and Air Quality Insights

## Overview

AtmoFlow is a robust data engineering pipeline built on Google Cloud Platform (GCP) that processes and analyzes weather and air quality data in both batch and streaming modes. The project combines historical weather data with real-time updates to provide comprehensive environmental insights. Here's the [link](https://dev.to/rahul_talatala/atmoflow-breathing-life-into-data-real-time-weather-and-air-quality-insights-5db0) to the blog, Happy Reading! ❤️

## Architecture

![Project Architecture](Project_Architecture.png)

The pipeline consists of several key components:

- **Data Collection**: Cloud Functions fetch data from weather and air quality APIs
- **Data Processing**: Dataproc clusters run PySpark jobs for both batch and streaming data
- **Data Storage**: Cloud Storage for raw data, BigQuery for processed data
- **Orchestration**: Cloud Composer (Apache Airflow) manages the pipeline
- **Visualization**: Looker dashboards for data analysis

## Project Structure

```
├── Air Quality/
│   ├── extract_current_air_quality_data.py
│   ├── extract_historical_air_quality_data.py
│   ├── nyc_air_quality_20240722_20250118.csv
│   └── nyc_air_quality_current_20250119_212214.json
├── Cloud Functions/
│   ├── batch_cloud_function.py
│   ├── batch_cloud_function_requirements.txt
│   ├── streaming_cloud_function.py
│   └── streaming_cloud_function_requirements.txt
├── DataProc/
│   ├── dataproc_batch_pyspark.py
│   └── dataproc_stream_batch_pyspark.py
├── Weather/
│   ├── extract_current_weather_data.py
│   ├── extract_historical_weather_data.py
│   ├── nyc_weather_20240722_20250118.csv
│   └── nyc_weather_current_20250119_212225.json
├── BigQuery_Tables.sql
├── cloud_composer.py
└── README.md
```

## Features

- **Dual Processing Modes**: Handles both batch historical data and real-time streaming updates
- **Data Quality Management**: Implements robust validation rules and error handling
- **Scalable Architecture**: Uses GCP's managed services for automatic scaling
- **Data Harmonization**: Combines weather and air quality metrics for comprehensive analysis
- **Optimized Storage**: Implements partitioning and clustering in BigQuery for efficient querying
- **Real-time Monitoring**: Tracks data quality metrics and processing statistics
- **Error Recovery**: Includes dead letter queues and retry mechanisms

## Technologies Used

- **Google Cloud Platform**
  - Cloud Functions
  - Cloud Storage
  - Pub/Sub
  - Dataproc
  - BigQuery
  - Cloud Composer
- **Apache Spark**
- **Python**
- **SQL**
- **Looker Studio**

## Setup and Installation

1. **Prerequisites**
   - Google Cloud Platform account
   - Python 3.8+
   - Apache Spark 3.x

2. **Environment Setup**
   ```bash
   # Clone the repository
   git clone https://github.com/yourusername/atmoflow.git
   cd atmoflow

   # Create and activate virtual environment
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   # or
   .\venv\Scripts\activate  # Windows

   # Install requirements
   pip install -r Cloud\ Functions/batch_cloud_function_requirements.txt
   pip install -r Cloud\ Functions/streaming_cloud_function_requirements.txt
   ```

3. **Configuration**
   - Add `.env` file wiht your GCP resources
   - Update with your GCP credentials and configurations
   - Set up required GCP services (Cloud Functions, Dataproc, BigQuery, etc.)

## Running the Pipeline

1. **Deploy Cloud Functions**
   ```bash
   gcloud functions deploy batch_data_collector --runtime python38 --trigger-http
   gcloud functions deploy stream_data_collector --runtime python38 --trigger-http
   ```

2. **Set up Cloud Composer DAG**
   - Upload `cloud_composer.py` to your Cloud Composer environment
   - Configure the Airflow variables as needed

3. **Create BigQuery Tables**
   - Execute the SQL scripts in `BigQuery_Tables.sql`

4. **Monitor the Pipeline**
   - Check Cloud Composer UI for DAG runs
   - Monitor logs in Cloud Logging
   - View processed data in BigQuery
   - Analyze results in Looker dashboards

## Connect With Me

Rahul Reddy Talatala - [LinkedIn](https://www.linkedin.com/in/rahul-reddy-t/) - rahul.talatala@gmail.com

## Acknowledgments

- Open-Meteo API for weather and air quality data
- Google Cloud Platform documentation
- Apache Spark and PySpark communities
