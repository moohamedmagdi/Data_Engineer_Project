

# Weather Data Pipeline with Python and PostgreSQL

## Project Overview

This project builds a simple **data pipeline** that collects weather data from a public API, processes it, and stores it in a PostgreSQL database.
The pipeline is containerized using Docker to ensure portability and easy deployment across different environments.

The goal of this project is to demonstrate core **Data Engineering concepts**, including:

* Data extraction from an API
* Data cleaning and transformation
* Database schema design
* ETL pipeline development
* Containerization using Docker
* Automation and logging

---

## Architecture

Weather data is collected from the Open-Meteo API, processed using Python, and then stored in a PostgreSQL database.

Pipeline flow:

Weather API → Python ETL Script → PostgreSQL Database

---

## Technologies Used

* Python
* PostgreSQL
* Docker
* REST API
* JSON Data Processing

---

## Data Source

Weather data is retrieved from the **Open-Meteo API**, which provides free weather forecast data without requiring an API key.

Example endpoint:

https://api.open-meteo.com/v1/forecast

Example parameters:

* latitude
* longitude
* current_weather

---

## Database Schema

The pipeline stores processed weather data in a PostgreSQL table.

Table: `weather_data`

Columns:

* id (Primary Key)
* temperature
* wind_speed
* wind_direction
* weather_code
* weather_time
* created_at

Example SQL schema:

```sql
CREATE TABLE weather_data (
    id SERIAL PRIMARY KEY,
    temperature FLOAT,
    wind_speed FLOAT,
    wind_direction INT,
    weather_code INT,
    weather_time TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## ETL Pipeline

The project follows a basic **ETL workflow**:

### 1. Extract

Fetch weather data from the Open-Meteo API.

### 2. Transform

Clean and normalize the JSON response to extract relevant fields such as:

* temperature
* wind speed
* wind direction
* weather code
* timestamp

### 3. Load

Insert the processed data into the PostgreSQL database.

---

## Running the Project

### 1. Start PostgreSQL using Docker

```
docker-compose up -d
```

### 2. Install Python dependencies

```
pip install -r requirements.txt
```

### 3. Run the ETL pipeline

```
python weather_pipeline.py
```

---

## Automation

The pipeline can be scheduled to run periodically using tools such as:

* Cron jobs
* Workflow orchestration tools

This allows automatic ingestion of weather data at scheduled intervals (e.g., hourly or daily).

---

## Logging and Monitoring

Basic logging is implemented to track:

* Successful data loads
* API failures
* Database insertion errors

Logs can be extended for monitoring pipeline health.

---

## Future Improvements

Possible improvements for this project include:

* Adding a workflow orchestrator for scheduling
* Implementing retry mechanisms for API failures
* Adding data validation checks
* Building a dashboard to visualize weather trends
* Expanding the schema for forecast data

---

## Author

Data Engineering Project – Weather Data Pipeline
