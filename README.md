# Data Engineering Taxi Project

## Overview
This project processes NYC taxi data using both batch and real-time pipelines.

## Features
- Batch data processing (Parquet files)
- Real-time file monitoring
- Data validation and error logging
- Data transformation
- Azure Blob Storage integration

## Project Structure
- `src/` → pipeline code
- `data/` → input/output folders (not included in repo)
- `notebooks/` → exploration
- `run_taxi_pipeline.py` → batch pipeline
- `src/realtime/realtime_pipeline.py` → real-time pipeline

## Setup

1. Install dependencies:

pip install -r requirements.txt

2. Create `.env` file:

AZURE_STORAGE_CONNECTION_STRING=your_connection_string
AZURE_CONTAINER_NAME=taxi-data

## Run Batch Pipeline

python run_taxi_pipeline.py

## Run Real-time Pipeline

python src/realtime/realtime_pipeline.py

Then place a CSV file into:

data/input/realtime/

## Notes
- Data files are not included due to size
- Error logs are saved in `data/error/`
- Processed files are saved in `data/output/`