# Data Engineering Taxi Project

## Overview

This project implements both **batch** and **real-time data pipelines** using:

- Python (pandas, numpy, pyarrow)
- Azure Blob Storage
- Apache Airflow (Docker)

It processes:

- **Part 1:** NYC Yellow Taxi dataset (Parquet)
- **Part 2:** Custom dirty orders dataset (CSV)

The pipelines include validation, transformation, logging, and cloud storage.

---

## Features

### Part 1 — Batch Processing

- Reads Parquet taxi dataset
- Validates data with strict rules
- Logs validation errors and bad rows
- Transforms data (adds multiple columns)
- Writes output locally and to Azure Blob Storage
- Scheduled with Airflow (runs daily)

---

### Part 2 — Real-time Processing

- Monitors input folder for new files
- Processes `.csv` / `.xlsx` files automatically
- Uses a dirty dataset (120 rows, 12 columns)
- Performs validation + backup validation
- Adds new calculated columns
- Removes duplicates
- Writes output locally and to Azure
- Archives processed files
- Scheduled with Airflow (runs every minute)

---

## Project Structure


data-engineering-taxi-project/

src/
readers/
validators/
processors/
writers/
realtime/
utils/

data/
input/
output/
error/
archive/

dags/
taxi_batch_dag.py
realtime_file_check_dag.py

scripts/
generate_realtime_dataset.py

notebooks/
run_taxi_pipeline.py
requirements.txt


---

## Setup

### 1. Install dependencies


pip install -r requirements.txt


---

### 2. Create `.env` file

Create a `.env` file in the root:


AZURE_STORAGE_CONNECTION_STRING=your_connection_string

AZURE_CONTAINER_NAME=taxi-data


---

## Datasets

### Batch dataset (NYC Taxi)

Download from:

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Place in:


data/input/


Expected filename:


yellow_tripdata_2026-01.parquet


---

### Real-time dataset

Generate using:


python scripts/generate_realtime_dataset.py


This creates:


data/input/realtime/orders_dirty.csv


---

## Running the Pipelines

### Batch Pipeline (manual)


python run_taxi_pipeline.py


---

### Real-time Pipeline (manual test)


python src/realtime/realtime_pipeline.py


---

## Airflow (Docker)

Airflow is used to schedule and run the pipelines.

### Start Airflow

From a separate `airflow-docker` folder:


docker compose up


Open:


http://localhost:8080


Login:


username: airflow
password: airflow


---

### DAGs

This project contains two DAGs:

#### 1. Batch DAG


taxi_batch_pipeline


- Schedule: `@daily`
- Runs the batch pipeline automatically once per day

---

#### 2. Real-time DAG


realtime_file_check_pipeline


- Schedule: every minute (`*/1 * * * *`)
- Checks input folder and processes files if present

---

### Important

The Airflow Docker setup must mount the project folder:


/opt/airflow/project


The DAGs use:

```python
PROJECT_PATH = "/opt/airflow/project"
Data Validation
Batch validation includes:
Missing values
Negative values (fare, total)
Invalid passenger count
Invalid datetime logic
Real-time validation includes:
Missing columns/values
Empty strings
Duplicate order IDs
Invalid numeric ranges
Invalid categories
Date inconsistencies
Data Processing
Batch pipeline adds:
trip_duration_minutes
average_speed_mph
pickup_year, pickup_month
revenue_per_mile
trip_distance_category
fare_category
trip_time_of_day

Removes:

VendorID
store_and_fwd_flag
RatecodeID
Real-time pipeline adds:
order_total
shipping_delay_days
order_value_category
processed_at
Output
Local output
data/output/
data/output/realtime/
Azure Blob Storage
Batch → processed/
Real-time → realtime/

Notes:

Data files are not included due to size
.env is excluded for security
Output and error files are generated dynamically
Airflow is required for scheduling

How to Test:

1. Batch

python run_taxi_pipeline.py

2. Real-time

python scripts/generate_realtime_dataset.py
python src/realtime/realtime_pipeline.py

3. Airflow

Start Docker Airflow
Trigger taxi_batch_pipeline
Trigger realtime_file_check_pipeline

Both should complete successfully

Author

Sieme
Data Engineering Project 2025–2026