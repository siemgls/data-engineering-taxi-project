# Data Engineering Taxi Project

## Overview

This project implements both **batch** and **real-time data pipelines** using Python (pandas), Azure Blob Storage, and Airflow.

It processes NYC taxi data (Part 1) and a custom dirty dataset (Part 2), applying validation, transformation, and cloud storage.

---

## Features

### Part 1 — Batch Processing

* Reads Parquet taxi dataset
* Data validation with clear rules
* Data transformation (new calculated columns)
* Error logging and bad row extraction
* Writes processed data locally and to Azure Blob Storage
* Airflow DAG for scheduling

### Part 2 — Real-time Processing

* Monitors input folder for new files
* Automatically triggers pipeline when a file arrives
* Custom dataset (120 rows, 12 columns, intentionally dirty)
* Data validation + backup validation
* Processor adds multiple new columns
* Removes duplicates
* Writes output locally and to Azure
* Archives processed input files
* Airflow DAG for orchestration

---

## Project Structure

```
src/
  readers/
  validators/
  processors/
  writers/
  realtime/

data/
  input/
  output/
  error/
  archive/

dags/
  taxi_batch_dag.py
  realtime_file_check_dag.py

notebooks/
run_taxi_pipeline.py
```

---

## Setup

### 1. Install dependencies

```
pip install -r requirements.txt
```

---

### 2. Create `.env` file

```
AZURE_STORAGE_CONNECTION_STRING=your_connection_string
AZURE_CONTAINER_NAME=taxi-data
```

---

## Running the Project

### Batch Pipeline

```
python run_taxi_pipeline.py
```

---

### Real-time Pipeline

Start the watcher:

```
python src/realtime/realtime_pipeline.py
```

Then generate or drop a file into:

```
data/input/realtime/
```

Example:

```
python scripts/generate_realtime_dataset.py
```

---

## Data Validation

Validation rules include:

* Missing values
* Invalid numeric ranges
* Invalid categories
* Duplicate records
* Logical inconsistencies (e.g., dates)

Errors are:

* Logged to `data/error/`
* Stored as separate bad row files

---

## Data Processing

### Taxi Dataset (Batch)

* Trip duration
* Average speed
* Revenue per mile
* Distance categories
* Fare categories
* Time of day classification

### Real-time Dataset

* Order total
* Shipping delay
* Order value category
* Processing timestamp

---

## Output

* Local: `data/output/`
* Azure Blob Storage:

  * `processed/` (batch)
  * `realtime/` (real-time)

---

## Notes

* Data files are excluded from GitHub
* `.env` is not committed for security
* Error logs and outputs are generated dynamically

---

## Technologies Used

* Python (pandas, numpy)
* Azure Blob Storage
* Apache Airflow (DAGs)
* Parquet / CSV

---
