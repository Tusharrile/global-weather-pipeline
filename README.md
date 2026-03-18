# Real-Time Weather Data Pipeline

This is an event-driven data pipeline built to process and model the GlobalWeatherRepository dataset. The pipeline demonstrates data streaming, the Medallion Architecture (Bronze, Silver, Gold), and orchestration on AWS and Databricks.

## Team

| Name | Role |
|------|------|
| **Adeesh Koshal** | Team Lead — Full-Stack Pipeline Architect |
| **Tushar Rile** | AWS Infrastructure & Streaming Specialist |
| **Revanth Sai** | Databricks & Data Transformation Engineer |

## Project Overview

When a CSV file is uploaded to AWS S3, it triggers a lambda-based streaming process:

CSV Upload -> S3 Event -> Lambda -> Kinesis Data Stream -> Kinesis Firehose -> S3 Bronze (Parquet)
S3 Bronze -> Databricks Silver (Clean & Transform) -> Databricks Gold (Star Schema)

This entire flow is orchestrated using Apache Airflow (MWAA) with Slack alerts monitoring each step. 

## Tech Stack

- **Storage:** AWS S3
- **Streaming:** AWS Lambda, Amazon Kinesis Data Stream, Amazon Kinesis Firehose
- **Processing:** Databricks Serverless, Apache Spark (PySpark)
- **Metadata Catalog:** AWS Glue + Crawler
- **Orchestration:** Apache Airflow (AWS MWAA)
- **Testing:** PyTest

## Repository Structure

```text
global-weather-pipeline/
├── Dashboard/                    # Analytics dashboards and exported pdfs/images
├── Datasets/                     # Source CSV data
│   └── GlobalWeather.csv
├── Development/
│   ├── Bronze/                   # Raw ingestion logic
│   ├── DAG/                      # Airflow DAG orchestration script
│   ├── Gold/                     # Star schema dimensional modelling
│   ├── Silver/                   # Cleaning and transformation logic
│   └── error handling/           # Pipeline error management and logging
├── Testing/
│   ├── Data quality/             # Validation scripts
│   └── PyTest/                   # Unit tests
└── README.md
```

## Dataset Details

The GlobalWeatherRepository dataset contains weather measurements, air quality metrics, astronomical events, and location data. 
- 127,646 rows, 53 columns.
- Partitioned by Country in the Bronze layer.

## Pipeline Layers

### Bronze Layer (Raw Ingestion)
The goal here is to store data exactly as it arrived without modifications. We read the raw CSV, add audit columns (ingested timestamp, source file string, and batch UUID), and write to Parquet partitioned by Country.

### Silver Layer (Cleaning & Transformation)
This layer enforces schema constraints and makes the data trustworthy:
- Column renaming for compatibility with Spark/Glue.
- Type casting to proper numeric or timestamp types.
- Deduplication and outlier removal (e.g., humidity over 100, impossible temperature readings).
- Null imputation using dataset averages.

### Gold Layer (Star Schema)
The final stage transforms the flat structure into a business-ready Star Schema:
- `fact_weather`: The main fact table with all numeric measurements.
- Dimensions: `dim_location`, `dim_condition`, `dim_astronomy`, `dim_date`.

## AWS Infrastructure Overview

- **S3 Bucket:** `global-weather-pipeline` with subfolders for raw/, bronze/, silver/, gold/, scripts/, and airflow/dags/.
- **Lambda:** Python function that batch-streams records into Kinesis when a new CSV lands in raw/.
- **Kinesis Firehose:** Buffers the stream and converts JSON to Apache Parquet using AWS Glue before writing to the bronze/ prefix.
- **MWAA (Airflow):** A 11-task DAG that coordinates the Databricks notebook runs and alerts the team via Slack webhooks.

## Data Quality & Testing

- Data quality notebooks check for row counts, duplicate records, null values, and foreign key integrity across the layers.
- PyTest test suites handle unit testing for the PySpark transformations locally.

## Weather Analytics Dashboard

Below are some key visual insights generated from the Gold Layer analytics tables.

### Global Temperature Insights
![Global Avg Temp](Dashboard/images/Global%20Avg%20Temp%20(°C).png)

### Rainfall Insights
![Top 10 Highest Rainfall Countries](Dashboard/images/Top%2010%20Highest%20Rainfall%20Countries.png)

### Air Quality Insights
![Global Avg PM2.5](Dashboard/images/Global%20Avg%20PM2.5.png)

### Wind Insights
![Average Wind Speed by Country](Dashboard/images/Average%20Wind%20Speed%20by%20Country.png)
