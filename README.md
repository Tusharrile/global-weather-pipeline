# 🌦️ Real-Time Weather Data Engineering Pipeline

## Project Overview

The **Real-Time Weather Data Engineering Pipeline** is a cloud-based data engineering project designed to ingest, process, and analyze weather data in near real-time.

The pipeline streams weather data through **Amazon Kinesis**, processes it using **Databricks and PySpark**, and stores the processed datasets in **Delta Lake tables** following the **Medallion Architecture (Bronze, Silver, Gold layers)**.

This architecture ensures scalable data ingestion, high data quality, and optimized datasets for analytics and reporting.

---

# 📊 Data Source

The data used in this project is based on a **Weather Dataset for Time Series Analysis** obtained from Kaggle.

The dataset includes weather observations such as:

- Temperature  
- Humidity  
- Wind speed  
- Pressure  
- Weather condition  
- Location information  
- Observation timestamps  

The data is streamed into the pipeline and stored in **AWS S3**.



---

## 🏗️ Data Architecture

This project follows the Medallion Architecture implemented in Databricks Unity Catalog.

### Bronze Layer (Raw Data)




**Purpose**

- Stores raw weather records exactly as received from the streaming source.
- No transformations are applied.
- Used for traceability and reprocessing.

**Source**

- Amazon Kinesis Data Stream
- Delivered to S3 using Amazon Firehose
- Ingested into Databricks.

---

### Silver Layer (Cleaned Data)



**Purpose**

- Stores raw weather records exactly as received from the streaming source.
- No transformations are applied.
- Used for traceability and reprocessing.

**Source**

- Amazon Kinesis Data Stream
- Delivered to S3 using Amazon Firehose
- Ingested into Databricks.

---

### Silver Layer (Cleaned Data)



**Purpose**

- Clean and standardized weather data.
- Removes duplicates.
- Fixes schema issues and null values.
- Creates structured dimension-ready datasets.

**Typical Transformations**

- Schema validation
- Null handling
- Duplicate removal
- Data type standardization
- Timestamp normalization

---

### Gold Layer (Analytics Layer)



**Purpose**

- Optimized for analytics and BI queries.
- Implements a Star Schema for weather analytics.

The fact table stores weather measurements, while dimension tables provide descriptive attributes.

**Example analytics queries**

- Average temperature by city
- Weather condition trends
- Wind speed analysis by time
- Pressure trends across locations



---

## 🔄 Key Transformations

The pipeline performs multiple transformations to improve data quality and analytical usability.

Main transformations include:

- Streaming weather data ingestion from Kinesis
- Writing raw data to AWS S3
- Loading data into Databricks Bronze tables
- Data cleaning and schema standardization
- Deduplication of weather records
- Creation of dimension tables
- Generation of fact tables for analytics
- Optimization using Delta Lake

---

## ⏱️ Pipeline Orchestration

The pipeline is orchestrated using Databricks Workflows.

### Pipeline Stages

1. Bronze Ingestion Job  
2. Silver Data Cleaning Job  
3. Gold Star Schema Generation Job  

**Schedule**



The pipeline can also process near real-time data streaming from Kinesis.

---

## 🚨 Error Handling and Monitoring

Pipeline failures and data quality issues are logged for monitoring.

**Error logs table**



**Common monitored events**

- Streaming ingestion failures
- Schema mismatches
- Data quality violations
- Missing fields
- Duplicate records

Alerts can be configured for pipeline failures.

---

## 🛠️ Technologies Used

| Technology | Purpose |
|------------|--------|
| AWS S3 | Data Lake storage |
| Amazon Kinesis | Real-time data streaming |
| Amazon Kinesis Firehose | Stream delivery to S3 |
| AWS Lambda | Event triggering |
| Databricks | Data processing platform |
| PySpark | Data transformation |
| Delta Lake | Storage format |
| Unity Catalog | Data governance |
| Git & GitHub | Version control |

---

## 🎯 Project Objective

The objective of this project is to build a scalable, cloud-based real-time data engineering pipeline capable of:

- Streaming weather data in real time
- Processing and cleaning data efficiently
- Storing structured datasets using Delta Lake
- Enabling fast analytical queries
- Supporting future weather analytics and forecasting applications

This project demonstrates modern data engineering practices using AWS and Databricks.

---

