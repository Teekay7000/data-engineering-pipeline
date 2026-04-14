# End-to-End African Economic Data Platform

A complete data engineering project that extracts, processes, transforms, and serves African economic indicators (GDP growth & unemployment) using the World Bank API.

---

## Project Overview

This project is a full **end-to-end data pipeline and analytics platform** that:

* Extracts economic data from the World Bank API
* Stores raw data in PostgreSQL
* Transforms and cleans datasets
* Builds a star schema for analytics
* Performs feature engineering
* Validates data quality
* Exposes data through a FastAPI REST API

It simulates a real-world **data engineering + analytics engineering system**.

---

## Architecture

```
World Bank API
      ↓
Data Ingestion Layer (Python)
      ↓
Raw Data Tables (PostgreSQL)
      ↓
Data Transformation Layer
      ↓
Cleaned Dataset (Feature Engineering)
      ↓
Star Schema (Fact & Dimensions)
      ↓
Data Quality Checks
      ↓
FastAPI Data Service Layer
```

---

## Features

### Data Ingestion

* Fetches GDP growth & unemployment data
* Handles pagination, retries, and rate limiting
* Covers 50+ African countries (2000–2023)

### Data Storage (PostgreSQL)

* Raw staging tables
* Cleaned data tables
* Star schema design (fact & dimensions)

### Data Transformation

* Joins multiple indicators
* Handles missing values
* Feature engineering:

  * GDP lag (1-year)
  * 5-year rolling averages
  * Unemployment rolling trends

### Data Quality Framework

* Row count validation
* Null checks
* Range validation
* Duplicate detection
* Referential integrity checks
* Data freshness monitoring

### API Layer (FastAPI)

Provides REST endpoints:

* `/countries` → list all countries
* `/data` → filtered economic data
* `/data/{country}` → country-specific time series
* `/summary` → aggregated statistics by region
* `/regions` → regional breakdown
* `/health` → system health check

---

## Tech Stack

* Python
* PostgreSQL
* FastAPI
* psycopg2
* REST APIs
* SQL
* Logging

---

## Data Model

### Fact Table

* GDP growth
* Unemployment rate
* Lag features
* Rolling averages

### Dimension Tables

* Country (region, sub-region)
* Time (decades, year groups)

---

## Key Engineering Concepts Used

* ETL Pipeline Design
* Data Warehousing (Star Schema)
* API Integration (REST)
* Feature Engineering
* Data Validation Framework
* Pagination & Rate Limiting
* Database Optimization (Indexes, Upserts)

---

## Project Structure (Suggested)

```
project/
│
├── api_fetcher.py
├── transformer.py
├── data_model.py
├── data_quality.py
├── api_service.py
├── database/
│   └── schema.sql
└── README.md
```

---

## Example Use Cases

* Economic trend analysis across Africa
* Country comparison dashboards
* Machine learning feature dataset
* Academic research & policy insights

---

## Why this project matters

This project demonstrates real-world skills in:

* Data engineering pipelines
* Backend API development
* Database design
* Analytics engineering
* Production-style logging and validation

It is aligned with entry-level data engineering roles at financial institutions and tech companies, including graduate programmes such as those at First National Bank.

---

## Author

Built as an independent data engineering project focused on real-world economic data systems.

---

## Future Improvements

* Docker containerization
* Cloud deployment (AWS / GCP)
* Airflow orchestration
* Dashboard (Power BI / Tableau)
* CI/CD pipeline

---

If you like this project, feel free to star the repo!
