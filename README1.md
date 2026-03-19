Data Engineering Pipeline
World Bank Africa — GDP & Unemployment Data Pipeline

A complete data engineering pipeline that fetches, stores, transforms, and prepares GDP Growth and Unemployment data for all 54 African countries using the World Bank API.

Overview

This project demonstrates an end-to-end pipeline in Data Engineering:

API → Raw Data (PostgreSQL) → Data Transformation → Analytics-Ready Data
 Project Structure
├── api_fetcher.py       # Fetches raw data from World Bank API
├── database.py          # Stores raw data into PostgreSQL
├── transformer.py       # Cleans, joins, and engineers features
└── README.md
Tech Stack

Python 3.8+

PostgreSQL

psycopg2

SQL

Requirements

Install dependencies:

pip install psycopg2-binary
PostgreSQL Setup

Create database:

CREATE DATABASE worldbank_africa;

Update your config in scripts:

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "worldbank_africa",
    "user": "postgres",
    "password": "your_password_here",
}
How to Run
Step 1: Load Raw Data
python database.py

This will:

Create raw tables

Fetch data from API

Store raw data

Step 2: Transform Data
python transformer.py

This will:

Join raw datasets

Clean missing values

Compute features

Store results in cleaned_data

Data Source

World Bank Open Data API

Indicator	Code	Description
GDP Growth	NY.GDP.MKTP.KD.ZG	GDP growth (annual %)
Unemployment	SL.UEM.TOTL.ZS	Unemployment (% of labour force)

Countries: 54 African Union member states

Years: 2000 – 2023

Database Layers
1. Raw Layer
raw_gdp_growth / raw_unemployment
Column	Type	Description
id	SERIAL	Primary key
country_iso3	CHAR(3)	Country code
country_name	TEXT	Country name
year	SMALLINT	Year
value	NUMERIC	Indicator value
indicator_id	TEXT	API indicator
indicator_name	TEXT	Indicator name
fetched_at	TIMESTAMPTZ	Timestamp
2. Cleaned Layer (NEW)
cleaned_data
Column	Type	Description
id	SERIAL	Primary key
country_iso3	CHAR(3)	Country code
country_name	TEXT	Country name
year	SMALLINT	Year
gdp_growth	NUMERIC	GDP growth %
unemployment	NUMERIC	Unemployment %
gdp_growth_lag1	NUMERIC	Previous year GDP
gdp_growth_roll5	NUMERIC	5-year rolling GDP
unemp_roll5	NUMERIC	5-year rolling unemployment
cleaned_at	TIMESTAMPTZ	Processing timestamp
Transformation Logic (transformer.py)
Data Cleaning

Joins GDP and unemployment tables

Drops rows with NULL values

Ensures valid country-year pairs

Feature Engineering
Feature	Description
gdp_growth_lag1	Previous year GDP growth
gdp_growth_roll5	5-year rolling GDP average
unemp_roll5	5-year rolling unemployment
Performance Optimization

Uses batch inserts (execute_batch)

Efficient grouping and sorting in Python

Idempotent Loads

Uses ON CONFLICT DO UPDATE

Safe to re-run pipeline without duplicates

Example Output
ISO3  Country        Year   GDP%    UNEMP%   LAG1   ROLL5_G   ROLL5_U
ZAF   South Africa   2015   1.200   25.300   2.100  1.800     24.900
African Countries Covered

All 54 African Union member states including:

Nigeria

South Africa

Egypt

Kenya

Ghana

Morocco

Tanzania

Algeria

Notes

Missing API values are stored as NULL in raw data

Cleaned layer removes incomplete records

API calls include delay (0.15s) to avoid rate limits

Key Concepts Demonstrated

ETL Pipelines

Data Cleaning & Validation

Feature Engineering

Batch Processing

Transaction Management

Idempotent Database Design

Next Steps

Build dimension tables & fact tables (star schema)

Add pipeline orchestration (e.g. Apache Airflow)

Create dashboards (Power BI / Tableau)

Add data quality checks
    Automate pipeline
