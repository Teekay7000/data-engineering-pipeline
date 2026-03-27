Data Engineering Pipeline
World Bank Africa — GDP & Unemployment

An end-to-end pipeline that collects, processes, and models economic data for 54 African countries using the World Bank API.

Overview
API → Raw Data → Cleaned Data → Star Schema

Built using Data Engineering and Data Warehousing principles.

Project Structure
├── api_fetcher.py
├── database.py
├── transformer.py
├── data_model.py
└── README.md
Tech Stack
Python
PostgreSQL
psycopg2
SQL
How to Run
python database.py      # Load raw data
python transformer.py   # Clean and engineer features
python data_model.py    # Build star schema
Data Layers
Raw
raw_gdp_growth, raw_unemployment
Cleaned
cleaned_data
Includes lag and rolling averages
Analytics (Star Schema)
dim_country
dim_time
fact_indicators
Features
Data cleaning and joins
Feature engineering (lag, rolling averages)
Batch inserts for performance
Idempotent loads (ON CONFLICT)
Next Steps
Add Airflow for automation
Build dashboards
Deploy to cloud
