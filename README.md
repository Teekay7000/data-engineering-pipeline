data-engineering-project

API data pipeline with PostgreSQL
World Bank Africa — GDP & Unemployment Data Pipeline

A data engineering pipeline that fetches GDP Growth and Unemployment data for all 54 African countries from the World Bank API and stores the raw data in PostgreSQL.
Project Structure

├── api_fetcher.py       # Fetches raw data from the World Bank API
├── database.py          # Stores raw data into PostgreSQL
└── README.md

Requirements

    Python 3.8+
    PostgreSQL
    psycopg2

Install the required library:

pip install psycopg2-binary

PostgreSQL Setup

    Open pgAdmin and create the database:

CREATE DATABASE worldbank_africa;

    Update the DB_CONFIG in database.py with your credentials:

DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "worldbank_africa",
    "user":     "postgres",
    "password": "your_password_here",
}

How to Run

database.py automatically calls api_fetcher.py so you only need to run one command:

python database.py

This will:

    Create the tables in PostgreSQL
    Fetch all data from the World Bank API
    Store the raw data into the database

Data Source

World Bank Open Data API
Indicator 	Code 	Description
GDP Growth 	NY.GDP.MKTP.KD.ZG 	GDP growth (annual %)
Unemployment 	SL.UEM.TOTL.ZS 	Unemployment, total (% of labour force)

    Countries: All 54 African Union member states
    Years: 2000 – 2023

Database Tables
raw_gdp_growth
Column 	Type 	Description
id 	SERIAL 	Primary key
country_iso3 	CHAR(3) 	ISO 3-letter country code
country_name 	TEXT 	Full country name
year 	SMALLINT 	Year of the data point
value 	NUMERIC 	GDP growth % (NULL if missing)
indicator_id 	TEXT 	World Bank indicator code
indicator_name 	TEXT 	World Bank indicator description
fetched_at 	TIMESTAMPTZ 	Timestamp of when data was fetched
raw_unemployment

Same structure as raw_gdp_growth but stores unemployment % values.
African Countries Covered

All 54 African Union member states including Nigeria, South Africa, Egypt, Ethiopia, Kenya, Ghana, Morocco, Tanzania, Algeria, and more.
Notes

    Re-running database.py will not create duplicate rows — it uses ON CONFLICT DO UPDATE to safely upsert data.
    Rows where the API returns no value are stored as NULL and are not dropped at this stage.
    The API is called with a 0.15s delay between requests to avoid rate limiting.

Next steps

    Data cleaning
    Create dimension tables
    Create fact tables
    Automate pipeline
