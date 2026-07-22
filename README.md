# World Bank Africa Data Pipeline

A data engineering pipeline that extracts, processes, validates, models, and exposes economic indicators for African countries using World Bank data.

The project collects GDP growth and unemployment data for 54 African countries, stores raw data in PostgreSQL, transforms it into analytical datasets, applies data quality checks, builds a star schema warehouse, and provides a REST API for data access.


## Features

- Extracts World Bank economic indicators
- Covers 54 African countries
- Stores raw API responses
- Performs data cleaning and feature engineering
- Builds analytical warehouse tables
- Implements data quality validation
- Provides API endpoints for data access
- Supports filtering by country, region, and year

## Technologies

- Python
- PostgreSQL
- FastAPI
- World Bank API
- Psycopg2
- SQL
- REST API


## Database Design

The project uses a layered data warehouse approach.

### Raw Layer

Stores data directly from the API.


Features created:

- Previous year GDP growth
- Five-year GDP growth average
- Five-year unemployment average



