import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from typing import Optional
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [api] %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

app = FastAPI(
    title="World Bank Africa API",
    description="Access cleaned GDP Growth and Unemployment data for all 54 African countries.",
    version="1.0.0"
)

DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "worldbank_africa",
    "user":     "postgres",
    "password": "2411",
}


@contextmanager
def get_conn():
    conn = psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)
    try:
        yield conn
    finally:
        conn.close()




@app.get("/health")
def health():
    log.info("Health check called")
    return {"status": "ok"}



@app.get("/countries")
def get_countries(
    page:     int = Query(1,  ge=1,  description="Page number"),
    per_page: int = Query(20, ge=1, le=100, description="Results per page"),
):
    offset = (page - 1) * per_page
    log.info("GET /countries  page=%d per_page=%d", page, per_page)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM dim_country")
            total = cur.fetchone()["count"]

            cur.execute("""
                SELECT country_iso3, country_name, region, sub_region
                FROM dim_country
                ORDER BY country_name
                LIMIT %s OFFSET %s
            """, (per_page, offset))
            rows = cur.fetchall()

    return {
        "page":       page,
        "per_page":   per_page,
        "total":      total,
        "pages":      -(-total // per_page),
        "countries":  [dict(r) for r in rows],
    }


#All data

@app.get("/data")
def get_data(
    country:   Optional[str] = Query(None, description="Filter by ISO3 code e.g. NGA"),
    region:    Optional[str] = Query(None, description="Filter by region e.g. Western Africa"),
    year:      Optional[int] = Query(None, description="Filter by exact year e.g. 2015"),
    from_year: Optional[int] = Query(None, description="Start year e.g. 2010"),
    to_year:   Optional[int] = Query(None, description="End year e.g. 2020"),
    page:      int           = Query(1,  ge=1,  description="Page number"),
    per_page:  int           = Query(20, ge=1, le=100, description="Results per page"),
):
    log.info("GET /data  country=%s region=%s year=%s from=%s to=%s page=%d",
             country, region, year, from_year, to_year, page)

    where  = "WHERE 1=1"
    params = []

    if country:
        where += " AND UPPER(f.country_iso3) = UPPER(%s)"
        params.append(country)
    if region:
        where += " AND LOWER(c.region) = LOWER(%s)"
        params.append(region)
    if year:
        where += " AND f.year = %s"
        params.append(year)
    if from_year:
        where += " AND f.year >= %s"
        params.append(from_year)
    if to_year:
        where += " AND f.year <= %s"
        params.append(to_year)

    base_sql = f"""
        FROM fact_indicators f
        JOIN dim_country c ON f.country_iso3 = c.country_iso3
        JOIN dim_time    t ON f.year         = t.year
        {where}
    """

    offset = (page - 1) * per_page

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) {base_sql}", params)
            total = cur.fetchone()["count"]

            cur.execute(f"""
                SELECT
                    f.country_iso3, c.country_name, c.region, c.sub_region,
                    f.year, t.decade_label, t.year_group,
                    f.gdp_growth, f.unemployment,
                    f.gdp_growth_lag1, f.gdp_growth_roll5, f.unemp_roll5
                {base_sql}
                ORDER BY c.country_name, f.year
                LIMIT %s OFFSET %s
            """, params + [per_page, offset])
            rows = cur.fetchall()

    if not rows:
        log.warning("GET /data — no results for filters applied")
        raise HTTPException(status_code=404, detail="No data found for the given filters.")

    log.info("GET /data — returned %d rows (total=%d)", len(rows), total)
    return {
        "page":     page,
        "per_page": per_page,
        "total":    total,
        "pages":    -(-total // per_page),
        "data":     [dict(r) for r in rows],
    }


#Single country

@app.get("/data/{country_iso3}")
def get_country_data(
    country_iso3: str,
    page:         int = Query(1,  ge=1,  description="Page number"),
    per_page:     int = Query(20, ge=1, le=100, description="Results per page"),
):
    log.info("GET /data/%s  page=%d", country_iso3, page)
    offset = (page - 1) * per_page

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM fact_indicators
                WHERE UPPER(country_iso3) = UPPER(%s)
            """, (country_iso3,))
            total = cur.fetchone()["count"]

            if total == 0:
                log.warning("GET /data/%s — country not found", country_iso3)
                raise HTTPException(status_code=404, detail=f"Country '{country_iso3}' not found.")

            cur.execute("""
                SELECT
                    f.country_iso3, c.country_name, c.region, c.sub_region,
                    f.year, t.decade_label,
                    f.gdp_growth, f.unemployment,
                    f.gdp_growth_lag1, f.gdp_growth_roll5, f.unemp_roll5
                FROM fact_indicators f
                JOIN dim_country c ON f.country_iso3 = c.country_iso3
                JOIN dim_time    t ON f.year         = t.year
                WHERE UPPER(f.country_iso3) = UPPER(%s)
                ORDER BY f.year
                LIMIT %s OFFSET %s
            """, (country_iso3, per_page, offset))
            rows = cur.fetchall()

    log.info("GET /data/%s — returned %d rows", country_iso3, len(rows))
    return {
        "page":     page,
        "per_page": per_page,
        "total":    total,
        "pages":    -(-total // per_page),
        "data":     [dict(r) for r in rows],
    }


#Summary

@app.get("/summary")
def get_summary(
    region:   Optional[str] = Query(None, description="Filter by region"),
    page:     int           = Query(1,  ge=1,  description="Page number"),
    per_page: int           = Query(20, ge=1, le=100, description="Results per page"),
):
    log.info("GET /summary  region=%s page=%d", region, page)
    offset = (page - 1) * per_page

    where  = "WHERE 1=1"
    params = []
    if region:
        where += " AND LOWER(c.region) = LOWER(%s)"
        params.append(region)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT COUNT(DISTINCT f.country_iso3)
                FROM fact_indicators f
                JOIN dim_country c ON f.country_iso3 = c.country_iso3
                {where}
            """, params)
            total = cur.fetchone()["count"]

            cur.execute(f"""
                SELECT
                    f.country_iso3,
                    c.country_name,
                    c.region,
                    COUNT(f.year)                          AS years_available,
                    ROUND(AVG(f.gdp_growth)::numeric,  2) AS avg_gdp_growth,
                    ROUND(AVG(f.unemployment)::numeric, 2) AS avg_unemployment,
                    ROUND(MIN(f.gdp_growth)::numeric,  2) AS min_gdp_growth,
                    ROUND(MAX(f.gdp_growth)::numeric,  2) AS max_gdp_growth
                FROM fact_indicators f
                JOIN dim_country c ON f.country_iso3 = c.country_iso3
                {where}
                GROUP BY f.country_iso3, c.country_name, c.region
                ORDER BY c.country_name
                LIMIT %s OFFSET %s
            """, params + [per_page, offset])
            rows = cur.fetchall()

    log.info("GET /summary — returned %d countries", len(rows))
    return {
        "page":     page,
        "per_page": per_page,
        "total":    total,
        "pages":    -(-total // per_page),
        "summary":  [dict(r) for r in rows],
    }




@app.get("/regions")
def get_regions():
    log.info("GET /regions")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT region, COUNT(*) AS country_count
                FROM dim_country
                GROUP BY region
                ORDER BY region
            """)
            rows = cur.fetchall()
    return {"regions": [dict(r) for r in rows]}
