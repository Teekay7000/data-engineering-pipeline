import logging
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, timezone
from contextlib import contextmanager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [data_model] %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "worldbank_africa",
    "user":     "postgres",
    "password": "your__password here",
}

AFRICAN_REGIONS = {
    "DZA": ("Northern Africa",  "Maghreb"),
    "EGY": ("Northern Africa",  "Maghreb"),
    "LBY": ("Northern Africa",  "Maghreb"),
    "MAR": ("Northern Africa",  "Maghreb"),
    "TUN": ("Northern Africa",  "Maghreb"),
    "SDN": ("Northern Africa",  "Nile Region"),
    "NGA": ("Western Africa",   "Gulf of Guinea"),
    "GHA": ("Western Africa",   "Gulf of Guinea"),
    "CIV": ("Western Africa",   "Gulf of Guinea"),
    "SEN": ("Western Africa",   "Sahel"),
    "MLI": ("Western Africa",   "Sahel"),
    "BFA": ("Western Africa",   "Sahel"),
    "NER": ("Western Africa",   "Sahel"),
    "GMB": ("Western Africa",   "Senegambia"),
    "GNB": ("Western Africa",   "Senegambia"),
    "SLE": ("Western Africa",   "Mano River"),
    "LBR": ("Western Africa",   "Mano River"),
    "GIN": ("Western Africa",   "Mano River"),
    "MRT": ("Western Africa",   "Sahel"),
    "CPV": ("Western Africa",   "Island States"),
    "BEN": ("Western Africa",   "Gulf of Guinea"),
    "TGO": ("Western Africa",   "Gulf of Guinea"),
    "ETH": ("Eastern Africa",   "Horn of Africa"),
    "SOM": ("Eastern Africa",   "Horn of Africa"),
    "ERI": ("Eastern Africa",   "Horn of Africa"),
    "DJI": ("Eastern Africa",   "Horn of Africa"),
    "KEN": ("Eastern Africa",   "Great Lakes"),
    "TZA": ("Eastern Africa",   "Great Lakes"),
    "UGA": ("Eastern Africa",   "Great Lakes"),
    "RWA": ("Eastern Africa",   "Great Lakes"),
    "BDI": ("Eastern Africa",   "Great Lakes"),
    "MDG": ("Eastern Africa",   "Island States"),
    "MUS": ("Eastern Africa",   "Island States"),
    "SYC": ("Eastern Africa",   "Island States"),
    "COM": ("Eastern Africa",   "Island States"),
    "SSD": ("Eastern Africa",   "Nile Region"),
    "ZAF": ("Southern Africa",  "SADC"),
    "NAM": ("Southern Africa",  "SADC"),
    "BWA": ("Southern Africa",  "SADC"),
    "ZWE": ("Southern Africa",  "SADC"),
    "ZMB": ("Southern Africa",  "SADC"),
    "MOZ": ("Southern Africa",  "SADC"),
    "MWI": ("Southern Africa",  "SADC"),
    "SWZ": ("Southern Africa",  "SADC"),
    "LSO": ("Southern Africa",  "SADC"),
    "AGO": ("Southern Africa",  "SADC"),
    "CMR": ("Central Africa",   "ECCAS"),
    "CAF": ("Central Africa",   "ECCAS"),
    "TCD": ("Central Africa",   "ECCAS"),
    "COD": ("Central Africa",   "ECCAS"),
    "COG": ("Central Africa",   "ECCAS"),
    "GAB": ("Central Africa",   "ECCAS"),
    "GNQ": ("Central Africa",   "ECCAS"),
    "STP": ("Central Africa",   "Island States"),
}


@contextmanager
def get_conn():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def create_star_schema():
    sql = """
    CREATE TABLE IF NOT EXISTS dim_country (
        country_iso3   CHAR(3)   PRIMARY KEY,
        country_name   TEXT      NOT NULL,
        region         TEXT,
        sub_region     TEXT
    );

    CREATE TABLE IF NOT EXISTS dim_time (
        year           SMALLINT  PRIMARY KEY,
        decade         SMALLINT  NOT NULL,
        decade_label   TEXT      NOT NULL,
        year_group     TEXT      NOT NULL
    );

    CREATE TABLE IF NOT EXISTS fact_indicators (
        id                  SERIAL        PRIMARY KEY,
        country_iso3        CHAR(3)       NOT NULL REFERENCES dim_country(country_iso3),
        year                SMALLINT      NOT NULL REFERENCES dim_time(year),
        gdp_growth          NUMERIC(10,4),
        unemployment        NUMERIC(10,4),
        gdp_growth_lag1     NUMERIC(10,4),
        gdp_growth_roll5    NUMERIC(10,4),
        unemp_roll5         NUMERIC(10,4),
        loaded_at           TIMESTAMPTZ   NOT NULL,
        UNIQUE (country_iso3, year)
    );
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            for stmt in sql.strip().split(";"):
                stmt = stmt.strip()
                if stmt:
                    cur.execute(stmt)

    log.info("Star schema created: dim_country, dim_time, fact_indicators")


def load_dim_country():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT country_iso3, country_name FROM cleaned_data")
            countries = cur.fetchall()

    rows = []
    for iso3, name in countries:
        region, sub_region = AFRICAN_REGIONS.get(iso3, ("Unknown", "Unknown"))
        rows.append((iso3, name, region, sub_region))

    sql = """
        INSERT INTO dim_country (country_iso3, country_name, region, sub_region)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (country_iso3)
        DO UPDATE SET
            country_name = EXCLUDED.country_name,
            region       = EXCLUDED.region,
            sub_region   = EXCLUDED.sub_region;
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            execute_batch(cur, sql, rows)

    log.info("Loaded %d rows → dim_country", len(rows))


def load_dim_time():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT year FROM cleaned_data ORDER BY year")
            years = [row[0] for row in cur.fetchall()]

    rows = []
    for year in years:
        decade       = (year // 10) * 10
        decade_label = f"{decade}s"
        if year <= 2005:
            year_group = "Early 2000s"
        elif year <= 2010:
            year_group = "Late 2000s"
        elif year <= 2015:
            year_group = "Early 2010s"
        elif year <= 2020:
            year_group = "Late 2010s"
        else:
            year_group = "Early 2020s"

        rows.append((year, decade, decade_label, year_group))

    sql = """
        INSERT INTO dim_time (year, decade, decade_label, year_group)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (year)
        DO UPDATE SET
            decade       = EXCLUDED.decade,
            decade_label = EXCLUDED.decade_label,
            year_group   = EXCLUDED.year_group;
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            execute_batch(cur, sql, rows)

    log.info("Loaded %d rows → dim_time", len(rows))


def load_fact_indicators():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT country_iso3, year, gdp_growth, unemployment,
                       gdp_growth_lag1, gdp_growth_roll5, unemp_roll5
                FROM cleaned_data
                ORDER BY country_iso3, year
            """)
            cols = [desc[0] for desc in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]

    now  = datetime.now(timezone.utc)
    data = [
        (
            r["country_iso3"], r["year"],
            r["gdp_growth"],   r["unemployment"],
            r["gdp_growth_lag1"], r["gdp_growth_roll5"], r["unemp_roll5"],
            now
        )
        for r in rows
    ]

    sql = """
        INSERT INTO fact_indicators
            (country_iso3, year, gdp_growth, unemployment,
             gdp_growth_lag1, gdp_growth_roll5, unemp_roll5, loaded_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (country_iso3, year)
        DO UPDATE SET
            gdp_growth        = EXCLUDED.gdp_growth,
            unemployment      = EXCLUDED.unemployment,
            gdp_growth_lag1   = EXCLUDED.gdp_growth_lag1,
            gdp_growth_roll5  = EXCLUDED.gdp_growth_roll5,
            unemp_roll5       = EXCLUDED.unemp_roll5,
            loaded_at         = EXCLUDED.loaded_at;
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            execute_batch(cur, sql, data, page_size=500)

    log.info("Loaded %d rows → fact_indicators", len(data))


def preview():
    sql = """
        SELECT
            c.country_name,
            c.region,
            c.sub_region,
            t.year,
            t.decade_label,
            t.year_group,
            f.gdp_growth,
            f.unemployment,
            f.gdp_growth_lag1,
            f.gdp_growth_roll5,
            f.unemp_roll5
        FROM fact_indicators f
        JOIN dim_country c ON f.country_iso3 = c.country_iso3
        JOIN dim_time    t ON f.year         = t.year
        ORDER BY c.country_iso3, t.year
        LIMIT 10;
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [desc[0] for desc in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]

    def fmt(v):
        return f"{float(v):.3f}" if v is not None else "NULL"

    print(f"\n STAR SCHEMA PREVIEW (first 10 rows)")
    print(f"  {'Country':<22} {'Region':<17} {'Year':<6} {'Decade':<8} {'GDP%':<8} {'UNEMP%'}")
    print("  " + "-" * 75)
    for r in rows:
        print(f"  {r['country_name']:<22} {r['region']:<17} {r['year']:<6} "
              f"{r['decade_label']:<8} {fmt(r['gdp_growth']):<8} {fmt(r['unemployment'])}")


if __name__ == "__main__":
    log.info("STEP 1 Create star schema tables")
    create_star_schema()

    log.info("STEP 2 Load dim_country")
    load_dim_country()

    log.info("STEP 3 Load dim_time")
    load_dim_time()

    log.info("STEP 4 Load fact_indicators")
    load_fact_indicators()

    preview()
