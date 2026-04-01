import logging
import psycopg2
from contextlib import contextmanager
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [data_quality] %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "worldbank_africa",
    "user":     "postgres",
    "password": "yourpassword here",
}

EXPECTED_COUNTRIES = 54
EXPECTED_YEAR_MIN  = 2000
EXPECTED_YEAR_MAX  = 2023
GDP_MIN            = -100.0
GDP_MAX            =  100.0
UNEMP_MIN          =   0.0
UNEMP_MAX          = 100.0


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


passed = []
failed = []


def check(name: str, result: bool, detail: str = ""):
    if result:
        passed.append(name)
        log.info("  PASS — %s %s", name, f"({detail})" if detail else "")
    else:
        failed.append(name)
        log.error("  FAIL — %s %s", name, f"({detail})" if detail else "")


def check_row_counts():
    log.info("CHECK 1: Row Counts")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM raw_gdp_growth")
            gdp_rows = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM raw_unemployment")
            unemp_rows = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM cleaned_data")
            cleaned_rows = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM fact_indicators")
            fact_rows = cur.fetchone()[0]

    check("raw_gdp_growth has rows",    gdp_rows > 0,      f"{gdp_rows} rows")
    check("raw_unemployment has rows",  unemp_rows > 0,    f"{unemp_rows} rows")
    check("cleaned_data has rows",      cleaned_rows > 0,  f"{cleaned_rows} rows")
    check("fact_indicators has rows",   fact_rows > 0,     f"{fact_rows} rows")
    check("cleaned <= raw rows",        cleaned_rows <= gdp_rows, f"{cleaned_rows} <= {gdp_rows}")


def check_country_coverage():
    log.info("CHECK 2: Country Coverage")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(DISTINCT country_iso3) FROM fact_indicators")
            country_count = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM dim_country")
            dim_count = cur.fetchone()[0]

    check("expected country count in fact",  country_count == EXPECTED_COUNTRIES, f"{country_count}/{EXPECTED_COUNTRIES}")
    check("dim_country fully populated",     dim_count == EXPECTED_COUNTRIES,     f"{dim_count}/{EXPECTED_COUNTRIES}")


def check_year_range():
    log.info("CHECK 3: Year Range")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT MIN(year), MAX(year) FROM fact_indicators")
            min_year, max_year = cur.fetchone()

    check("year min is correct",  min_year == EXPECTED_YEAR_MIN, f"{min_year}")
    check("year max is correct",  max_year == EXPECTED_YEAR_MAX, f"{max_year}")


def check_nulls():
    log.info("CHECK 4: NULL Values")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM fact_indicators WHERE gdp_growth IS NULL")
            null_gdp = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM fact_indicators WHERE unemployment IS NULL")
            null_unemp = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM dim_country WHERE country_name IS NULL")
            null_names = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM dim_country WHERE region IS NULL")
            null_regions = cur.fetchone()[0]

    check("no NULL gdp_growth in fact",      null_gdp == 0,     f"{null_gdp} nulls found")
    check("no NULL unemployment in fact",    null_unemp == 0,   f"{null_unemp} nulls found")
    check("no NULL country names",           null_names == 0,   f"{null_names} nulls found")
    check("no NULL regions in dim_country",  null_regions == 0, f"{null_regions} nulls found")


def check_value_ranges():
    log.info("CHECK 5: Value Ranges")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM fact_indicators WHERE gdp_growth < %s OR gdp_growth > %s", (GDP_MIN, GDP_MAX))
            gdp_out = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM fact_indicators WHERE unemployment < %s OR unemployment > %s", (UNEMP_MIN, UNEMP_MAX))
            unemp_out = cur.fetchone()[0]

            cur.execute("SELECT MIN(gdp_growth), MAX(gdp_growth) FROM fact_indicators")
            gdp_min_actual, gdp_max_actual = cur.fetchone()

            cur.execute("SELECT MIN(unemployment), MAX(unemployment) FROM fact_indicators")
            unemp_min_actual, unemp_max_actual = cur.fetchone()

    check("gdp_growth within expected range",    gdp_out == 0,   f"min={gdp_min_actual}, max={gdp_max_actual}")
    check("unemployment within expected range",  unemp_out == 0, f"min={unemp_min_actual}, max={unemp_max_actual}")


def check_duplicates():
    log.info("CHECK 6: Duplicates")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM (
                    SELECT country_iso3, year, COUNT(*)
                    FROM fact_indicators
                    GROUP BY country_iso3, year
                    HAVING COUNT(*) > 1
                ) duplicates
            """)
            dup_fact = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM (
                    SELECT country_iso3, COUNT(*)
                    FROM dim_country
                    GROUP BY country_iso3
                    HAVING COUNT(*) > 1
                ) duplicates
            """)
            dup_country = cur.fetchone()[0]

    check("no duplicate rows in fact_indicators",  dup_fact == 0,    f"{dup_fact} duplicates")
    check("no duplicate rows in dim_country",      dup_country == 0, f"{dup_country} duplicates")


def check_referential_integrity():
    log.info("CHECK 7: Referential Integrity")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM fact_indicators f
                LEFT JOIN dim_country c ON f.country_iso3 = c.country_iso3
                WHERE c.country_iso3 IS NULL
            """)
            orphan_countries = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM fact_indicators f
                LEFT JOIN dim_time t ON f.year = t.year
                WHERE t.year IS NULL
            """)
            orphan_years = cur.fetchone()[0]

    check("all fact country_iso3 exist in dim_country",  orphan_countries == 0, f"{orphan_countries} orphans")
    check("all fact years exist in dim_time",            orphan_years == 0,     f"{orphan_years} orphans")


def check_freshness():
    log.info("CHECK 8: Data Freshness")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(loaded_at) FROM fact_indicators")
            last_loaded = cur.fetchone()[0]

    if last_loaded:
        days_old = (datetime.now(last_loaded.tzinfo) - last_loaded).days
        check("data loaded within last 8 days", days_old <= 8, f"last loaded {days_old} day(s) ago")
    else:
        check("data freshness", False, "no loaded_at found")


def print_summary():
    total = len(passed) + len(failed)
    print(f"\n{'═' * 55}")
    print(f"  DATA QUALITY REPORT")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'═' * 55}")
    print(f"  Total checks : {total}")
    print(f"  Passed       : {len(passed)}")
    print(f"  Failed       : {len(failed)}")
    print(f"{'─' * 55}")

    if failed:
        print(f"\n FAILED CHECKS:")
        for f in failed:
            print(f"     - {f}")
    else:
        print(f"\n  All checks passed, data quality is good.")

    print(f"\n{'═' * 55}\n")


if __name__ == "__main__":
    log.info("Starting data quality checks...")

    check_row_counts()
    check_country_coverage()
    check_year_range()
    check_nulls()
    check_value_ranges()
    check_duplicates()
    check_referential_integrity()
    check_freshness()

    print_summary()
