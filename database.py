"""
database.py
===========
Stores raw World Bank API data (GDP Growth & Unemployment)
for all African countries into PostgreSQL.

Tables created
──────────────
    raw_gdp_growth      — one row per (country, year)
    raw_unemployment    — one row per (country, year)

Usage
─────
    # Configure your DB connection at the top, then:
    python database.py

    # Or import and call from another script:
    from database import init_db, save_raw_records

Requirements
────────────
    pip install psycopg2-binary
"""

import logging
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, timezone
from contextlib import contextmanager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [database] %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── PostgreSQL connection settings — update these ─────────────────────────────
DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "worldbank_africa",
    "user":     "postgres",
    "password": "2411",
}

# Map indicator name → table name
TABLE_MAP = {
    "gdp_growth":   "raw_gdp_growth",
    "unemployment": "raw_unemployment",
}

# ── DDL ───────────────────────────────────────────────────────────────────────
DDL = """
CREATE TABLE IF NOT EXISTS raw_gdp_growth (
    id              SERIAL          PRIMARY KEY,
    country_iso3    CHAR(3)         NOT NULL,
    country_name    TEXT,
    year            SMALLINT        NOT NULL,
    value           NUMERIC(10, 4),             -- GDP growth % (NULL = missing)
    indicator_id    TEXT,
    indicator_name  TEXT,
    fetched_at      TIMESTAMPTZ     NOT NULL,
    UNIQUE (country_iso3, year)
);

CREATE TABLE IF NOT EXISTS raw_unemployment (
    id              SERIAL          PRIMARY KEY,
    country_iso3    CHAR(3)         NOT NULL,
    country_name    TEXT,
    year            SMALLINT        NOT NULL,
    value           NUMERIC(10, 4),             -- Unemployment % (NULL = missing)
    indicator_id    TEXT,
    indicator_name  TEXT,
    fetched_at      TIMESTAMPTZ     NOT NULL,
    UNIQUE (country_iso3, year)
);
"""


# ── Connection ────────────────────────────────────────────────────────────────

@contextmanager
def get_conn():
    """Yield a psycopg2 connection; commit on success, rollback on error."""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── Schema ────────────────────────────────────────────────────────────────────

def init_db() -> None:
    """Create staging tables if they don't already exist."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
    log.info("Tables ready: raw_gdp_growth, raw_unemployment")


# ── Write ─────────────────────────────────────────────────────────────────────

def save_raw_records(indicator_name: str, records: list) -> int:
    """
    Upsert raw World Bank API records into the correct PostgreSQL table.

    Parameters
    ----------
    indicator_name : "gdp_growth" or "unemployment"
    records        : list of raw dicts straight from the World Bank API

    Returns
    -------
    int — number of rows upserted
    """
    table = TABLE_MAP.get(indicator_name)
    if not table:
        raise ValueError(f"Unknown indicator: '{indicator_name}'. "
                         f"Choose from: {list(TABLE_MAP.keys())}")

    now  = datetime.now(timezone.utc)
    rows = []

    for rec in records:
        iso3       = rec.get("countryiso3code") or rec.get("country", {}).get("id", "")
        name       = rec.get("country", {}).get("value", "")
        year_str   = rec.get("date", "")
        value      = rec.get("value")                   # may be None
        ind_id     = rec.get("indicator", {}).get("id", "")
        ind_name   = rec.get("indicator", {}).get("value", "")

        if not iso3 or not year_str:
            continue

        try:
            year = int(year_str)
        except ValueError:
            log.warning("Skipping record with invalid year: %s", year_str)
            continue

        rows.append((iso3, name, year, value, ind_id, ind_name, now))

    if not rows:
        log.warning("No valid rows to insert for indicator: %s", indicator_name)
        return 0

    sql = f"""
        INSERT INTO {table}
            (country_iso3, country_name, year, value,
             indicator_id, indicator_name, fetched_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (country_iso3, year)
        DO UPDATE SET
            country_name   = EXCLUDED.country_name,
            value          = EXCLUDED.value,
            indicator_id   = EXCLUDED.indicator_id,
            indicator_name = EXCLUDED.indicator_name,
            fetched_at     = EXCLUDED.fetched_at;
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            execute_batch(cur, sql, rows, page_size=500)

    log.info("Upserted %d rows → %s", len(rows), table)
    return len(rows)


# ── Read (optional helper) ────────────────────────────────────────────────────

def load_raw_records(indicator_name: str) -> list:
    """
    Read all rows from a raw staging table.
    Returns a list of dicts ordered by country + year.
    """
    table = TABLE_MAP.get(indicator_name)
    if not table:
        raise ValueError(f"Unknown indicator: '{indicator_name}'")

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT * FROM {table} ORDER BY country_iso3, year"
            )
            cols = [desc[0] for desc in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]

    log.info("Loaded %d rows from %s", len(rows), table)
    return rows


# ── Row counts helper ─────────────────────────────────────────────────────────

def row_counts() -> dict:
    """Return row counts for both staging tables."""
    counts = {}
    with get_conn() as conn:
        with conn.cursor() as cur:
            for ind, table in TABLE_MAP.items():
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                counts[ind] = cur.fetchone()[0]
    return counts


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    sys.path.insert(0, ".")

    # Import the fetcher
    try:
        from api_fetcher import fetch_all_african_data
    except ImportError:
        log.error("api_fetcher.py not found — make sure it's in the same directory.")
        sys.exit(1)

    log.info("═" * 55)
    log.info("  STAGE 1 — Initialise PostgreSQL tables")
    log.info("═" * 55)
    init_db()

    log.info("═" * 55)
    log.info("  STAGE 2 — Fetch raw data from World Bank API")
    log.info("═" * 55)
    raw_data = fetch_all_african_data()

    log.info("═" * 55)
    log.info("  STAGE 3 — Store raw data in PostgreSQL")
    log.info("═" * 55)
    for ind_name, records in raw_data.items():
        n = save_raw_records(ind_name, records)
        log.info("  ✓ %-15s  %d rows stored", ind_name, n)

    log.info("═" * 55)
    log.info("  DONE — Final row counts")
    log.info("═" * 55)
    for ind, count in row_counts().items():
        log.info("  %-20s  %d rows", ind, count)