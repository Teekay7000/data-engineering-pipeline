import logging
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, timezone
from contextlib import contextmanager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [transformer] %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "worldbank_africa",
    "user":     "postgres",
    "password": "2411",
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


def create_cleaned_table():
    sql = """
    CREATE TABLE IF NOT EXISTS cleaned_data (
        id                  SERIAL          PRIMARY KEY,
        country_iso3        CHAR(3)         NOT NULL,
        country_name        TEXT,
        year                SMALLINT        NOT NULL,
        gdp_growth          NUMERIC(10, 4),
        unemployment        NUMERIC(10, 4),
        gdp_growth_lag1     NUMERIC(10, 4),
        gdp_growth_roll5    NUMERIC(10, 4),
        unemp_roll5         NUMERIC(10, 4),
        cleaned_at          TIMESTAMPTZ     NOT NULL,
        UNIQUE (country_iso3, year)
    );
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
    log.info("Table ready: cleaned_data")


def load_and_join_raw():
    sql = """
        SELECT
            g.country_iso3,
            g.country_name,
            g.year,
            g.value   AS gdp_growth,
            u.value   AS unemployment
        FROM raw_gdp_growth g
        INNER JOIN raw_unemployment u
            ON g.country_iso3 = u.country_iso3
            AND g.year        = u.year
        WHERE g.value IS NOT NULL
          AND u.value IS NOT NULL
        ORDER BY g.country_iso3, g.year;
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [desc[0] for desc in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]

    log.info("Loaded %d rows after joining and dropping NULLs", len(rows))
    return rows


def compute_features(rows):
    from collections import defaultdict

    grouped = defaultdict(list)
    for row in rows:
        grouped[row["country_iso3"]].append(row)

    enriched = []
    for iso3, records in grouped.items():
        records.sort(key=lambda r: r["year"])

        gdp_values   = [r["gdp_growth"]   for r in records]
        unemp_values = [r["unemployment"] for r in records]

        for i, row in enumerate(records):
            lag1 = float(gdp_values[i - 1]) if i >= 1 else None

            window_gdp   = [float(v) for v in gdp_values[max(0, i - 4):i + 1]   if v is not None]
            window_unemp = [float(v) for v in unemp_values[max(0, i - 4):i + 1] if v is not None]

            roll5_gdp   = round(sum(window_gdp)   / len(window_gdp),   4) if len(window_gdp)   >= 3 else None
            roll5_unemp = round(sum(window_unemp) / len(window_unemp), 4) if len(window_unemp) >= 3 else None

            enriched.append({
                "country_iso3":     row["country_iso3"],
                "country_name":     row["country_name"],
                "year":             row["year"],
                "gdp_growth":       round(float(row["gdp_growth"]),   4),
                "unemployment":     round(float(row["unemployment"]), 4),
                "gdp_growth_lag1":  round(lag1, 4) if lag1 is not None else None,
                "gdp_growth_roll5": roll5_gdp,
                "unemp_roll5":      roll5_unemp,
            })

    log.info("Features computed for %d records across %d countries", len(enriched), len(grouped))
    return enriched


def save_cleaned_data(rows):
    now = datetime.now(timezone.utc)
    sql = """
        INSERT INTO cleaned_data
            (country_iso3, country_name, year, gdp_growth, unemployment,
             gdp_growth_lag1, gdp_growth_roll5, unemp_roll5, cleaned_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (country_iso3, year)
        DO UPDATE SET
            country_name      = EXCLUDED.country_name,
            gdp_growth        = EXCLUDED.gdp_growth,
            unemployment      = EXCLUDED.unemployment,
            gdp_growth_lag1   = EXCLUDED.gdp_growth_lag1,
            gdp_growth_roll5  = EXCLUDED.gdp_growth_roll5,
            unemp_roll5       = EXCLUDED.unemp_roll5,
            cleaned_at        = EXCLUDED.cleaned_at;
    """
    data = [
        (
            r["country_iso3"], r["country_name"], r["year"],
            r["gdp_growth"],   r["unemployment"],
            r["gdp_growth_lag1"], r["gdp_growth_roll5"], r["unemp_roll5"],
            now
        )
        for r in rows
    ]

    with get_conn() as conn:
        with conn.cursor() as cur:
            execute_batch(cur, sql, data, page_size=500)

    log.info("Saved %d rows → cleaned_data", len(data))
    return len(data)


def preview():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM cleaned_data")
            total = cur.fetchone()[0]

            cur.execute("""
                SELECT country_iso3, country_name, year, gdp_growth,
                       unemployment, gdp_growth_lag1, gdp_growth_roll5, unemp_roll5
                FROM cleaned_data
                ORDER BY country_iso3, year
                LIMIT 10
            """)
            cols = [desc[0] for desc in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]

    def fmt(v):
        return f"{float(v):.3f}" if v is not None else "NULL"

    print(f"\n── CLEANED DATA — {total} total rows ───────────────────────────────────")
    print(f"  {'ISO3':<5} {'Country':<22} {'Year':<6} {'GDP%':<8} {'UNEMP%':<8} {'LAG1':<8} {'ROLL5_G':<9} {'ROLL5_U'}")
    print("  " + "-" * 82)
    for r in rows:
        print(f"  {r['country_iso3']:<5} {r['country_name']:<22} {r['year']:<6} "
              f"{fmt(r['gdp_growth']):<8} {fmt(r['unemployment']):<8} "
              f"{fmt(r['gdp_growth_lag1']):<8} {fmt(r['gdp_growth_roll5']):<9} {fmt(r['unemp_roll5'])}")


if __name__ == "__main__":
    log.info("STEP 1 — Create cleaned_data table")
    create_cleaned_table()

    log.info("STEP 2 — Load and join raw tables, drop NULLs")
    raw_rows = load_and_join_raw()

    log.info("STEP 3 — Compute lag and rolling average features")
    enriched_rows = compute_features(raw_rows)

    log.info("STEP 4 — Save cleaned data to PostgreSQL")
    save_cleaned_data(enriched_rows)

    preview()