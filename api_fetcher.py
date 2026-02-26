import time
import json
import urllib.request
import urllib.parse
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [api_fetcher] %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# 
WB_BASE_URL = "https://api.worldbank.org/v2"
START_YEAR  = 2000
END_YEAR    = 2023
PER_PAGE    = 1000

INDICATORS = {
    "gdp_growth":   "NY.GDP.MKTP.KD.ZG",   # GDP growth (annual %)
    "unemployment": "SL.UEM.TOTL.ZS",       # Unemployment, total (% of labour force)
}

AFRICAN_COUNTRIES = [
    "DZA", "AGO", "BEN", "BWA", "BFA", "BDI", "CPV", "CMR", "CAF", "TCD",
    "COM", "COD", "COG", "CIV", "DJI", "EGY", "GNQ", "ERI", "SWZ", "ETH",
    "GAB", "GMB", "GHA", "GIN", "GNB", "KEN", "LSO", "LBR", "LBY", "MDG",
    "MWI", "MLI", "MRT", "MUS", "MAR", "MOZ", "NAM", "NER", "NGA", "RWA",
    "STP", "SEN", "SLE", "SOM", "ZAF", "SSD", "SDN", "TZA", "TGO", "TUN",
    "UGA", "ZMB", "ZWE", "SYC",
]

#

def build_url(country_code: str, indicator_code: str, page: int = 1) -> str:
    params = urllib.parse.urlencode({
        "format":   "json",
        "per_page": PER_PAGE,
        "date":     f"{START_YEAR}:{END_YEAR}",
        "page":     page,
    })
    return f"{WB_BASE_URL}/country/{country_code}/indicator/{indicator_code}?{params}"


def get(url: str, retries: int = 3, backoff: float = 2.0):
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "WB-Pipeline/1.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as exc:
            wait = backoff ** attempt
            log.warning("Attempt %d/%d failed — %s. Retrying in %.1fs…", attempt, retries, exc, wait)
            time.sleep(wait)
    log.error("All retries exhausted for: %s", url)
    return None


#

def fetch_indicator(country_code: str, indicator_code: str) -> list:
    """Fetch all yearly records for one country + indicator (handles pagination)."""
    all_records = []
    page = 1

    while True:
        url  = build_url(country_code, indicator_code, page)
        data = get(url)

        if data is None or len(data) < 2:
            break

        meta, records = data[0], data[1]

        if records:
            all_records.extend(records)

        if page >= int(meta.get("pages", 1)):
            break
        page += 1

    return all_records


def fetch_all_african_data() -> dict:
    """
    Fetch GDP growth and unemployment for all 54 African countries.

    Returns:
        {
            "gdp_growth":   [raw_record, ...],
            "unemployment": [raw_record, ...],
        }
    """
    results = {name: [] for name in INDICATORS}
    total   = len(AFRICAN_COUNTRIES) * len(INDICATORS)
    done    = 0

    for country in AFRICAN_COUNTRIES:
        for ind_name, ind_code in INDICATORS.items():
            log.info("[%d/%d] Fetching %-14s for %s", done + 1, total, ind_name, country)
            records = fetch_indicator(country, ind_code)
            results[ind_name].extend(records)
            done += 1
            time.sleep(0.15)    # polite rate-limiting — don't hammer the API

    log.info("─" * 55)
    log.info("FETCH COMPLETE")
    log.info("  GDP Growth rows    : %d", len(results["gdp_growth"]))
    log.info("  Unemployment rows  : %d", len(results["unemployment"]))
    log.info("─" * 55)
    return results


# 

if __name__ == "__main__":
    raw_data = fetch_all_african_data()

    # Preview first 3 records from each indicator
    for ind_name, records in raw_data.items():
        print(f"\n── {ind_name.upper()} (first 3 records) ──")
        for rec in records[:3]:
            country = rec.get("country", {}).get("value", "?")
            iso3    = rec.get("countryiso3code", "?")
            year    = rec.get("date", "?")
            value   = rec.get("value")

            print(f"  {iso3}  {country:<25}  {year}  →  {value}")
