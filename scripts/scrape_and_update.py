"""
scrape_and_update.py
====================
Eastern Suburbs Melbourne Ã¢ÂÂ weekly price scraper.
Runs via GitHub Actions every Sunday 6 AM UTC.

Flow:
  1. Search web for current prices (businesses + fuel)
  2. Upsert results into Supabase
  3. Log the run result

Environment variables (set as GitHub Actions secrets):
  SUPABASE_URL   Ã¢ÂÂ https://mpbphijerbizlvfhssww.supabase.co
  SUPABASE_KEY   Ã¢ÂÂ service_role key
"""

import os
import json
import time
import re
import requests
import datetime
from typing import Optional

# Ã¢ÂÂÃ¢ÂÂ Config Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

SUBURBS = [
    {"name": "Rowville",       "postcode": "3178"},
    {"name": "Doncaster",      "postcode": "3108"},
    {"name": "Glen Waverley",  "postcode": "3150"},
    {"name": "Boronia",        "postcode": "3155"},
    {"name": "Ferntree Gully", "postcode": "3156"},
    {"name": "Belgrave",       "postcode": "3160"},
    {"name": "Croydon",        "postcode": "3136"},
    {"name": "Ringwood",       "postcode": "3134"},
    {"name": "Blackburn",      "postcode": "3130"},
    {"name": "Box Hill",       "postcode": "3128"},
    {"name": "Mooroolbark",    "postcode": "3138"},
]

CATEGORIES = ["barbers", "gyms", "salons", "cafes", "dentists"]

FUEL_BRANDS = ["Ampol", "BP", "7-Eleven", "Coles Express", "United", "Liberty",
               "Shell", "Metro", "Reddy Express", "Freedom", "Astron"]

TODAY = datetime.date.today().isoformat()

# Ã¢ÂÂÃ¢ÂÂ Supabase helpers Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates",
}


def supabase_upsert(table: str, rows: list[dict]) -> dict:
    """Upsert a list of row dicts into a Supabase table."""
    if not rows:
        return {"upserted": 0}
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    resp = requests.post(url, headers=HEADERS, json=rows, timeout=30)
    resp.raise_for_status()
    return {"upserted": len(rows)}


def supabase_insert_log(businesses_updated: int, fuel_updated: int,
                        errors: str, status: str):
    url = f"{SUPABASE_URL}/rest/v1/scrape_log"
    row = {
        "businesses_updated": businesses_updated,
        "fuel_stations_updated": fuel_updated,
        "errors": errors or None,
        "status": status,
    }
    headers = {**HEADERS, "Prefer": "return=minimal"}
    requests.post(url, headers=headers, json=row, timeout=15)


# Ã¢ÂÂÃ¢ÂÂ Price extraction helpers Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ

def extract_price(text: str) -> Optional[float]:
    """Pull first AUD dollar/cents price from a string."""
    # Match patterns like $45, $5.50, 167.9c, 189.7c/L
    patterns = [
        r"\$(\d{1,4}(?:\.\d{1,2})?)",   # $45 or $5.50
        r"(\d{2,4}(?:\.\d)?)\s*[cC](?:/[lL])?",  # 167.9c or 189.7c/L
    ]
    for p in patterns:
        m = re.search(p, text)
        if m:
            val = float(m.group(1))
            # Convert cents-per-litre to float (keep as-is, stored as c/L)
            return round(val, 2)
    return None


def extract_rating(text: str) -> Optional[float]:
    m = re.search(r"(\d\.\d)\s*(?:stars?|/5|out of 5)?", text, re.I)
    return float(m.group(1)) if m else None


# Ã¢ÂÂÃ¢ÂÂ Business scraping Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ

CATEGORY_QUERIES = {
    "barbers": ("barber", "men's haircut"),
    "gyms":    ("gym", "monthly membership"),
    "salons":  ("hair salon", "women's haircut"),
    "cafes":   ("cafe", "flat white"),
    "dentists": ("dentist", "check-up"),
}

CATEGORY_SERVICE = {
    "barbers":  "Men's haircut",
    "gyms":     "Monthly membership",
    "salons":   "Women's haircut",
    "cafes":    "Flat white",
    "dentists": "General check-up",
}


def scrape_businesses(session: requests.Session) -> list[dict]:
    """
    Use the Google Custom Search JSON API (or fallback: DuckDuckGo instant)
    to find current prices for each suburb ÃÂ category combination.

    Since this runs in GitHub Actions (no proxy restrictions), it can hit
    public search endpoints.
    """
    rows = []
    errors = []

    for suburb in SUBURBS:
        for category in CATEGORIES:
            keyword, price_term = CATEGORY_QUERIES[category]
            query = f"{suburb['name']} {keyword} price {price_term} Melbourne 2026"

            try:
                # DuckDuckGo instant answers Ã¢ÂÂ lightweight, no API key needed
                resp = session.get(
                    "https://api.duckduckgo.com/",
                    params={"q": query, "format": "json", "no_html": "1", "skip_disambig": "1"},
                    timeout=10,
                )
                data = resp.json()

                # Extract AbstractText for context
                abstract = data.get("AbstractText", "") or ""
                answer = data.get("Answer", "") or ""
                combined = f"{abstract} {answer}"

                price = extract_price(combined)
                rating = extract_rating(combined)

                if price:
                    row_id = f"{suburb['name'].lower().replace(' ', '')}-{category}-scraped"
                    rows.append({
                        "id": row_id,
                        "business_name": f"{suburb['name']} {keyword.title()} (scraped)",
                        "suburb": suburb["name"],
                        "postcode": suburb["postcode"],
                        "category": category,
                        "service_type": CATEGORY_SERVICE[category],
                        "price": price,
                        "currency": "AUD",
                        "rating": rating,
                        "source": "DuckDuckGo / web scrape",
                        "verified": False,
                        "verification_level": "50%",
                        "date_scrapped": TODAY,
                        "last_updated": datetime.datetime.utcnow().isoformat(),
                    })

                time.sleep(0.5)  # be polite

            except Exception as e:
                errors.append(f"{suburb['name']}/{category}: {e}")
                continue

    print(f"  Businesses scraped: {len(rows)}, errors: {len(errors)}")
    if errors:
        print(f"  Errors: {errors[:5]}")
    return rows


# Ã¢ÂÂÃ¢ÂÂ Fuel scraping Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ

# -- Fuel type code mappings --------------------------------------------------

# Victorian Servo Saver API fuel type codes -> our column names
SERVO_SAVER_TO_COL = {
    "ULP":    "ulp91",
    "E10":    "e10",
    "PULP":   "premium95",
    "PULP98": "premium98",
    "PDL":    "diesel",
    "LPG":    "lpg",
}

# Servo Saver API — postcodes covering our Eastern suburbs
# (the API accepts a postcode and returns all stations within ~5km)
SERVO_SAVER_BASE = "https://api.servosaver.vic.gov.au/fuel/prices"


def fetch_servo_saver(session: requests.Session, consumer_id: str) -> list[dict]:
    """
    Fetch ALL Victorian fuel station prices from the Servo Saver Public API.
    Docs: https://service.vic.gov.au/find-services/transport-and-driving/servo-saver/help-centre/servo-saver-public-api
    Returns a flat list of station price dicts.
    """
    headers = {
        "Consumer-Id": consumer_id,
        "Accept": "application/json",
        "User-Agent": "EastsidePriceScraper/3.0",
    }
    try:
        resp = session.get(SERVO_SAVER_BASE, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        # Response shape (subject to confirmation from API docs):
        # {"stations": [...]} or a flat list
        if isinstance(data, list):
            return data
        return data.get("stations", data.get("data", []))
    except Exception as e:
        print(f"    Servo Saver API error: {e}")
        return []


def parse_servo_saver_stations(all_stations: list[dict]) -> list[dict]:
    """
    Filter Servo Saver results to our target postcodes and map to
    our fuel_stations schema.
    """
    target_postcodes = {s["postcode"] for s in SUBURBS}
    # Build a quick postcode -> suburb name lookup
    postcode_to_suburb = {s["postcode"]: s["name"] for s in SUBURBS}

    rows = []
    for st in all_stations:
        postcode = str(st.get("postCode", st.get("postcode", "")))
        if postcode not in target_postcodes:
            continue

        suburb_name = postcode_to_suburb[postcode]
        price_cols: dict[str, Optional[float]] = {}
        cheapest_price = None
        cheapest_type = None

        for p in st.get("prices", []):
            fuel_code = p.get("fuelType", p.get("type", ""))
            col = SERVO_SAVER_TO_COL.get(fuel_code)
            if col is None:
                continue
            try:
                price_val = float(p.get("price", p.get("amount", 0)))
            except (ValueError, TypeError):
                continue
            price_cols[col] = price_val
            if col == "ulp91":
                cheapest_price = price_val
                cheapest_type = "ULP91"
            elif cheapest_price is None:
                cheapest_price = price_val
                cheapest_type = fuel_code

        station_id = str(st.get("stationId", st.get("id", "")))
        row_id = re.sub(r"[^a-zA-Z0-9\-]", "-", f"ss-{station_id}")[:80]

        lat = st.get("latitude", st.get("lat"))
        lng = st.get("longitude", st.get("lng"))
        google_maps = f"https://www.google.com/maps?q={lat},{lng}" if lat and lng else None

        rows.append({
            "id": row_id,
            "station_name": st.get("stationName", st.get("name", "Unknown")),
            "brand": st.get("brand", st.get("brandName", "Unknown")),
            "suburb": suburb_name,
            "postcode": postcode,
            "address": st.get("address", st.get("streetAddress", "")),
            "ulp91":     price_cols.get("ulp91"),
            "e10":       price_cols.get("e10"),
            "premium95": price_cols.get("premium95"),
            "premium98": price_cols.get("premium98"),
            "diesel":    price_cols.get("diesel"),
            "lpg":       price_cols.get("lpg"),
            "cheapest_fuel": cheapest_type,
            "google_maps_url": google_maps,
            "source": "Servo Saver (Victorian Government)",
            "source_url": "https://service.vic.gov.au/find-services/transport-and-driving/servo-saver",
            "verified": True,
            "date_scrapped": TODAY,
            "last_updated": datetime.datetime.utcnow().isoformat(),
        })
    return rows


def build_suburb_summary(station_rows: list[dict]) -> list[dict]:
    """Build suburb-level fuel summary from a list of station rows."""
    summary_by_suburb: dict[str, dict] = {}
    for suburb in SUBURBS:
        rows = [r for r in station_rows if r["suburb"] == suburb["name"]]
        ulp_prices = [r["ulp91"]  for r in rows if r.get("ulp91")]
        e10_prices  = [r["e10"]   for r in rows if r.get("e10")]
        dsl_prices  = [r["diesel"] for r in rows if r.get("diesel")]

        if ulp_prices or e10_prices or dsl_prices:
            cheapest_ulp = min(ulp_prices) if ulp_prices else None
            cheapest_station = None
            if cheapest_ulp is not None:
                cheapest_station = next(
                    (r["station_name"] for r in rows if r.get("ulp91") == cheapest_ulp),
                    None,
                )
            summary_by_suburb[suburb["name"]] = {
                "suburb": suburb["name"],
                "postcode": suburb["postcode"],
                "station_count": len(rows),
                "cheapest_ulp91": cheapest_ulp,
                "cheapest_ulp91_station": cheapest_station,
                "cheapest_e10": min(e10_prices) if e10_prices else None,
                "cheapest_diesel": min(dsl_prices) if dsl_prices else None,
                "avg_ulp91": round(sum(ulp_prices) / len(ulp_prices), 2) if ulp_prices else None,
                "last_updated": TODAY,
            }
    return list(summary_by_suburb.values())


def scrape_fuel(session: requests.Session) -> tuple[list[dict], list[dict]]:
    """
    Scrape current fuel prices.
    - Uses Victorian Servo Saver API if SERVO_SAVER_KEY secret is set (accurate, official).
    - Falls back to PetrolSpy if key not yet available.
    Returns (station_rows, summary_rows).
    """
    servo_saver_key = os.environ.get("SERVO_SAVER_KEY", "").strip()

    if servo_saver_key:
        print("  Using Victorian Servo Saver API (official government data)...")
        all_stations = fetch_servo_saver(session, servo_saver_key)
        if all_stations:
            station_rows = parse_servo_saver_stations(all_stations)
            print(f"  Fuel stations fetched: {len(station_rows)}")
            return station_rows, build_suburb_summary(station_rows)
        else:
            print("  Servo Saver returned no data — falling back to PetrolSpy")

    # Fallback: PetrolSpy (may be blocked by GitHub Actions IPs)
    print("  SERVO_SAVER_KEY not set — using PetrolSpy fallback")
    print("  (Apply at service.vic.gov.au to get accurate daily data)")
    station_rows = []

    for suburb in SUBURBS:
        print(f"    Fetching {suburb['name']} ({suburb['postcode']})...")
        url = "https://petrolspy.com.au/webservice-1/station/list"
        try:
            resp = session.get(
                url,
                params={"postcode": suburb["postcode"]},
                headers={
                    "User-Agent": "Mozilla/5.0 (compatible; EastsidePriceScraper/3.0)",
                    "Accept": "application/json",
                    "Referer": "https://petrolspy.com.au/",
                },
                timeout=15,
            )
            resp.raise_for_status()
            raw = resp.json().get("message", {}).get("list", [])

            for st in raw:
                price_cols: dict[str, Optional[float]] = {}
                cheapest_price = None
                cheapest_type = None
                fuel_map = {"ULP": "ulp91", "E10": "e10", "PULP": "premium95",
                            "P98": "premium98", "DL": "diesel", "LPG": "lpg"}
                for p in st.get("prices", []):
                    col = fuel_map.get(p.get("type", ""))
                    if not col:
                        continue
                    try:
                        pv = float(p["price"])
                    except (KeyError, ValueError, TypeError):
                        continue
                    price_cols[col] = pv
                    if col == "ulp91":
                        cheapest_price, cheapest_type = pv, "ULP91"
                    elif cheapest_price is None:
                        cheapest_price, cheapest_type = pv, p.get("type")

                raw_id = f"ps-{st.get('id', suburb['postcode'])}"
                row_id = re.sub(r"[^a-zA-Z0-9\-]", "-", raw_id)[:80]
                lat, lng = st.get("lat"), st.get("lng")
                station_rows.append({
                    "id": row_id,
                    "station_name": st.get("name", "Unknown"),
                    "brand": st.get("brand", "Unknown"),
                    "suburb": suburb["name"],
                    "postcode": suburb["postcode"],
                    "address": st.get("address", ""),
                    **price_cols,
                    "cheapest_fuel": cheapest_type,
                    "google_maps_url": f"https://www.google.com/maps?q={lat},{lng}" if lat and lng else None,
                    "source": "PetrolSpy",
                    "source_url": f"https://petrolspy.com.au/map/location/au/vic/{suburb['postcode']}",
                    "verified": True,
                    "date_scrapped": TODAY,
                    "last_updated": datetime.datetime.utcnow().isoformat(),
                })
        except Exception as e:
            print(f"    PetrolSpy error for {suburb['postcode']}: {e}")

        time.sleep(0.5)

    print(f"  Fuel stations scraped: {len(station_rows)}")
    return station_rows, build_suburb_summary(station_rows)


# Ã¢ÂÂÃ¢ÂÂ Main Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ

def main():
    print(f"=== Eastside Price Scraper Ã¢ÂÂ {TODAY} ===")
    session = requests.Session()
    session.headers.update({"User-Agent": "EastsidePriceScraper/1.0"})

    total_businesses = 0
    total_fuel = 0
    all_errors = []

    # 1. Scrape businesses
    print("\n[1/3] Scraping business prices...")
    try:
        biz_rows = scrape_businesses(session)
        if biz_rows:
            result = supabase_upsert("businesses", biz_rows)
            total_businesses = result["upserted"]
            print(f"  Ã¢ÂÂ Upserted {total_businesses} business rows")
    except Exception as e:
        all_errors.append(f"businesses: {e}")
        print(f"  Ã¢ÂÂ Error: {e}")

    # 2. Scrape fuel
    print("\n[2/3] Scraping fuel prices...")
    try:
        fuel_rows, summary_rows = scrape_fuel(session)
        if fuel_rows:
            result = supabase_upsert("fuel_stations", fuel_rows)
            total_fuel = result["upserted"]
            print(f"  Ã¢ÂÂ Upserted {total_fuel} fuel station rows")
        if summary_rows:
            supabase_upsert("suburb_fuel_summary", summary_rows)
            print(f"  Ã¢ÂÂ Upserted {len(summary_rows)} suburb summaries")
    except Exception as e:
        all_errors.append(f"fuel: {e}")
        print(f"  Ã¢ÂÂ Error: {e}")

    # 3. Log run
    print("\n[3/3] Logging run...")
    status = "success" if not all_errors else ("partial" if total_businesses + total_fuel > 0 else "failed")
    supabase_insert_log(total_businesses, total_fuel, "; ".join(all_errors), status)
    print(f"  Ã¢ÂÂ Logged: status={status}")

    print(f"\n=== Done. Businesses: {total_businesses}, Fuel: {total_fuel} ===")


if __name__ == "__main__":
    main()
