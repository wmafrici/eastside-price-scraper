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
import uuid
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
    {"name": "Forest Hill",    "postcode": "3131"},
    {"name": "Knox",           "postcode": "3148"},
]

CATEGORIES = ["barbers", "gyms", "salons", "cafes", "dentists"]  # used for reference only

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



# Business competitor scraping is handled on-demand by scripts/scrape_competitors.py
# Run that script manually when onboarding a new trial client.


# Ã¢ÂÂÃ¢ÂÂ Fuel scraping Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ

# -- Fuel type code mappings --------------------------------------------------

# Victorian Fair Fuel Open Data API fuel type codes -> our column names
# https://api.fuel.service.vic.gov.au/open-data/v1
SERVO_SAVER_TO_COL = {
    "U91":  "ulp91",
    "E10":  "e10",
    "P95":  "premium95",
    "P98":  "premium98",
    "DSL":  "diesel",
    "PDSL": "diesel",   # premium diesel -> diesel column
    "LPG":  "lpg",
}

FAIR_FUEL_BASE = "https://api.fuel.service.vic.gov.au/open-data/v1/fuel/prices"


def fetch_servo_saver(session: requests.Session, consumer_id: str) -> list[dict]:
    """
    Fetch ALL Victorian fuel station prices from the Fair Fuel Open Data API.
    Docs: https://api.fuel.service.vic.gov.au/open-data/v1
    Required headers: x-consumer-id, x-transactionid (UUID v4 per request), User-Agent
    Response: {"fuelPriceDetails": [{fuelStation: {...}, fuelPrices: [...]}]}
    Returns a flat list of fuelPriceDetails dicts.
    """
    headers = {
        "x-consumer-id": consumer_id,
        "x-transactionid": str(uuid.uuid4()),
        "User-Agent": "EastsidePriceScraper/3.0",
        "Accept": "application/json",
    }
    try:
        resp = session.get(FAIR_FUEL_BASE, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data.get("fuelPriceDetails", [])
    except Exception:
        return []


def _extract_postcode(address: str) -> str:
    """Extract 4-digit Australian postcode from address string."""
    # Look for 4-digit number at end of address, e.g. "123 Main St, Melbourne VIC 3000"
    matches = re.findall(r"\b(\d{4})\b", address)
    return matches[-1] if matches else ""


def parse_servo_saver_stations(all_entries: list[dict]) -> list[dict]:
    """
    Filter Fair Fuel Open Data API results to our target postcodes and map to
    our fuel_stations schema.
    Each entry has: fuelStation (id, name, brandId, address, location) + fuelPrices[]
    """
    target_postcodes = {s["postcode"] for s in SUBURBS}
    postcode_to_suburb = {s["postcode"]: s["name"] for s in SUBURBS}

    rows = []
    for entry in all_entries:
        station = entry.get("fuelStation", {})
        address = station.get("address", "")
        postcode = _extract_postcode(address)

        if postcode not in target_postcodes:
            continue

        suburb_name = postcode_to_suburb[postcode]
        price_cols: dict[str, Optional[float]] = {}
        cheapest_price = None
        cheapest_type = None

        for p in entry.get("fuelPrices", []):
            if not p.get("isAvailable", True):
                continue
            fuel_code = p.get("fuelType", "")
            col = SERVO_SAVER_TO_COL.get(fuel_code)
            if col is None:
                continue
            try:
                price_val = float(p.get("price", 0))
            except (ValueError, TypeError):
                continue
            # Only overwrite diesel if we don't have a value yet (prefer DSL over PDSL)
            if col in price_cols and col == "diesel":
                pass
            else:
                price_cols[col] = price_val
            if col == "ulp91" and cheapest_price is None:
                cheapest_price = price_val
                cheapest_type = "ULP91"
            elif cheapest_price is None:
                cheapest_price = price_val
                cheapest_type = fuel_code

        station_id = station.get("id", "")
        row_id = re.sub(r"[^a-zA-Z0-9\-]", "-", f"ff-{station_id}")[:80]

        loc = station.get("location", {})
        lat = loc.get("latitude")
        lng = loc.get("longitude")
        google_maps = f"https://www.google.com/maps?q={lat},{lng}" if lat and lng else None

        rows.append({
            "id": row_id,
            "station_name": station.get("name", "Unknown"),
            "brand": station.get("brandId", "Unknown"),
            "suburb": suburb_name,
            "postcode": postcode,
            "address": address,
            "ulp91":     price_cols.get("ulp91"),
            "e10":       price_cols.get("e10"),
            "premium95": price_cols.get("premium95"),
            "premium98": price_cols.get("premium98"),
            "diesel":    price_cols.get("diesel"),
            "lpg":       price_cols.get("lpg"),
            "cheapest_fuel": cheapest_type,
            "google_maps_url": google_maps,
            "source": "Fair Fuel Open Data (Victorian Government)",
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

    total_fuel = 0
    all_errors = []

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
    status = "success" if not all_errors else f"errors: {all_errors}"
    print(f"  \u2713 Run complete: {status}")



if __name__ == "__main__":
    main()
