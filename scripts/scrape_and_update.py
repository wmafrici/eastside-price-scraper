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

# PetrolSpy fuel type codes -> our column names
PETROLSPY_TO_COL = {
    "ULP":  "ulp91",
    "E10":  "e10",
    "PULP": "premium95",
    "P98":  "premium98",
    "DL":   "diesel",
    "LPG":  "lpg",
}

PETROLSPY_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; EastsidePriceScraper/2.0)",
    "Accept": "application/json",
    "Referer": "https://petrolspy.com.au/",
}


def fetch_petrolspy(session: requests.Session, postcode: str) -> list[dict]:
    """
    Fetch all stations near a postcode from PetrolSpy's public JSON feed.
    Endpoint: GET https://petrolspy.com.au/webservice-1/station/list?postcode=<postcode>
    Response: {"success": true, "message": {"list": [...]}}
    """
    url = "https://petrolspy.com.au/webservice-1/station/list"
    try:
        resp = session.get(url, params={"postcode": postcode},
                           headers=PETROLSPY_HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return data.get("message", {}).get("list", [])
    except Exception as e:
        print(f"    PetrolSpy error for {postcode}: {e}")
        return []


def parse_petrolspy_stations(raw_stations: list[dict], suburb: dict) -> list[dict]:
    """Convert PetrolSpy raw station dicts into our fuel_stations schema rows."""
    rows = []
    for st in raw_stations:
        price_cols: dict[str, Optional[float]] = {}
        cheapest_price = None
        cheapest_type = None

        for p in st.get("prices", []):
            fuel_type = p.get("type", "")
            col = PETROLSPY_TO_COL.get(fuel_type)
            if col is None:
                continue
            try:
                price_val = float(p["price"])
            except (KeyError, ValueError, TypeError):
                continue
            price_cols[col] = price_val
            if col == "ulp91":
                cheapest_price = price_val
                cheapest_type = "ULP91"
            elif cheapest_price is None:
                cheapest_price = price_val
                cheapest_type = fuel_type

        raw_id = f"ps-{st.get('id', suburb['postcode'] + '-' + st.get('name', 'unknown'))}"
        row_id = re.sub(r"[^a-zA-Z0-9\-]", "-", raw_id)[:80]

        google_maps = None
        lat = st.get("lat")
        lng = st.get("lng")
        if lat and lng:
            google_maps = f"https://www.google.com/maps?q={lat},{lng}"

        rows.append({
            "id": row_id,
            "station_name": st.get("name", "Unknown"),
            "brand": st.get("brand", "Unknown"),
            "suburb": suburb["name"],
            "postcode": suburb["postcode"],
            "address": st.get("address", ""),
            "ulp91":     price_cols.get("ulp91"),
            "e10":       price_cols.get("e10"),
            "premium95": price_cols.get("premium95"),
            "premium98": price_cols.get("premium98"),
            "diesel":    price_cols.get("diesel"),
            "lpg":       price_cols.get("lpg"),
            "cheapest_fuel": cheapest_type,
            "google_maps_url": google_maps,
            "source": "PetrolSpy",
            "source_url": f"https://petrolspy.com.au/map/location/au/vic/{suburb['postcode']}",
            "verified": True,
            "date_scrapped": TODAY,
            "last_updated": datetime.datetime.utcnow().isoformat(),
        })
    return rows


def scrape_fuel(session: requests.Session) -> tuple[list[dict], list[dict]]:
    """
    Scrape current fuel prices via PetrolSpy's public JSON endpoint.
    Returns (station_rows, summary_rows).
    """
    station_rows = []
    summary_by_suburb: dict[str, dict] = {}

    for suburb in SUBURBS:
        print(f"    Fetching {suburb['name']} ({suburb['postcode']})...")
        raw = fetch_petrolspy(session, suburb["postcode"])
        rows = parse_petrolspy_stations(raw, suburb)
        station_rows.extend(rows)

        ulp_prices = [r["ulp91"]   for r in rows if r.get("ulp91")]
        e10_prices  = [r["e10"]    for r in rows if r.get("e10")]
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

        time.sleep(0.5)

    print(f"  Fuel stations scraped: {len(station_rows)}")
    return station_rows, list(summary_by_suburb.values())


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
