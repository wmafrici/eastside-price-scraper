"""
scrape_competitors.py
=====================
On-demand competitor scraper for Eastside Price Tracker.

Run this script when onboarding a new trial client to populate
the businesses table with real local competitors.

Usage:
    python scripts/scrape_competitors.py --suburb "Ringwood" --category "barbers"
    python scripts/scrape_competitors.py --suburb "Forest Hill" --category "cafes"

Categories: barbers, gyms, salons, cafes, dentists

Environment variables:
    SUPABASE_URL        - your Supabase project URL
    SUPABASE_KEY        - service role key
    GOOGLE_PLACES_KEY   - Google Places API key (get free at console.cloud.google.com)

How it works:
    1. Calls Google Places API to find real local businesses in the suburb
    2. For each business found, extracts name, address, rating, and price level
    3. Maps Google's price level (1-4) to a realistic AUD price for the category
    4. Upserts all results into the Supabase businesses table
    5. Prints a summary of what was found and saved

Getting a Google Places API key (free):
    1. Go to console.cloud.google.com
    2. Create a new project
    3. Enable "Places API"
    4. Create credentials -> API key
    5. Add as GitHub secret GOOGLE_PLACES_KEY
    6. Free tier gives $200/month credit (~5,000 searches/month)
"""

import os
import re
import sys
import time
import argparse
import datetime
import requests
from typing import Optional

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
GOOGLE_PLACES_KEY = os.environ.get("GOOGLE_PLACES_KEY", "")

TODAY = datetime.date.today().isoformat()

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

SUBURB_LOOKUP = {s["name"].lower(): s for s in SUBURBS}

# Google Places search terms per category
CATEGORY_SEARCH = {
    "barbers":  "barber shop",
    "gyms":     "gym fitness",
    "salons":   "hair salon",
    "cafes":    "cafe coffee",
    "dentists": "dentist",
}

# Service type label per category
CATEGORY_SERVICE = {
    "barbers":  "Men's haircut",
    "gyms":     "Monthly membership",
    "salons":   "Women's haircut",
    "cafes":    "Flat white",
    "dentists": "General check-up",
}

# Realistic Melbourne Eastern suburbs price ranges per category (AUD)
# Based on Google price_level: 1=budget, 2=mid, 3=upscale, 4=premium, None=mid
PRICE_MAP = {
    "barbers":  {1: 20.0, 2: 30.0, 3: 45.0, 4: 60.0, None: 30.0},
    "gyms":     {1: 30.0, 2: 55.0, 3: 80.0, 4: 120.0, None: 55.0},
    "salons":   {1: 40.0, 2: 75.0, 3: 110.0, 4: 160.0, None: 75.0},
    "cafes":    {1: 4.50, 2: 5.50, 3: 6.50, 4: 8.00, None: 5.50},
    "dentists": {1: 180.0, 2: 250.0, 3: 350.0, 4: 500.0, None: 250.0},
}

# ---------------------------------------------------------------------------
# Supabase helpers
# ---------------------------------------------------------------------------

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates",
}


def supabase_upsert(table: str, rows: list[dict]) -> int:
    if not rows:
        return 0
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    resp = requests.post(url, headers=HEADERS, json=rows, timeout=30)
    resp.raise_for_status()
    return len(rows)


# ---------------------------------------------------------------------------
# Google Places API
# ---------------------------------------------------------------------------

PLACES_BASE = "https://maps.googleapis.com/maps/api/place"


def search_places(suburb: str, category: str, api_key: str) -> list[dict]:
    """
    Search Google Places for businesses in a suburb+category.
    Returns up to 20 results.
    """
    search_term = CATEGORY_SEARCH[category]
    query = f"{search_term} in {suburb} Melbourne VIC Australia"

    url = f"{PLACES_BASE}/textsearch/json"
    params = {
        "query": query,
        "key": api_key,
        "region": "au",
        "language": "en",
    }

    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        status = data.get("status")
        if status != "OK":
            print(f"  Google Places API status: {status}")
            return []
        return data.get("results", [])
    except Exception as e:
        print(f"  Google Places API error: {e}")
        return []


def parse_place(place: dict, suburb_info: dict, category: str) -> Optional[dict]:
    """Convert a Google Places result to a businesses table row."""
    name = place.get("name", "")
    address = place.get("formatted_address", "")
    rating = place.get("rating")
    price_level = place.get("price_level")  # 1-4 or None
    place_id = place.get("place_id", "")

    # Only include places actually in our suburb (filter by address)
    suburb_name = suburb_info["name"]
    postcode = suburb_info["postcode"]
    if suburb_name.lower() not in address.lower() and postcode not in address:
        return None

    # Map price level to estimated AUD price
    price = PRICE_MAP[category].get(price_level, PRICE_MAP[category][None])

    # Build a stable ID
    row_id = re.sub(r"[^a-zA-Z0-9\-]", "-", f"gp-{place_id}")[:80]

    google_maps = f"https://www.google.com/maps/place/?q=place_id:{place_id}"

    return {
        "id": row_id,
        "business_name": name,
        "suburb": suburb_name,
        "postcode": postcode,
        "category": category,
        "service_type": CATEGORY_SERVICE[category],
        "price": price,
        "currency": "AUD",
        "rating": rating,
        "source": "Google Places API",
        "verified": False,
        "verification_level": "70%",
        "date_scrapped": TODAY,
        "last_updated": datetime.datetime.utcnow().isoformat(),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def scrape(suburb_name: str, category: str):
    suburb_name = suburb_name.strip()
    category = category.strip().lower()

    # Validate inputs
    suburb_info = SUBURB_LOOKUP.get(suburb_name.lower())
    if not suburb_info:
        print(f"ERROR: Unknown suburb '{suburb_name}'. Valid suburbs: {[s['name'] for s in SUBURBS]}")
        sys.exit(1)
    if category not in CATEGORY_SEARCH:
        print(f"ERROR: Unknown category '{category}'. Valid: {list(CATEGORY_SEARCH.keys())}")
        sys.exit(1)
    if not GOOGLE_PLACES_KEY:
        print("ERROR: GOOGLE_PLACES_KEY environment variable not set.")
        print("Get a free key at console.cloud.google.com -> Enable Places API -> Create credentials")
        sys.exit(1)

    print(f"\n=== Competitor Scraper: {suburb_name} / {category} ===")
    print(f"  Searching Google Places...")

    places = search_places(suburb_name, category, GOOGLE_PLACES_KEY)
    print(f"  Found {len(places)} places from Google")

    rows = []
    for place in places:
        row = parse_place(place, suburb_info, category)
        if row:
            rows.append(row)
        time.sleep(0.1)

    print(f"  Filtered to {len(rows)} businesses in {suburb_name}")

    if rows:
        upserted = supabase_upsert("businesses", rows)
        print(f"  Upserted {upserted} rows to Supabase businesses table")
        print("\n  Businesses saved:")
        for r in rows:
            print(f"    - {r['business_name']} | rating: {r['rating']} | est. price: ${r['price']}")
    else:
        print(f"  No businesses found in {suburb_name} for category {category}")

    print(f"\n=== Done ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape local competitors for a suburb + category")
    parser.add_argument("--suburb", required=True, help="Suburb name e.g. 'Ringwood'")
    parser.add_argument("--category", required=True, help="Category: barbers, gyms, salons, cafes, dentists")
    args = parser.parse_args()
    scrape(args.suburb, args.category)
