"""
seed_supabase.py
================
One-time script to load the scraped seedData.json and fuelPriceData.json
into Supabase.

Also runs automatically on first GitHub Actions deploy (see workflow).

Usage (local):
  SUPABASE_URL=https://mpbphijerbizlvfhssww.supabase.co \
  SUPABASE_KEY=<service_role_key> \
  python scripts/seed_supabase.py
"""

import os
import json
import datetime
import requests

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
TODAY = datetime.date.today().isoformat()

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates",
}


def upsert(table: str, rows: list[dict], batch_size: int = 50) -> int:
    """Upsert rows in batches. Returns total count upserted."""
    total = 0
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        resp = requests.post(url, headers=HEADERS, json=batch, timeout=30)
        if not resp.ok:
            print(f"  ✗ Error upserting to {table}: {resp.status_code} {resp.text[:200]}")
            continue
        total += len(batch)
    return total


def load_businesses(path: str) -> list[dict]:
    """Map seedData.json businesses → Supabase businesses table rows."""
    with open(path) as f:
        data = json.load(f)

    rows = []
    for b in data.get("businesses", []):
        rows.append({
            "id": b.get("id"),
            "business_name": b.get("businessName"),
            "suburb": b.get("suburb"),
            "postcode": b.get("postcode"),
            "category": b.get("category"),
            "service_type": b.get("serviceType"),
            "price": b.get("price"),
            "currency": b.get("currency", "AUD"),
            "price_range": b.get("priceRange"),
            "rating": b.get("rating"),
            "rating_count": b.get("ratingCount"),
            "notes": b.get("notes"),
            "phone": b.get("phone"),
            "website": b.get("website"),
            "google_maps_url": b.get("googleMapsUrl"),
            "address": b.get("address"),
            "source": b.get("source"),
            "source_url": b.get("sourceUrl"),
            "verified": b.get("verified", False),
            "verification_level": b.get("verificationLevel"),
            "date_scrapped": b.get("dataScrapped") or TODAY,
            "last_updated": b.get("lastUpdated") or TODAY,
        })
    return rows


def load_fuel_stations(path: str) -> tuple[list[dict], list[dict]]:
    """Map fuelPriceData.json → fuel_stations + suburb_fuel_summary rows."""
    with open(path) as f:
        data = json.load(f)

    station_rows = []
    for s in data.get("stations", []):
        prices = s.get("prices", {})
        station_rows.append({
            "id": s.get("id"),
            "station_name": s.get("stationName"),
            "brand": s.get("brand"),
            "suburb": s.get("suburb"),
            "postcode": s.get("postcode"),
            "address": s.get("address"),
            "phone": s.get("phone"),
            "opening_hours": s.get("openingHours"),
            "ulp91": prices.get("ulp91"),
            "e10": prices.get("e10"),
            "premium95": prices.get("premium95"),
            "premium98": prices.get("premium98"),
            "diesel": prices.get("diesel"),
            "lpg": prices.get("lpg"),
            "cheapest_fuel": s.get("cheapestFuel"),
            "google_maps_url": s.get("googleMapsUrl"),
            "source": s.get("source"),
            "source_url": s.get("sourceUrl"),
            "verified": s.get("verified", False),
            "date_scrapped": s.get("dateScrapped") or TODAY,
            "last_updated": s.get("dateScrapped") or TODAY,
        })

    summary_rows = []
    for s in data.get("suburbSummary", []):
        summary_rows.append({
            "suburb": s.get("suburb"),
            "postcode": s.get("postcode"),
            "station_count": s.get("stationCount"),
            "cheapest_ulp91": s.get("cheapestULP91"),
            "cheapest_ulp91_station": s.get("cheapestULP91Station"),
            "cheapest_e10": s.get("cheapestE10"),
            "cheapest_diesel": s.get("cheapestDiesel"),
            "avg_ulp91": s.get("avgULP91"),
            "last_updated": TODAY,
        })

    return station_rows, summary_rows


def main():
    print("=== Seeding Supabase from local JSON files ===\n")

    # Locate JSON files (look in data/ or outputs/)
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    candidates_biz = [
        os.path.join(base, "data", "seedData.json"),
        os.path.join(base, "seedData.json"),
    ]
    candidates_fuel = [
        os.path.join(base, "data", "fuelPriceData.json"),
        os.path.join(base, "fuelPriceData.json"),
    ]

    biz_path = next((p for p in candidates_biz if os.path.exists(p)), None)
    fuel_path = next((p for p in candidates_fuel if os.path.exists(p)), None)

    # --- Businesses ---
    if biz_path:
        print(f"[1/2] Loading businesses from {biz_path}")
        rows = load_businesses(biz_path)
        count = upsert("businesses", rows)
        print(f"  ✓ Upserted {count}/{len(rows)} business rows\n")
    else:
        print("[1/2] seedData.json not found — skipping businesses\n"
              "      (place seedData.json in the data/ folder)\n")

    # --- Fuel ---
    if fuel_path:
        print(f"[2/2] Loading fuel prices from {fuel_path}")
        station_rows, summary_rows = load_fuel_stations(fuel_path)
        count = upsert("fuel_stations", station_rows)
        print(f"  ✓ Upserted {count}/{len(station_rows)} fuel station rows")
        count2 = upsert("suburb_fuel_summary", summary_rows)
        print(f"  ✓ Upserted {count2}/{len(summary_rows)} suburb summary rows\n")
    else:
        print("[2/2] fuelPriceData.json not found — skipping fuel\n"
              "      (place fuelPriceData.json in the data/ folder)\n")

    print("=== Seed complete ===")


if __name__ == "__main__":
    main()
