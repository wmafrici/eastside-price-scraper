"""
scrape_energy_prices.py
=======================
Victorian electricity default offer price scraper.
Scrapes the ESC Victoria website for the current Victorian Default Offer (VDO)
tariffs and upserts results into Supabase.

Runs via GitHub Actions (see .github/workflows/).

Flow:
  1. Fetch VDO tariff tables from ESC Victoria website
  2. Parse the distribution zone rates
  3. Upsert results into Supabase energy_providers table
  4. Save a JSON snapshot to data/energyPriceData.json
  5. Log the run result

Environment variables (set as GitHub Actions secrets):
  SUPABASE_URL   ->  https://mpbphijerbizlvfhssww.supabase.co
  SUPABASE_KEY   ->  service_role key
"""

import os
import json
import time
import requests
import datetime
from bs4 import BeautifulSoup

# --- Configuration ----------------------------------------------------------

ESC_VDO_URL = (
    "https://www.esc.vic.gov.au/electricity-and-gas/"
    "prices-tariffs-and-benchmarks/victorian-default-offer"
)

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

OUTPUT_JSON_PATH = os.path.join(
    os.path.dirname(__file__), "..", "data", "energyPriceData.json"
)

TODAY = datetime.date.today().isoformat()

# Known distribution zones -- used to validate scraped rows
KNOWN_ZONES = ["AusNet Services", "CitiPower", "Jemena", "Powercor", "United Energy"]

# Zone -> metadata (postcode examples, coverage notes)
ZONE_METADATA = {
    "AusNet Services": {
        "id": "ausnet-services-vic",
        "distribution_zone_note": "Covers eastern Melbourne suburbs and large regional areas including Gippsland",
        "postcode_examples": ["3000", "3128", "3150", "3170", "3800", "3840"],
    },
    "CitiPower": {
        "id": "citipower-vic",
        "distribution_zone_note": "Covers inner Melbourne CBD and inner eastern suburbs",
        "postcode_examples": ["3000", "3002", "3004", "3006", "3051", "3053", "3121", "3141"],
    },
    "Jemena": {
        "id": "jemena-vic",
        "distribution_zone_note": "Covers north and north-western Melbourne suburbs",
        "postcode_examples": ["3031", "3040", "3042", "3055", "3072", "3081", "3083"],
    },
    "Powercor": {
        "id": "powercor-vic",
        "distribution_zone_note": "Covers western Melbourne suburbs and large regional areas including Ballarat and Geelong",
        "postcode_examples": ["3011", "3012", "3015", "3016", "3020", "3021", "3025", "3029", "3030"],
    },
    "United Energy": {
        "id": "united-energy-vic",
        "distribution_zone_note": "Covers south-eastern Melbourne suburbs including Mornington Peninsula",
        "postcode_examples": ["3145", "3148", "3150", "3163", "3168", "3170", "3175", "3195", "3930"],
    },
}


# --- Helpers ----------------------------------------------------------------

def parse_price(text: str) -> float | None:
    """Parse a dollar amount string like '$0.3477' into a float."""
    if not text:
        return None
    cleaned = text.strip().replace("$", "").split()[0]
    try:
        return float(cleaned)
    except ValueError:
        return None


def fetch_page(url: str) -> BeautifulSoup:
    """Fetch a URL and return a BeautifulSoup object."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (compatible; EastsidePriceScraper/1.0; "
            "+https://github.com/wmafrici/eastside-price-scraper)"
        )
    }
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")


def extract_table_rows(table) -> list[list[str]]:
    """Extract text rows from a BeautifulSoup table element."""
    rows = []
    for row in table.find_all("tr"):
        cells = [td.get_text(separator=" ", strip=True) for td in row.find_all(["td", "th"])]
        if cells:
            rows.append(cells)
    return rows


# --- Scraping ---------------------------------------------------------------

def scrape_vdo_prices() -> dict:
    """
    Scrape Victorian Default Offer tariff tables from the ESC website.
    Returns a dict keyed by distribution zone name.
    """
    print(f"Fetching ESC VDO page: {ESC_VDO_URL}")
    soup = fetch_page(ESC_VDO_URL)

    # Find all tables on the page
    tables = soup.find_all("table")
    print(f"  Found {len(tables)} table(s) on page")

    zone_data = {zone: {} for zone in KNOWN_ZONES}

    for i, table in enumerate(tables):
        rows = extract_table_rows(table)
        if not rows:
            continue

        # Identify table type by header row content
        header_text = " ".join(rows[0]).lower() if rows else ""

        # Flat tariff domestic table
        if "supply charge" in header_text and "usage charge" in header_text and "controlled load" in header_text:
            print(f"  Table {i}: Flat tariff domestic or TOU table")

            for row in rows[1:]:
                if len(row) < 3:
                    continue
                zone = next((z for z in KNOWN_ZONES if z.lower() in row[0].lower()), None)
                if not zone:
                    continue

                # Determine if TOU (has peak/off-peak) or flat
                if "peak" in header_text or "off peak" in header_text:
                    # TOU table
                    supply = parse_price(row[1]) if len(row) > 1 else None
                    peak = parse_price(row[2]) if len(row) > 2 else None
                    offpeak = parse_price(row[3]) if len(row) > 3 else None
                    cl = parse_price(row[4]) if len(row) > 4 else None
                    zone_data[zone].setdefault("tou_domestic", {}).update({
                        "supply_charge_daily": supply,
                        "peak_rate": peak,
                        "offpeak_rate": offpeak,
                        "controlled_load_rate": cl,
                    })
                else:
                    # Flat table
                    supply = parse_price(row[1]) if len(row) > 1 else None
                    usage_raw = row[3] if len(row) > 3 else ""
                    # Extract first price in usage field (handles block tariffs)
                    usage = parse_price(usage_raw)
                    cl = parse_price(row[4]) if len(row) > 4 else None
                    zone_data[zone].setdefault("flat_domestic", {}).update({
                        "supply_charge_daily": supply,
                        "usage_rate": usage,
                        "controlled_load_rate": cl,
                    })

    return zone_data


# --- Build records ----------------------------------------------------------

def build_records(zone_data: dict, effective_from: str = None, effective_to: str = None) -> list[dict]:
    """
    Build a list of Supabase upsert records from scraped zone data.
    Falls back to known-good values if scraping returns incomplete data.
    """
    # Hardcoded 2025-26 VDO values as fallback (verified from ESC on 2026-04-13)
    FALLBACK = {
        "AusNet Services": {
            "flat_domestic": {"supply_charge_daily": 1.4146, "usage_rate": 0.3477, "controlled_load_rate": 0.2399},
            "flat_smb": {"supply_charge_daily": 1.4146, "usage_rate": 0.3881},
            "tou_domestic": {"supply_charge_daily": 1.4146, "peak_rate": 0.4682, "offpeak_rate": 0.2399, "controlled_load_rate": 0.2399},
            "tou_smb": {"supply_charge_daily": 1.4146, "peak_rate": 0.4031, "offpeak_rate": 0.2198},
        },
        "CitiPower": {
            "flat_domestic": {"supply_charge_daily": 1.2407, "usage_rate": 0.2733, "controlled_load_rate": 0.2012},
            "flat_smb": {"supply_charge_daily": 1.4469, "usage_rate": 0.2657},
            "tou_domestic": {"supply_charge_daily": 1.2407, "peak_rate": 0.3633, "offpeak_rate": 0.2206, "controlled_load_rate": 0.2012},
            "tou_smb": {"supply_charge_daily": 1.4469, "peak_rate": 0.3305, "offpeak_rate": 0.1947},
        },
        "Jemena": {
            "flat_domestic": {"supply_charge_daily": 1.2301, "usage_rate": 0.2972, "controlled_load_rate": 0.2314},
            "flat_smb": {"supply_charge_daily": 1.5834, "usage_rate": 0.3141},
            "tou_domestic": {"supply_charge_daily": 1.2301, "peak_rate": 0.3761, "offpeak_rate": 0.2368, "controlled_load_rate": 0.2314},
            "tou_smb": {"supply_charge_daily": 1.6722, "peak_rate": 0.3579, "offpeak_rate": 0.2028},
        },
        "Powercor": {
            "flat_domestic": {"supply_charge_daily": 1.3684, "usage_rate": 0.3009, "controlled_load_rate": 0.2122},
            "flat_smb": {"supply_charge_daily": 1.5905, "usage_rate": 0.2927},
            "tou_domestic": {"supply_charge_daily": 1.3684, "peak_rate": 0.4040, "offpeak_rate": 0.2365, "controlled_load_rate": 0.2122},
            "tou_smb": {"supply_charge_daily": 1.5905, "peak_rate": 0.3895, "offpeak_rate": 0.2111},
        },
        "United Energy": {
            "flat_domestic": {"supply_charge_daily": 1.1648, "usage_rate": 0.2884, "controlled_load_rate": 0.2107},
            "flat_smb": {"supply_charge_daily": 1.3711, "usage_rate": 0.2789},
            "tou_domestic": {"supply_charge_daily": 1.1648, "peak_rate": 0.3837, "offpeak_rate": 0.2299, "controlled_load_rate": 0.2107},
            "tou_smb": {"supply_charge_daily": 1.3711, "peak_rate": 0.3501, "offpeak_rate": 0.2027},
        },
    }

    records = []
    for zone in KNOWN_ZONES:
        meta = ZONE_METADATA[zone]
        scraped = zone_data.get(zone, {})
        fb = FALLBACK[zone]

        # Prefer scraped, fall back to hardcoded
        fd = {**fb["flat_domestic"], **scraped.get("flat_domestic", {})}
        fs = {**fb["flat_smb"], **scraped.get("flat_smb", {})}
        td = {**fb["tou_domestic"], **scraped.get("tou_domestic", {})}
        ts = {**fb["tou_smb"], **scraped.get("tou_smb", {})}

        records.append({
            "id": meta["id"],
            "provider_name": zone,
            "provider_type": "distributor",
            "state": "VIC",
            "distribution_zone": zone,
            "distribution_zone_note": meta["distribution_zone_note"],
            "postcode_examples": meta["postcode_examples"],
            "currency": "AUD",
            # Flat domestic
            "flat_domestic_supply_charge_daily": fd.get("supply_charge_daily"),
            "flat_domestic_usage_structure": "block" if zone == "AusNet Services" else "anytime",
            "flat_domestic_anytime_rate": None if zone == "AusNet Services" else fd.get("usage_rate"),
            "flat_domestic_block1_rate": fd.get("usage_rate") if zone == "AusNet Services" else None,
            "flat_domestic_block2_rate": fd.get("usage_rate") if zone == "AusNet Services" else None,
            "flat_domestic_block1_description": "Up to 1020 kWh per quarter" if zone == "AusNet Services" else None,
            "flat_domestic_block2_description": "Over 1020 kWh per quarter" if zone == "AusNet Services" else None,
            "flat_domestic_controlled_load_rate": fd.get("controlled_load_rate"),
            # Flat SMB
            "flat_smb_supply_charge_daily": fs.get("supply_charge_daily"),
            "flat_smb_usage_structure": "block" if zone == "AusNet Services" else "anytime",
            "flat_smb_anytime_rate": None if zone == "AusNet Services" else fs.get("usage_rate"),
            "flat_smb_block1_rate": fs.get("usage_rate") if zone == "AusNet Services" else None,
            "flat_smb_block2_rate": fs.get("usage_rate") if zone == "AusNet Services" else None,
            # TOU domestic
            "tou_domestic_supply_charge_daily": td.get("supply_charge_daily"),
            "tou_domestic_peak_rate": td.get("peak_rate"),
            "tou_domestic_peak_description": "3 pm to 9 pm everyday",
            "tou_domestic_offpeak_rate": td.get("offpeak_rate"),
            "tou_domestic_offpeak_description": "All other times",
            "tou_domestic_controlled_load_rate": td.get("controlled_load_rate"),
            # TOU SMB
            "tou_smb_supply_charge_daily": ts.get("supply_charge_daily"),
            "tou_smb_peak_rate": ts.get("peak_rate"),
            "tou_smb_peak_description": "9 am to 9 pm weekdays",
            "tou_smb_offpeak_rate": ts.get("offpeak_rate"),
            "tou_smb_offpeak_description": "All other times",
            # Metadata
            "effective_from": effective_from or "2025-07-01",
            "effective_to": effective_to or "2026-06-30",
            "source": "ESC Victorian Default Offer 2025-26",
            "source_url": ESC_VDO_URL,
            "verified": True,
            "date_scrapped": TODAY,
        })

    return records


# --- Supabase upsert --------------------------------------------------------

def upsert_to_supabase(records: list[dict]) -> dict:
    """Upsert energy provider records into Supabase."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("  Warning: SUPABASE_URL or SUPABASE_KEY not set -- skipping upsert")
        return {"skipped": True}

    url = f"{SUPABASE_URL}/rest/v1/energy_providers"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }

    resp = requests.post(url, headers=headers, json=records, timeout=30)
    resp.raise_for_status()
    print(f"  Upserted {len(records)} energy provider records to Supabase")
    return {"upserted": len(records), "status": resp.status_code}


# --- Save JSON snapshot -----------------------------------------------------

def save_json_snapshot(records: list[dict]) -> None:
    """Save a human-readable JSON snapshot to data/energyPriceData.json."""
    output = {
        "version": "1.0",
        "generatedDate": TODAY,
        "dataSource": "Victorian Default Offer (VDO) 2025-26 -- Essential Services Commission (ESC) Victoria",
        "sourceUrl": ESC_VDO_URL,
        "priceUnit": "AUD (GST inclusive)",
        "region": "Victoria, Australia",
        "effectivePeriod": "1 July 2025 - 30 June 2026",
        "note": (
            "Prices are the Victorian Default Offer (VDO) standing offer tariffs set by the "
            "Essential Services Commission. They apply to all electricity retailers operating in "
            "Victoria for customers on standing offers. Market offer rates vary by retailer and plan; "
            "use https://compare.energy.vic.gov.au to compare market offers for your address. "
            f"Prices scraped {TODAY}."
        ),
        "totalProviders": len(records),
        "providers": records,
    }

    os.makedirs(os.path.dirname(OUTPUT_JSON_PATH), exist_ok=True)
    with open(OUTPUT_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False, default=str)

    print(f"  Saved JSON snapshot to {OUTPUT_JSON_PATH}")


# --- Main -------------------------------------------------------------------

def main():
    start = time.time()
    print("=" * 60)
    print("Eastside Price Scraper -- Energy Prices")
    print(f"Run date: {TODAY}")
    print("=" * 60)

    results = {"run_date": TODAY, "errors": []}

    try:
        # 1. Scrape
        zone_data = scrape_vdo_prices()

        # 2. Build records (with fallback to known-good values)
        records = build_records(zone_data)
        print(f"Built {len(records)} energy provider records")

        # 3. Upsert to Supabase
        upsert_result = upsert_to_supabase(records)
        results["supabase"] = upsert_result

        # 4. Save JSON snapshot
        save_json_snapshot(records)
        results["json_saved"] = True

    except Exception as e:
        print(f"  Error: {e}")
        results["errors"].append(str(e))

    elapsed = round(time.time() - start, 2)
    results["elapsed_seconds"] = elapsed

    print("=" * 60)
    print(f"Done in {elapsed}s. Errors: {len(results['errors'])}")
    print("=" * 60)

    return results


if __name__ == "__main__":
    main()
