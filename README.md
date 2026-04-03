# Eastside Price Scraper

Automated price scraper for Eastern Suburbs Melbourne using GitHub Actions + Supabase.

## How It Works

- **Runs every Sunday at 6 AM UTC** (4 PM AEST) via GitHub Actions
- **Scrapes prices** for barbers, gyms, salons, cafes, dentists, and fuel across 8 suburbs
- **Upserts into Supabase** automatically — your Loveable app reads live prices

## Repo Structure

```
eastside-price-scraper/
├── .github/workflows/
│   └── weekly-update.yml     # GitHub Actions schedule
├── data/
│   ├── seedData.json          # Initial businesses data (62 businesses)
│   └── fuelPriceData.json     # Initial fuel prices (28 stations)
├── scripts/
│   ├── scrape_and_update.py   # Weekly scraper — runs on schedule
│   └── seed_supabase.py       # One-time seed from JSON files
└── supabase/migrations/
    └── 001_create_tables.sql  # Run once in Supabase SQL editor
```

## First-Time Setup

### 1. Create Supabase tables
Go to your [Supabase SQL editor](https://supabase.com/dashboard/project/mpbphijerbizlvfhssww/sql) and run the contents of `supabase/migrations/001_create_tables.sql`.

### 2. Add GitHub Secrets
Go to **GitHub repo → Settings → Secrets and variables → Actions → New repository secret** and add:

| Secret name    | Value |
|----------------|-------|
| `SUPABASE_URL` | `https://mpbphijerbizlvfhssww.supabase.co` |
| `SUPABASE_KEY` | *(your service_role key)* |

### 3. Seed initial data
Go to **GitHub repo → Actions → Weekly Price Update → Run workflow** and tick **"Run seed script only"** → Run.

This loads all 62 businesses and 28 fuel stations from the JSON files into Supabase.

### 4. Connect Loveable
In your Loveable project, connect to Supabase with your project URL and anon key. Query the `businesses`, `fuel_stations`, and `suburb_fuel_summary` tables.

## Tables

| Table | Description |
|-------|-------------|
| `businesses` | Barbers, gyms, salons, cafes, dentists (62 rows) |
| `fuel_stations` | Petrol station prices (28 stations) |
| `suburb_fuel_summary` | Cheapest/avg fuel by suburb |
| `scrape_log` | Run history and error tracking |

## Tech Stack

- **GitHub Actions** — free automation, runs weekly
- **Supabase** — free tier database (up to 500MB)
- **Python + requests** — lightweight scraper
- **Loveable** — frontend app

Total cost: **$0/month**
