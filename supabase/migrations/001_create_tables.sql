-- ============================================================
-- Eastside Price Tracker: Supabase Schema
-- Run this once in the Supabase SQL editor to set up tables
-- ============================================================

-- ============================================================
-- TABLE: businesses
-- Stores barbers, gyms, salons, cafes, dentists
-- ============================================================
CREATE TABLE IF NOT EXISTS businesses (
  id TEXT PRIMARY KEY,
  business_name TEXT NOT NULL,
  suburb TEXT NOT NULL,
  postcode TEXT NOT NULL,
  category TEXT NOT NULL CHECK (category IN ('barbers','gyms','salons','cafes','dentists')),
  service_type TEXT,
  price NUMERIC(8,2),
  currency TEXT DEFAULT 'AUD',
  price_range TEXT,
  rating NUMERIC(3,1),
  rating_count INTEGER,
  notes TEXT,
  phone TEXT,
  website TEXT,
  google_maps_url TEXT,
  address TEXT,
  source TEXT,
  source_url TEXT,
  verified BOOLEAN DEFAULT FALSE,
  verification_level TEXT,
  date_scrapped DATE,
  last_updated TIMESTAMPTZ DEFAULT NOW(),
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Index for common query patterns
CREATE INDEX IF NOT EXISTS idx_businesses_suburb ON businesses(suburb);
CREATE INDEX IF NOT EXISTS idx_businesses_category ON businesses(category);
CREATE INDEX IF NOT EXISTS idx_businesses_suburb_category ON businesses(suburb, category);

-- Enable Row Level Security (read-only public access)
ALTER TABLE businesses ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Public read access on businesses"
  ON businesses FOR SELECT
  USING (true);

-- ============================================================
-- TABLE: fuel_stations
-- Stores petrol station fuel prices
-- ============================================================
CREATE TABLE IF NOT EXISTS fuel_stations (
  id TEXT PRIMARY KEY,
  station_name TEXT NOT NULL,
  brand TEXT,
  suburb TEXT NOT NULL,
  postcode TEXT NOT NULL,
  address TEXT,
  phone TEXT,
  opening_hours TEXT,
  ulp91 NUMERIC(6,1),
  e10 NUMERIC(6,1),
  premium95 NUMERIC(6,1),
  premium98 NUMERIC(6,1),
  diesel NUMERIC(6,1),
  lpg NUMERIC(6,1),
  cheapest_fuel TEXT,
  google_maps_url TEXT,
  source TEXT,
  source_url TEXT,
  verified BOOLEAN DEFAULT FALSE,
  date_scrapped DATE,
  last_updated TIMESTAMPTZ DEFAULT NOW(),
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Index for common query patterns
CREATE INDEX IF NOT EXISTS idx_fuel_suburb ON fuel_stations(suburb);
CREATE INDEX IF NOT EXISTS idx_fuel_brand ON fuel_stations(brand);
CREATE INDEX IF NOT EXISTS idx_fuel_ulp91 ON fuel_stations(ulp91);

-- Enable Row Level Security (read-only public access)
ALTER TABLE fuel_stations ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Public read access on fuel_stations"
  ON fuel_stations FOR SELECT
  USING (true);

-- ============================================================
-- TABLE: suburb_fuel_summary
-- Pre-aggregated suburb-level cheapest/avg prices
-- ============================================================
CREATE TABLE IF NOT EXISTS suburb_fuel_summary (
  suburb TEXT PRIMARY KEY,
  postcode TEXT NOT NULL,
  station_count INTEGER DEFAULT 0,
  cheapest_ulp91 NUMERIC(6,1),
  cheapest_ulp91_station TEXT,
  cheapest_e10 NUMERIC(6,1),
  cheapest_diesel NUMERIC(6,1),
  avg_ulp91 NUMERIC(6,2),
  last_updated TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE suburb_fuel_summary ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Public read access on suburb_fuel_summary"
  ON suburb_fuel_summary FOR SELECT
  USING (true);

-- ============================================================
-- TABLE: scrape_log
-- Tracks every scraper run for debugging
-- ============================================================
CREATE TABLE IF NOT EXISTS scrape_log (
  id SERIAL PRIMARY KEY,
  run_at TIMESTAMPTZ DEFAULT NOW(),
  businesses_updated INTEGER DEFAULT 0,
  fuel_stations_updated INTEGER DEFAULT 0,
  errors TEXT,
  status TEXT CHECK (status IN ('success','partial','failed'))
);
