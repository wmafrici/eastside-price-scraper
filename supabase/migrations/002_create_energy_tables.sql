-- ============================================================
-- Eastside Price Tracker: Energy Tables Migration
-- Run this in the Supabase SQL editor after 001_create_tables.sql
-- ============================================================

-- ============================================================
-- TABLE: energy_providers
-- Stores Victorian electricity distribution zones and their
-- Victorian Default Offer (VDO) tariff rates
-- ============================================================
CREATE TABLE IF NOT EXISTS energy_providers (
  id TEXT PRIMARY KEY,
  provider_name TEXT NOT NULL,
  provider_type TEXT NOT NULL CHECK (provider_type IN ('distributor', 'retailer')),
  state TEXT NOT NULL DEFAULT 'VIC',
  distribution_zone TEXT NOT NULL,
  distribution_zone_note TEXT,
  postcode_examples TEXT[], -- array of example postcodes in this zone
  currency TEXT DEFAULT 'AUD',

  -- Flat tariff (domestic)
  flat_domestic_supply_charge_daily NUMERIC(6,4),
  flat_domestic_usage_structure TEXT,   -- 'anytime' or 'block'
  flat_domestic_anytime_rate NUMERIC(6,4),
  flat_domestic_block1_rate NUMERIC(6,4),
  flat_domestic_block2_rate NUMERIC(6,4),
  flat_domestic_block1_description TEXT,
  flat_domestic_block2_description TEXT,
  flat_domestic_controlled_load_rate NUMERIC(6,4),

  -- Flat tariff (small business, <40 MWh/yr)
  flat_smb_supply_charge_daily NUMERIC(6,4),
  flat_smb_usage_structure TEXT,
  flat_smb_anytime_rate NUMERIC(6,4),
  flat_smb_block1_rate NUMERIC(6,4),
  flat_smb_block2_rate NUMERIC(6,4),

  -- Time-of-use tariff (domestic)
  tou_domestic_supply_charge_daily NUMERIC(6,4),
  tou_domestic_peak_rate NUMERIC(6,4),
  tou_domestic_peak_description TEXT,
  tou_domestic_offpeak_rate NUMERIC(6,4),
  tou_domestic_offpeak_description TEXT,
  tou_domestic_controlled_load_rate NUMERIC(6,4),

  -- Time-of-use tariff (small business, <40 MWh/yr)
  tou_smb_supply_charge_daily NUMERIC(6,4),
  tou_smb_peak_rate NUMERIC(6,4),
  tou_smb_peak_description TEXT,
  tou_smb_offpeak_rate NUMERIC(6,4),
  tou_smb_offpeak_description TEXT,

  -- Metadata
  effective_from DATE,
  effective_to DATE,
  source TEXT,
  source_url TEXT,
  verified BOOLEAN DEFAULT FALSE,
  date_scrapped DATE,
  last_updated TIMESTAMPTZ DEFAULT NOW(),
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_energy_providers_state ON energy_providers(state);
CREATE INDEX IF NOT EXISTS idx_energy_providers_zone ON energy_providers(distribution_zone);

-- Enable Row Level Security (read-only public access)
ALTER TABLE energy_providers ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Public read access on energy_providers"
  ON energy_providers FOR SELECT
  USING (true);

-- ============================================================
-- SEED: Insert Victorian Default Offer 2025-26 data
-- Source: ESC Victoria -- https://www.esc.vic.gov.au/electricity-and-gas/prices-tariffs-and-benchmarks/victorian-default-offer
-- Effective: 1 July 2025 - 30 June 2026
-- All prices include GST
-- ============================================================

INSERT INTO energy_providers (
  id,
  provider_name,
  provider_type,
  state,
  distribution_zone,
  distribution_zone_note,
  postcode_examples,
  flat_domestic_supply_charge_daily,
  flat_domestic_usage_structure,
  flat_domestic_anytime_rate,
  flat_domestic_block1_rate,
  flat_domestic_block2_rate,
  flat_domestic_block1_description,
  flat_domestic_block2_description,
  flat_domestic_controlled_load_rate,
  flat_smb_supply_charge_daily,
  flat_smb_usage_structure,
  flat_smb_anytime_rate,
  flat_smb_block1_rate,
  flat_smb_block2_rate,
  tou_domestic_supply_charge_daily,
  tou_domestic_peak_rate,
  tou_domestic_peak_description,
  tou_domestic_offpeak_rate,
  tou_domestic_offpeak_description,
  tou_domestic_controlled_load_rate,
  tou_smb_supply_charge_daily,
  tou_smb_peak_rate,
  tou_smb_peak_description,
  tou_smb_offpeak_rate,
  tou_smb_offpeak_description,
  effective_from,
  effective_to,
  source,
  source_url,
  verified,
  date_scrapped
) VALUES
  -- AusNet Services
  (
    'ausnet-services-vic',
    'AusNet Services',
    'distributor',
    'VIC',
    'AusNet Services',
    'Covers eastern Melbourne suburbs and large regional areas including Gippsland',
    ARRAY['3000','3128','3150','3170','3800','3840'],
    1.4146, 'block', NULL, 0.3477, 0.3477,
    'Up to 1020 kWh per quarter', 'Over 1020 kWh per quarter', 0.2399,
    1.4146, 'block', NULL, 0.3881, 0.3881,
    1.4146, 0.4682, '3 pm to 9 pm everyday', 0.2399, 'All other times', 0.2399,
    1.4146, 0.4031, '9 am to 9 pm weekdays', 0.2198, 'All other times',
    '2025-07-01', '2026-06-30',
    'ESC Victorian Default Offer 2025-26',
    'https://www.esc.vic.gov.au/electricity-and-gas/prices-tariffs-and-benchmarks/victorian-default-offer',
    TRUE, '2026-04-13'
  ),
  -- CitiPower
  (
    'citipower-vic',
    'CitiPower',
    'distributor',
    'VIC',
    'CitiPower',
    'Covers inner Melbourne CBD and inner eastern suburbs',
    ARRAY['3000','3002','3004','3006','3051','3053','3121','3141'],
    1.2407, 'anytime', 0.2733, NULL, NULL, NULL, NULL, 0.2012,
    1.4469, 'anytime', 0.2657, NULL, NULL,
    1.2407, 0.3633, '3 pm to 9 pm everyday', 0.2206, 'All other times', 0.2012,
    1.4469, 0.3305, '9 am to 9 pm weekdays', 0.1947, 'All other times',
    '2025-07-01', '2026-06-30',
    'ESC Victorian Default Offer 2025-26',
    'https://www.esc.vic.gov.au/electricity-and-gas/prices-tariffs-and-benchmarks/victorian-default-offer',
    TRUE, '2026-04-13'
  ),
  -- Jemena
  (
    'jemena-vic',
    'Jemena',
    'distributor',
    'VIC',
    'Jemena',
    'Covers north and north-western Melbourne suburbs',
    ARRAY['3031','3040','3042','3055','3072','3081','3083'],
    1.2301, 'anytime', 0.2972, NULL, NULL, NULL, NULL, 0.2314,
    1.5834, 'anytime', 0.3141, NULL, NULL,
    1.2301, 0.3761, '3 pm to 9 pm everyday', 0.2368, 'All other times', 0.2314,
    1.6722, 0.3579, '9 am to 9 pm weekdays', 0.2028, 'All other times',
    '2025-07-01', '2026-06-30',
    'ESC Victorian Default Offer 2025-26',
    'https://www.esc.vic.gov.au/electricity-and-gas/prices-tariffs-and-benchmarks/victorian-default-offer',
    TRUE, '2026-04-13'
  ),
  -- Powercor
  (
    'powercor-vic',
    'Powercor',
    'distributor',
    'VIC',
    'Powercor',
    'Covers western Melbourne suburbs and large regional areas including Ballarat and Geelong',
    ARRAY['3011','3012','3015','3016','3020','3021','3025','3029','3030'],
    1.3684, 'anytime', 0.3009, NULL, NULL, NULL, NULL, 0.2122,
    1.5905, 'anytime', 0.2927, NULL, NULL,
    1.3684, 0.4040, '3 pm to 9 pm everyday', 0.2365, 'All other times', 0.2122,
    1.5905, 0.3895, '9 am to 9 pm weekdays', 0.2111, 'All other times',
    '2025-07-01', '2026-06-30',
    'ESC Victorian Default Offer 2025-26',
    'https://www.esc.vic.gov.au/electricity-and-gas/prices-tariffs-and-benchmarks/victorian-default-offer',
    TRUE, '2026-04-13'
  ),
  -- United Energy
  (
    'united-energy-vic',
    'United Energy',
    'distributor',
    'VIC',
    'United Energy',
    'Covers south-eastern Melbourne suburbs including Mornington Peninsula',
    ARRAY['3145','3148','3150','3163','3168','3170','3175','3195','3930'],
    1.1648, 'anytime', 0.2884, NULL, NULL, NULL, NULL, 0.2107,
    1.3711, 'anytime', 0.2789, NULL, NULL,
    1.1648, 0.3837, '3 pm to 9 pm everyday', 0.2299, 'All other times', 0.2107,
    1.3711, 0.3501, '9 am to 9 pm weekdays', 0.2027, 'All other times',
    '2025-07-01', '2026-06-30',
    'ESC Victorian Default Offer 2025-26',
    'https://www.esc.vic.gov.au/electricity-and-gas/prices-tariffs-and-benchmarks/victorian-default-offer',
    TRUE, '2026-04-13'
  )
ON CONFLICT (id) DO UPDATE SET
  flat_domestic_supply_charge_daily = EXCLUDED.flat_domestic_supply_charge_daily,
  flat_domestic_anytime_rate = EXCLUDED.flat_domestic_anytime_rate,
  flat_domestic_block1_rate = EXCLUDED.flat_domestic_block1_rate,
  flat_domestic_block2_rate = EXCLUDED.flat_domestic_block2_rate,
  flat_domestic_controlled_load_rate = EXCLUDED.flat_domestic_controlled_load_rate,
  flat_smb_supply_charge_daily = EXCLUDED.flat_smb_supply_charge_daily,
  flat_smb_anytime_rate = EXCLUDED.flat_smb_anytime_rate,
  tou_domestic_supply_charge_daily = EXCLUDED.tou_domestic_supply_charge_daily,
  tou_domestic_peak_rate = EXCLUDED.tou_domestic_peak_rate,
  tou_domestic_offpeak_rate = EXCLUDED.tou_domestic_offpeak_rate,
  tou_domestic_controlled_load_rate = EXCLUDED.tou_domestic_controlled_load_rate,
  tou_smb_supply_charge_daily = EXCLUDED.tou_smb_supply_charge_daily,
  tou_smb_peak_rate = EXCLUDED.tou_smb_peak_rate,
  tou_smb_offpeak_rate = EXCLUDED.tou_smb_offpeak_rate,
  effective_from = EXCLUDED.effective_from,
  effective_to = EXCLUDED.effective_to,
  date_scrapped = EXCLUDED.date_scrapped,
  last_updated = NOW();
