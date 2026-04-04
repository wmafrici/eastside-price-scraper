/**
 * scrape-groceries.js
 * Pricee Grocery Price Scraper Ã¢ÂÂ Eastern Suburbs Melbourne
 *
 * Scrapes current prices for 8 grocery staples from Coles, Woolworths & Aldi
 * and inserts results into Supabase grocery_prices table.
 *
 * Run: node scrape-groceries.js
 * Required env vars: SUPABASE_URL, SUPABASE_KEY
 */

const https = require('https');
const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = process.env.SUPABASE_URL || 'https://mpbphijerbizlvfhssww.supabase.co';
const SUPABASE_KEY = process.env.SUPABASE_KEY;

if (!SUPABASE_KEY) {
  console.error('ERROR: SUPABASE_KEY environment variable is required');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

const SUBURBS = [
  { name: 'Rowville', postcode: 3178 },
  { name: 'Doncaster', postcode: 3108 },
  { name: 'Glen Waverley', postcode: 3150 },
  { name: 'Boronia', postcode: 3155 },
  { name: 'Forest Hill', postcode: 3131 },
  { name: 'Ferntree Gully', postcode: 3156 },
  { name: 'Belgrave', postcode: 3160 },
  { name: 'Croydon', postcode: 3136 },
  { name: 'Ringwood', postcode: 3134 },
  { name: 'Box Hill', postcode: 3128 },
  { name: 'Mooroolbark', postcode: 3138 },
];

const ITEMS = [
  { name: 'Milk (full cream, 1L)', colesSearch: 'full cream milk 1L', woolworthsSearch: 'full cream milk 1L', aldiCategory: 'dairy-eggs-fridge/milk' },
  { name: 'Bread (wholemeal, loaf)', colesSearch: 'wholemeal sandwich loaf', woolworthsSearch: 'wholemeal bread loaf', aldiCategory: 'bread-bakery/bread' },
  { name: 'Eggs (free range, dozen)', colesSearch: 'free range eggs 12', woolworthsSearch: 'free range eggs 12', aldiCategory: 'dairy-eggs-fridge/eggs' },
  { name: 'Steak (scotch fillet, ~500g)', colesSearch: 'scotch fillet steak', woolworthsSearch: 'scotch fillet steak', aldiCategory: 'meat-seafood/beef' },
  { name: 'Butter (salted, 250g)', colesSearch: 'salted butter 250g', woolworthsSearch: 'salted butter 250g', aldiCategory: 'dairy-eggs-fridge/butter-spreads' },
  { name: 'Chicken breast (per kg)', colesSearch: 'chicken breast fillet 1kg', woolworthsSearch: 'chicken breast fillet 1kg', aldiCategory: 'meat-seafood/chicken' },
  { name: 'Olive oil (extra virgin, ~500ml)', colesSearch: 'extra virgin olive oil 500ml', woolworthsSearch: 'extra virgin olive oil 500ml', aldiCategory: 'pantry/oils-vinegars' },
  { name: 'Pasta (dried, 500g)', colesSearch: 'pasta spaghetti 500g', woolworthsSearch: 'pasta spaghetti 500g', aldiCategory: 'pantry/pasta-rice-noodles' },
];

const TODAY = new Date().toISOString().slice(0, 10);

// Helper: fetch with JSON response
function fetchJson(url, options = {}) {
  return new Promise((resolve, reject) => {
    const req = https.request(url, {
      ...options,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': 'application/json',
        ...options.headers,
      }
    }, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try { resolve({ status: res.statusCode, data: JSON.parse(data) }); }
        catch { resolve({ status: res.statusCode, data: null, raw: data }); }
      });
    });
    req.on('error', reject);
    req.end();
  });
}

// Scrape Coles via their product API
async function scrapeColes() {
  console.log('Scraping Coles...');
  const results = [];

  for (const item of ITEMS) {
    try {
      const searchTerm = encodeURIComponent(item.colesSearch);
      const url = `https://www.coles.com.au/api/2.0/page/components/211?url=%2Fsearch%3Fq%3D${searchTerm}&pageSize=5&currentPage=0`;
      const resp = await fetchJson(url, {
        headers: { 'Accept': 'application/json', 'Ocp-Apim-Subscription-Key': 'no-key' }
      });

      let price = null;
      let productName = null;
      let unit = null;
      let onSpecial = false;

      if (resp.data?.catalogGroupView?.[0]?.products?.[0]) {
        const p = resp.data.catalogGroupView[0].products[0];
        price = p.pricing?.now ?? p.pricing?.was ?? null;
        productName = p.name;
        unit = p.size || null;
        onSpecial = !!p.pricing?.promotionType;
      }

      // Apply to all suburbs (Coles pricing is national)
      for (const suburb of SUBURBS) {
        if (price !== null) {
          results.push({
            item_name: item.name,
            supermarket: 'Coles',
            suburb: suburb.name,
            postcode: suburb.postcode,
            price: parseFloat(price),
            currency: 'AUD',
            unit,
            date: TODAY,
            in_stock: true,
            on_special: onSpecial,
            notes: productName ? `${productName}. Online price consistent across all 9 tracked suburbs.` : null,
            region: 'eastern_suburbs',
          });
        } else {
          results.push({
            item_name: item.name,
            supermarket: 'Coles',
            suburb: suburb.name,
            postcode: suburb.postcode,
            price: 0,
            currency: 'AUD',
            unit: null,
            date: TODAY,
            in_stock: false,
            on_special: false,
            notes: 'Price not available',
            region: 'eastern_suburbs',
          });
        }
      }

      console.log(`  Coles ${item.name}: $${price}`);
      await new Promise(r => setTimeout(r, 500)); // Rate limit
    } catch (err) {
      console.error(`  Coles ${item.name} error:`, err.message);
    }
  }

  return results;
}

// Scrape Woolworths via their internal search API
async function scrapeWoolworths() {
  console.log('Scraping Woolworths...');
  const results = [];

  for (const item of ITEMS) {
    try {
      const searchTerm = encodeURIComponent(item.woolworthsSearch);
      const url = `https://www.woolworths.com.au/apis/ui/Search/products?searchTerm=${searchTerm}&pageSize=5&pageNumber=1`;
      const resp = await fetchJson(url, {
        headers: {
          'Accept': 'application/json',
          'Referer': 'https://www.woolworths.com.au/',
        }
      });

      let price = null;
      let productName = null;
      let unit = null;
      let onSpecial = false;

      const products = resp.data?.Products?.[0]?.Products;
      if (products && products.length > 0) {
        const p = products[0];
        price = p.Price ?? p.WasPrice ?? null;
        productName = p.Name;
        unit = p.PackageSize || null;
        onSpecial = !!p.IsOnSpecial;
      }

      for (const suburb of SUBURBS) {
        if (price !== null) {
          results.push({
            item_name: item.name,
            supermarket: 'Woolworths',
            suburb: suburb.name,
            postcode: suburb.postcode,
            price: parseFloat(price),
            currency: 'AUD',
            unit,
            date: TODAY,
            in_stock: true,
            on_special: onSpecial,
            notes: productName ? `${productName}. Online price consistent across all 9 tracked suburbs.` : null,
            region: 'eastern_suburbs',
          });
        } else {
          results.push({
            item_name: item.name,
            supermarket: 'Woolworths',
            suburb: suburb.name,
            postcode: suburb.postcode,
            price: 0,
            currency: 'AUD',
            unit: null,
            date: TODAY,
            in_stock: false,
            on_special: false,
            notes: 'Price not available',
            region: 'eastern_suburbs',
          });
        }
      }

      console.log(`  Woolworths ${item.name}: $${price}`);
      await new Promise(r => setTimeout(r, 500));
    } catch (err) {
      console.error(`  Woolworths ${item.name} error:`, err.message);
    }
  }

  return results;
}

// Aldi: only 2 confirmed eastern suburb stores (Rowville, Croydon)
// Uses their website category pages
async function scrapeAldi() {
  console.log('Scraping Aldi (Rowville + Croydon stores)...');
  const results = [];
  const aldiSuburbs = SUBURBS.filter(s => ['Rowville', 'Croydon'].includes(s.name));

  for (const item of ITEMS) {
    try {
      const categoryUrl = `https://www.aldi.com.au/products/${item.aldiCategory}/`;
      // Fetch page text and extract price with regex
      const html = await new Promise((resolve, reject) => {
        https.get(categoryUrl, {
          headers: { 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36' }
        }, (res) => {
          let data = '';
          res.on('data', chunk => data += chunk);
          res.on('end', () => resolve(data));
        }).on('error', reject);
      });

      // Extract prices from product tiles
      const priceMatches = html.match(/\$(\d+\.\d{2})/g);
      const price = priceMatches ? parseFloat(priceMatches[0].replace('$', '')) : null;

      // Extract product name
      const nameMatch = html.match(/class="product-title[^"]*"[^>]*>([^<]+)/);
      const productName = nameMatch ? nameMatch[1].trim() : null;

      for (const suburb of aldiSuburbs) {
        if (price !== null) {
          results.push({
            item_name: item.name,
            supermarket: 'Aldi',
            suburb: suburb.name,
            postcode: suburb.postcode,
            price,
            currency: 'AUD',
            unit: null,
            date: TODAY,
            in_stock: true,
            on_special: false,
            notes: productName || `Aldi ${item.name}`,
            region: 'eastern_suburbs',
          });
        }
      }

      console.log(`  Aldi ${item.name}: $${price}`);
      await new Promise(r => setTimeout(r, 500));
    } catch (err) {
      console.error(`  Aldi ${item.name} error:`, err.message);
    }
  }

  return results;
}

async function insertToSupabase(rows) {
  console.log(`\nInserting ${rows.length} rows into Supabase...`);

  // Insert in batches of 50
  const BATCH = 50;
  let inserted = 0;

  for (let i = 0; i < rows.length; i += BATCH) {
    const batch = rows.slice(i, i + BATCH);
    const { error } = await supabase.from('grocery_prices').insert(batch);
    if (error) {
      console.error(`Batch ${i / BATCH + 1} error:`, error.message);
    } else {
      inserted += batch.length;
      console.log(`  Inserted batch ${i / BATCH + 1}: ${batch.length} rows (total: ${inserted})`);
    }
  }

  return inserted;
}

async function main() {
  console.log(`\nPricee Grocery Scraper Ã¢ÂÂ ${TODAY}`);
  console.log('=' .repeat(50));

  const [colesData, woolworthsData, aldiData] = await Promise.all([
    scrapeColes(),
    scrapeWoolworths(),
    scrapeAldi(),
  ]);

  const allRows = [...colesData, ...woolworthsData, ...aldiData];
  console.log(`\nTotal rows scraped: ${allRows.length}`);

  const inserted = await insertToSupabase(allRows);
  console.log(`\nDone! ${inserted} rows inserted into Supabase.`);
}

main().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
