import os
import requests
import json
from datetime import datetime

# Get secrets from GitHub
COWORK_API_KEY = os.getenv('COWORK_API_KEY')
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

print(f"Starting scrape at {datetime.now()}")
print(f"Supabase URL: {SUPABASE_URL[:50]}...")

# For now, this is a test script
# It will be replaced with your actual Cowork scraping logic

def test_connection():
    """Test that we can connect to Supabase"""
    headers = {
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json'
    }
    
    try:
        response = requests.get(
            f"{SUPABASE_URL}/rest/v1/businesses?limit=1",
            headers=headers,
            timeout=10
        )
        
        if response.status_code == 200:
            print("✓ Successfully connected to Supabase!")
            print(f"Got {len(response.json())} records")
            return True
        else:
            print(f"✗ Supabase error: {response.status_code}")
            print(response.text)
            return False
    except Exception as e:
        print(f"✗ Connection failed: {e}")
        return False

def main():
    print("Testing Supabase connection...")
    
    if test_connection():
        print("✓ All systems ready!")
        print("Next step: Replace this with your Cowork scraping script")
    else:
        print("✗ Connection test failed")
        print("Check your SUPABASE_URL and SUPABASE_KEY secrets")

if __name__ == '__main__':
    main()
