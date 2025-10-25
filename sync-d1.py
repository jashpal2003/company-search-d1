"""
GitHub Actions Sync Script for Cloudflare D1
Syncs 2M+ Indian companies from OGD to Cloudflare D1 Database
"""

import os
import requests
import time
from datetime import datetime

# Configuration
CLOUDFLARE_ACCOUNT_ID = os.getenv('CLOUDFLARE_ACCOUNT_ID')
CLOUDFLARE_API_TOKEN = os.getenv('CLOUDFLARE_API_TOKEN')
D1_DATABASE_ID = os.getenv('D1_DATABASE_ID')
OGD_API_KEY = "579b464db66ec23bdd000001374c3ea40d5040795584f9345656aee7"

OGD_API_BASE = "https://api.data.gov.in/resource/"
RESOURCE_ID = "ec58dab7-d891-4abb-936e-d5d274a6ce9b"

# D1 API endpoints
D1_API_BASE = f"https://api.cloudflare.com/client/v4/accounts/{CLOUDFLARE_ACCOUNT_ID}/d1/database/{D1_DATABASE_ID}"

def execute_d1_query(sql, params=None):
    """Execute SQL query on D1 database via Cloudflare API"""
    headers = {
        'Authorization': f'Bearer {CLOUDFLARE_API_TOKEN}',
        'Content-Type': 'application/json'
    }
    
    payload = {
        'sql': sql
    }
    
    if params:
        payload['params'] = params
    
    response = requests.post(
        f"{D1_API_BASE}/query",
        headers=headers,
        json=payload
    )
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"❌ D1 Query Error: {response.status_code} - {response.text}")
        return None

def sync_companies_batch(offset, limit=1000):
    """Fetch batch of companies from OGD API"""
    params = {
        'api-key': OGD_API_KEY,
        'format': 'json',
        'limit': limit,
        'offset': offset,
       
    }
    
    try:
        response = requests.get(
            f"{OGD_API_BASE}{RESOURCE_ID}",
            params=params,
            timeout=30
        )
        
        if response.status_code == 200:
            data = response.json()
            return data.get('records', [])
        else:
            print(f"❌ OGD API Error: {response.status_code}")
            return []
    except Exception as e:
        print(f"❌ Error fetching data: {str(e)}")
        return []

def insert_companies_batch(companies):
    """Insert batch of companies into D1"""
    if not companies:
        return 0
    
    # Build batch insert query
    values = []
    for company in companies:
        cin = company.get('CORPORATE_IDENTIFICATION_NUMBER')
        if not cin:
            continue
        
        # Escape single quotes in strings
        name = company.get('COMPANY_NAME', '').replace("'", "''")
        status = company.get('COMPANY_STATUS', '').replace("'", "''")
        reg_date = company.get('DATE_OF_REGISTRATION', '').replace("'", "''")
        company_class = company.get('COMPANY_CLASS', '').replace("'", "''")
        roc = company.get('REGISTRAR_OF_COMPANIES', '').replace("'", "''")
        email = company.get('EMAIL', '').replace("'", "''")
        state = company.get('STATE', '').replace("'", "''")
        
        values.append(f"('{name}', '{cin}', '{status}', '{reg_date}', '{company_class}', '{roc}', '{email}', '{state}')")
    
    if not values:
        return 0
    
    # Insert with UPSERT logic (SQLite doesn't support full UPSERT, use REPLACE)
    sql = f"""
    INSERT OR REPLACE INTO companies 
    (company_name, cin, status, registration_date, company_class, roc, email, state)
    VALUES {', '.join(values)}
    """
    
    result = execute_d1_query(sql)
    
    if result:
        return len(values)
    return 0

def main():
    print("=" * 60)
    print("Cloudflare D1 Company Data Sync")
    print("=" * 60)
    print(f"Started at: {datetime.utcnow()}")
    print()
    
    # Check if database is accessible
    print("✓ Checking D1 database connection...")
    test_result = execute_d1_query("SELECT COUNT(*) as count FROM companies")
    if test_result:
        print(f"✓ Database connected. Current companies: {test_result}")
    else:
        print("❌ Database connection failed!")
        return
    
    # Sync companies in batches
    total_synced = 0
    offset = 0
    batch_size = 1000
    max_records = 2000000  # 2M companies (fits in 10 GB)
    
    print()
    print(f"Starting sync... (Target: {max_records:,} companies)")
    print()
    
    while total_synced < max_records:
        print(f"📥 Fetching batch {offset // batch_size + 1}... (Offset: {offset:,})")
        
        # Fetch batch from OGD
        companies = sync_companies_batch(offset, batch_size)
        
        if not companies:
            print("✓ No more records available from OGD")
            break
        
        # Insert into D1
        print(f"💾 Inserting {len(companies)} companies into D1...")
        inserted = insert_companies_batch(companies)
        total_synced += inserted
        
        print(f"✓ Inserted: {inserted} | Total: {total_synced:,}")
        print()
        
        # Check if we got all available data
        if len(companies) < batch_size:
            print("✓ Reached end of available data")
            break
        
        offset += batch_size
        
        # Rate limiting (be nice to APIs)
        time.sleep(2)
        
        # Stop if we've reached our target
        if total_synced >= max_records:
            print(f"✓ Reached target of {max_records:,} companies")
            break
    
    # Get final statistics
    print()
    print("=" * 60)
    print("Sync Complete!")
    print("=" * 60)
    
    stats_result = execute_d1_query("""
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN status = 'Active' THEN 1 END) as active
        FROM companies
    """)
    
    if stats_result and stats_result.get('result'):
        stats = stats_result['result'][0]['results'][0]
        print(f"Total companies: {stats.get('total', 0):,}")
        print(f"Active companies: {stats.get('active', 0):,}")
    
    print(f"Completed at: {datetime.utcnow()}")
    print("=" * 60)

if __name__ == '__main__':
    main()
