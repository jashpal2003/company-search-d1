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


print("D1_API_BASE ", D1_API_BASE )
print("CLOUDFLARE_API_TOKEN ", CLOUDFLARE_API_TOKEN )
print("CLOUDFLARE_ACCOUNT_ID ", CLOUDFLARE_ACCOUNT_ID )
print("D1_DATABASE_ID ", D1_DATABASE_ID )
print("OGD_API_KEY ", OGD_API_KEY )


def execute_d1_query(sql, params=None):
    """Execute SQL query on D1 database via Cloudflare API"""
    # Check if essential secrets are missing (can happen if GitHub secret is wrong)
    if not CLOUDFLARE_API_TOKEN or not CLOUDFLARE_ACCOUNT_ID or not D1_DATABASE_ID:
         print("❌ Error: Cloudflare API Token, Account ID, or D1 Database ID is missing. Check GitHub Secrets.")
         return None

    headers = {
        'Authorization': f'Bearer {CLOUDFLARE_API_TOKEN}',
        'Content-Type': 'application/json'
    }

    payload = {
        'sql': sql
    }

    if params:
        payload['params'] = params

    try:
        response = requests.post(
            f"{D1_API_BASE}/query",
            headers=headers,
            json=payload,
            timeout=60 # Increased timeout for potentially long inserts
        )
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        return response.json()

    except requests.exceptions.RequestException as e:
        print(f"❌ D1 Query Request Error: {e}")
        # Attempt to print more details from the response if available
        try:
             print(f"   Response Status: {e.response.status_code}")
             print(f"   Response Text: {e.response.text}")
        except AttributeError:
             pass # No response object available
        return None

def sync_companies_batch(offset, limit=1000):
    """Fetch batch of companies from OGD API"""
    # Check if OGD API Key is missing
    if not OGD_API_KEY:
        print("❌ Error: OGD_API_KEY is missing. Check GitHub Secret.")
        return []

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
        response.raise_for_status() # Raise HTTPError for bad responses

        data = response.json()
        return data.get('records', [])

    except requests.exceptions.RequestException as e:
        print(f"❌ OGD API Request Error: {e}")
        try:
             print(f"   Response Status: {e.response.status_code}")
             print(f"   Response Text: {e.response.text}")
        except AttributeError:
             pass
        return [] # Return empty list on error
    except Exception as e:
        print(f"❌ Error fetching or parsing OGD data: {str(e)}")
        return []

def insert_companies_batch(companies):
    """Insert batch of companies into D1"""
    if not companies:
        return 0 # Return 0 if no companies to insert

    values = []
    for company in companies:
        # Use .get() with default None to handle missing keys gracefully
        cin = company.get('corporate_identification_number')
        if not cin:
            # print("Skipping record with missing CIN") # Optional: uncomment for debugging
            continue # Skip records without a CIN

        # Use lowercase keys and provide empty string defaults
        name = company.get('company_name', '').replace("'", "''")
        status = company.get('company_status', '').replace("'", "''")
        reg_date = company.get('date_of_registration', '').replace("'", "''") # Check format if errors occur
        company_class = company.get('company_class', '').replace("'", "''")
        roc = company.get('registrar_of_companies', '').replace("'", "''")
        # Check API response for the correct email key ('email_id' or 'email')
        email = company.get('email_id', company.get('email', '')).replace("'", "''")
        # Check API response for the correct state key ('registered_state' or 'state')
        state = company.get('registered_state', company.get('state', '')).replace("'", "''")

        # Basic validation/sanitization (can be expanded)
        if len(cin) > 50: cin = cin[:50] # Example: truncate long CINs if needed
        if len(name) > 255: name = name[:255] # Example: truncate long names

        values.append(f"('{name}', '{cin}', '{status}', '{reg_date}', '{company_class}', '{roc}', '{email}', '{state}')")

    if not values:
        print("ℹ️ No valid company records found in the current batch to insert.")
        return 0 # Return 0 if all records were skipped

    # Build the potentially very long SQL string
    sql = f"""
    INSERT OR REPLACE INTO companies
    (company_name, cin, status, registration_date, company_class, roc, email, state)
    VALUES {', '.join(values)}
    """

    # Execute the query
    result = execute_d1_query(sql)

    # Check the result and return the count OR 0 on failure
    if result and result.get('success', False):
        # D1 success response might contain metadata about changes
        meta = result.get('result', [{}])[0].get('meta', {})
        rows_written = meta.get('rows_written', len(values)) # Use rows_written if available, else assume all succeeded
        # print(f"D1 Meta: {meta}") # Optional: uncomment for debugging
        return rows_written
    else:
        print(f"❌ Failed to insert batch. D1 Result: {result}")
        return 0 # *** THIS IS THE FIX for the TypeError ***

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
        try:
            # Safely access nested data
            count = test_result.get('result', [{}])[0].get('results', [{}])[0].get('count', 'N/A')
            print(f"✓ Database connected. Current companies: {count}")
        except (IndexError, KeyError, TypeError):
             print(f"✓ Database connected, but couldn't parse count. Response: {test_result}")
    else:
        print("❌ Database connection failed!")
        return # Exit if connection fails

    # Sync companies in batches
    total_synced = 0
    offset = 0
    batch_size = 500 # Reduced batch size to potentially avoid "SQL too long" errors
    max_records = 2000000 # 2M companies

    print()
    print(f"Starting sync... (Target: {max_records:,} companies, Batch Size: {batch_size})")
    print()

    while total_synced < max_records:
        batch_num = offset // batch_size + 1
        print(f"📥 Fetching batch {batch_num}... (Offset: {offset:,})")

        # Fetch batch from OGD
        companies = sync_companies_batch(offset, batch_size)

        if companies is None: # Handle case where sync_companies_batch had an error and returned None explicitly
             print("❌ Error fetching batch, stopping sync.")
             break

        if not companies:
            # Check if this is the first batch or not
            if offset == 0:
                 print("❌ No records received from OGD API on the first batch. Check API Key and Resource ID.")
            else:
                 print("✓ No more records available from OGD or end of data reached.")
            break # Exit the loop

        # Insert into D1
        print(f"💾 Inserting {len(companies)} fetched companies into D1...")
        inserted = insert_companies_batch(companies)

        # inserted should now always be an int (0 on failure)
        if inserted is None: # Should not happen now, but good to check
             print("❌ Insert function returned None unexpectedly. Stopping sync.")
             inserted = 0 # Treat as 0 inserted for safety
             break

        total_synced += inserted

        print(f"✓ Inserted in batch {batch_num}: {inserted} | Total Synced: {total_synced:,}")
        print()

        # Check if we got fewer records than requested (means end of data)
        if len(companies) < batch_size:
            print("✓ Reached end of available data (received fewer records than batch size).")
            break

        offset += batch_size

        # Rate limiting (be nice to APIs)
        time.sleep(1) # Slightly reduced sleep time

        # Safety break if something goes wrong and offset increases indefinitely
        if batch_num > (max_records / batch_size) + 100: # Allow some extra loops
             print("❌ Safety break: Exceeded expected number of batches. Stopping sync.")
             break

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

    if stats_result and stats_result.get('success', False):
         try:
            stats = stats_result.get('result', [{}])[0].get('results', [{}])[0]
            print(f"Total companies in D1: {stats.get('total', 'N/A'):,}")
            print(f"Active companies in D1: {stats.get('active', 'N/A'):,}")
         except (IndexError, KeyError, TypeError):
             print(f"✓ Sync finished, but couldn't parse final stats. Response: {stats_result}")
    else:
         print("❌ Could not fetch final statistics from D1.")

    print(f"Completed at: {datetime.utcnow()}")
    print("=" * 60)

if __name__ == '__main__':
    main()
