from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import httpx
import sqlite3
import json
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager
import logging
import asyncio
from collections import defaultdict

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Rate limiting configuration
RATE_LIMIT_REQUESTS = 60  # requests per window
RATE_LIMIT_WINDOW = 60  # seconds
BATCH_DELAY = 1.0  # seconds to wait after 60 requests

# Database setup
def init_db():
    """Initialize SQLite database"""
    conn = sqlite3.connect('data.db')
    cursor = conn.cursor()
    
    # Table 1: Filtered email accounts (with warmup filters)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS filtered_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Table 2: Domain-focused data (no warmup filters)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS domain_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS fetch_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            status TEXT NOT NULL,
            message TEXT,
            fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

async def rate_limited_request(client: httpx.AsyncClient, url: str, request_count: list):
    """
    Make a rate-limited HTTP request.
    Allows 60 requests, then waits 1 second before continuing.
    """
    # Check if we've hit the rate limit
    if request_count[0] > 0 and request_count[0] % RATE_LIMIT_REQUESTS == 0:
        logger.info(f"Hit {RATE_LIMIT_REQUESTS} requests, waiting {BATCH_DELAY} seconds...")
        await asyncio.sleep(BATCH_DELAY)
    
    request_count[0] += 1
    
    if request_count[0] % 100 == 0:
        logger.info(f"Made {request_count[0]} requests so far...")
    
    response = await client.get(url)
    response.raise_for_status()
    return response.json()

# Fetch and filter data function
async def fetch_and_process_data():
    """
    Fetch data from SmartLead API and create two tables:
    1. Table 1 (filtered_data): Email accounts filtered by warmup reputation and date
    2. Table 2 (domain_data): Domain-level aggregations without warmup filters
    """
    logger.info("Starting SmartLead data fetch job...")
    
    try:
        import os
        
        api_key = os.getenv('SMARTLEAD_API_KEY', 'YOUR_API_KEY_HERE')
        
        if api_key == 'YOUR_API_KEY_HERE':
            raise ValueError("Please set your SmartLead API key in the SMARTLEAD_API_KEY environment variable or update the code")
        
        base_url = "https://server.smartlead.ai/api/v1"
        smart_senders_url = "https://smart-senders.smartlead.ai/api/v1/smart-senders"
        
        request_count = [0]  # Track total requests for rate limiting
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            
            # ========================================
            # STEP 1: Get vendor mapping from Smart Senders
            # ========================================
            logger.info("Fetching vendor data from Smart Senders...")
            
            # Get domain list
            domain_list_url = f"{smart_senders_url}/get-domain-list?api_key={api_key}"
            domain_list = await rate_limited_request(client, domain_list_url, request_count)
            
            # Get vendors
            vendors_url = f"{smart_senders_url}/get-vendors?api_key={api_key}"
            vendors = await rate_limited_request(client, vendors_url, request_count)
            
            # Create vendor lookup: domain -> vendor_id -> vendor_name
            vendor_lookup = {v.get('id'): v.get('name') for v in vendors if isinstance(vendors, list)}
            domain_vendor_map = {}
            
            if isinstance(domain_list, list):
                for domain_entry in domain_list:
                    domain = domain_entry.get('domain')
                    vendor_id = domain_entry.get('vendor_id')
                    if domain and vendor_id:
                        domain_vendor_map[domain] = vendor_lookup.get(vendor_id, 'Unknown')
            
            logger.info(f"Loaded vendor mapping for {len(domain_vendor_map)} domains")
            
            # ========================================
            # STEP 2: Get all email accounts (shared for both tables)
            # ========================================
            logger.info("Fetching all email accounts...")
            all_email_accounts = []
            offset = 0
            limit = 100
            
            while True:
                url = f"{base_url}/email-accounts/?api_key={api_key}&offset={offset}&limit={limit}"
                data = await rate_limited_request(client, url, request_count)
                
                if not data or len(data) == 0:
                    break
                
                for account in data:
                    warmup_details = account.get('warmup_details') or {}
                    
                    # Parse domain from email
                    from_email = account.get('from_email', '')
                    domain = from_email.split('@')[1] if '@' in from_email else None
                    
                    all_email_accounts.append({
                        'email_account_id': account.get('id'),
                        'email_address': from_email,
                        'domain': domain,
                        'type': account.get('type'),
                        'message_per_day': account.get('message_per_day', 0),
                        'warmup_reputation': warmup_details.get('warmup_reputation'),
                        'warmup_details': warmup_details
                    })
                
                if len(data) < limit:
                    break
                offset += limit
            
            logger.info(f"Fetched {len(all_email_accounts)} total email accounts")
            
            # ========================================
            # STEP 3: Get all campaigns (shared for both tables)
            # ========================================
            logger.info("Fetching campaigns...")
            url = f"{base_url}/campaigns?api_key={api_key}"
            campaigns = await rate_limited_request(client, url, request_count)
            
            campaigns_list = []
            for campaign in campaigns:
                campaigns_list.append({
                    'campaign_id': campaign.get('id'),
                    'campaign_name': campaign.get('name'),
                    'campaign_status': campaign.get('status')
                })
            
            logger.info(f"Fetched {len(campaigns_list)} campaigns")
            
            # ========================================
            # STEP 4: Get email accounts for each campaign (shared, with rate limiting)
            # ========================================
            logger.info("Fetching email accounts per campaign (rate limited)...")
            campaign_account_mappings = []
            
            for i, campaign in enumerate(campaigns_list):
                campaign_id = campaign['campaign_id']
                url = f"{base_url}/campaigns/{campaign_id}/email-accounts?api_key={api_key}"
                
                try:
                    campaign_accounts = await rate_limited_request(client, url, request_count)
                    
                    for account_mapping in campaign_accounts:
                        campaign_account_mappings.append({
                            'email_account_id': account_mapping.get('id'),
                            'campaign_id': campaign_id,
                            'time_added_to_campaign': account_mapping.get('created_at')
                        })
                    
                    if (i + 1) % 20 == 0:
                        logger.info(f"Processed {i + 1}/{len(campaigns_list)} campaigns...")
                        
                except Exception as e:
                    logger.warning(f"Could not fetch accounts for campaign {campaign_id}: {str(e)}")
                    continue
            
            logger.info(f"Fetched {len(campaign_account_mappings)} campaign-account mappings")
            
            # ========================================
            # TABLE 1: Filtered email accounts (with warmup filters)
            # ========================================
            logger.info("Building Table 1 (filtered email accounts)...")
            
            # Filter by warmup reputation <= 98%
            filtered_accounts = []
            for account in all_email_accounts:
                reputation = account['warmup_reputation']
                
                if reputation is None:
                    continue
                else:
                    try:
                        rep_str = str(reputation).replace('%', '').strip()
                        rep_value = float(rep_str)
                        if rep_value <= 98:
                            filtered_accounts.append(account)
                    except (ValueError, TypeError):
                        logger.warning(f"Could not parse warmup_reputation: {reputation}")
                        continue
            
            logger.info(f"After reputation filter: {len(filtered_accounts)} accounts")
            
            # Get warmup_start_date for filtered accounts (rate limited)
            logger.info("Fetching warmup start dates for filtered accounts...")
            accounts_with_dates = []
            
            for i, account in enumerate(filtered_accounts):
                email_id = account['email_account_id']
                url = f"{base_url}/email-accounts/{email_id}?api_key={api_key}"
                
                try:
                    account_detail = await rate_limited_request(client, url, request_count)
                    warmup_info = account_detail.get('warmup_details') or {}
                    
                    accounts_with_dates.append({
                        **account,
                        'warmup_start_date': warmup_info.get('created_at')
                    })
                    
                    if (i + 1) % 50 == 0:
                        logger.info(f"Fetched details for {i + 1}/{len(filtered_accounts)} accounts...")
                        
                except Exception as e:
                    logger.warning(f"Could not fetch details for email account {email_id}: {str(e)}")
                    accounts_with_dates.append({
                        **account,
                        'warmup_start_date': None
                    })
            
            # Filter by warmup_start_date >= 2 weeks ago
            two_weeks_ago = datetime.now(timezone.utc) - timedelta(weeks=2)
            date_filtered_accounts = []
            
            for account in accounts_with_dates:
                warmup_start = account['warmup_start_date']
                
                if warmup_start is None:
                    continue
                else:
                    try:
                        start_date = datetime.fromisoformat(warmup_start.replace('Z', '+00:00'))
                        if start_date <= two_weeks_ago:
                            date_filtered_accounts.append(account)
                    except (ValueError, AttributeError) as e:
                        logger.warning(f"Could not parse warmup_start_date: {warmup_start}")
                        continue
            
            logger.info(f"After date filter: {len(date_filtered_accounts)} accounts")
            
            # Join with campaigns for Table 1
            campaign_lookup = {}
            for mapping in campaign_account_mappings:
                email_id = mapping['email_account_id']
                if email_id not in campaign_lookup:
                    campaign_lookup[email_id] = []
                campaign_lookup[email_id].append(mapping)
            
            campaign_info_lookup = {c['campaign_id']: c for c in campaigns_list}
            
            table1_data = []
            for account in date_filtered_accounts:
                email_id = account['email_account_id']
                campaigns = campaign_lookup.get(email_id, [None])
                
                if not campaigns or campaigns == [None]:
                    table1_data.append({
                        'email_account_id': email_id,
                        'email_address': account['email_address'],
                        'warmup_reputation': account['warmup_reputation'],
                        'warmup_start_date': account['warmup_start_date'],
                        'campaign_id': None,
                        'campaign_name': None,
                        'campaign_status': None,
                        'time_added_to_campaign': None
                    })
                else:
                    for campaign_mapping in campaigns:
                        campaign_id = campaign_mapping['campaign_id']
                        campaign_info = campaign_info_lookup.get(campaign_id, {})
                        
                        table1_data.append({
                            'email_account_id': email_id,
                            'email_address': account['email_address'],
                            'warmup_reputation': account['warmup_reputation'],
                            'warmup_start_date': account['warmup_start_date'],
                            'campaign_id': campaign_id,
                            'campaign_name': campaign_info.get('campaign_name'),
                            'campaign_status': campaign_info.get('campaign_status'),
                            'time_added_to_campaign': campaign_mapping['time_added_to_campaign']
                        })
            
            logger.info(f"Table 1 complete: {len(table1_data)} rows")
            
            # ========================================
            # TABLE 2: Domain-focused data (no warmup filters)
            # ========================================
            logger.info("Building Table 2 (domain data)...")
            
            # Create domain + campaign aggregations
            domain_campaign_data = defaultdict(lambda: {
                'email_accounts': set(),
                'total_messages_per_day': 0,
                'type': None,
                'campaign_status': None
            })
            
            # Map all email accounts to their campaigns
            for account in all_email_accounts:
                email_id = account['email_account_id']
                domain = account['domain']
                type_value = account['type']
                messages_per_day = account['message_per_day'] or 0
                
                campaigns = campaign_lookup.get(email_id, [])
                
                # If account has campaigns, add to each campaign row
                if campaigns:
                    for campaign_mapping in campaigns:
                        campaign_id = campaign_mapping['campaign_id']
                        campaign_info = campaign_info_lookup.get(campaign_id, {})
                        campaign_name = campaign_info.get('campaign_name')
                        campaign_status = campaign_info.get('campaign_status')
                        
                        if domain and campaign_name:
                            key = (domain, campaign_name)
                            domain_campaign_data[key]['email_accounts'].add(email_id)
                            domain_campaign_data[key]['total_messages_per_day'] += messages_per_day
                            domain_campaign_data[key]['campaign_status'] = campaign_status
                            if not domain_campaign_data[key]['type']:
                                domain_campaign_data[key]['type'] = type_value
                else:
                    # Account not in any campaign - create row with null campaign
                    if domain:
                        key = (domain, None)
                        domain_campaign_data[key]['email_accounts'].add(email_id)
                        domain_campaign_data[key]['total_messages_per_day'] += messages_per_day
                        if not domain_campaign_data[key]['type']:
                            domain_campaign_data[key]['type'] = type_value
            
            # Build final Table 2
            table2_data = []
            for (domain, campaign_name), data in domain_campaign_data.items():
                vendor_name = domain_vendor_map.get(domain, 'Unknown')
                
                table2_data.append({
                    'domain': domain,
                    'vendor_name': vendor_name,
                    'campaign_name': campaign_name,
                    'campaign_status': data['campaign_status'],
                    'type': data['type'],
                    'number_of_email_accounts': len(data['email_accounts']),
                    'total_messages_per_day': data['total_messages_per_day']
                })
            
            logger.info(f"Table 2 complete: {len(table2_data)} rows")
            
            # ========================================
            # Store both tables in database
            # ========================================
            conn = sqlite3.connect('data.db')
            cursor = conn.cursor()
            
            # Store Table 1
            cursor.execute('DELETE FROM filtered_data')
            cursor.execute(
                'INSERT INTO filtered_data (data) VALUES (?)',
                (json.dumps(table1_data),)
            )
            
            # Store Table 2
            cursor.execute('DELETE FROM domain_data')
            cursor.execute(
                'INSERT INTO domain_data (data) VALUES (?)',
                (json.dumps(table2_data),)
            )
            
            # Log success
            cursor.execute(
                'INSERT INTO fetch_log (status, message) VALUES (?, ?)',
                ('success', f'Successfully processed data. '
                           f'Table 1: {len(table1_data)} rows, '
                           f'Table 2: {len(table2_data)} rows, '
                           f'Total API requests: {request_count[0]}')
            )
            
            conn.commit()
            conn.close()
            
            logger.info(f"Data stored successfully. Total API requests made: {request_count[0]}")
            
            return {
                "status": "success",
                "table1_rows": len(table1_data),
                "table2_rows": len(table2_data),
                "total_api_requests": request_count[0],
                "email_accounts_fetched": len(all_email_accounts),
                "campaigns_fetched": len(campaigns_list)
            }
        
    except Exception as e:
        logger.error(f"Error fetching data: {str(e)}")
        
        # Log error
        conn = sqlite3.connect('data.db')
        cursor = conn.cursor()
        cursor.execute(
            'INSERT INTO fetch_log (status, message) VALUES (?, ?)',
            ('error', str(e))
        )
        conn.commit()
        conn.close()
        
        return {"status": "error", "message": str(e)}

# Lifespan context manager for startup/shutdown
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    try:
        init_db()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
    
    logger.info("API ready - use /refresh endpoint or trigger via n8n workflow")
    
    yield
    
    # Shutdown
    logger.info("API shutting down")

# Initialize FastAPI app
app = FastAPI(
    title="SmartLead Data API",
    description="On-demand data fetching service for SmartLead - trigger via n8n workflows",
    version="2.0.0",
    lifespan=lifespan
)

@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "status": "running",
        "service": "SmartLead Data API",
        "version": "2.0.0",
        "timestamp": datetime.utcnow().isoformat(),
        "scheduling": "Managed via n8n workflows",
        "endpoints": {
            "table1": "/filtered-data",
            "table2": "/domain-data",
            "refresh": "/refresh",
            "logs": "/logs"
        }
    }

@app.get("/filtered-data")
async def get_filtered_data():
    """
    Get Table 1: Filtered email accounts with warmup filters applied.
    Returns email accounts with warmup_reputation <= 98% and warmup_start_date >= 2 weeks ago.
    """
    try:
        conn = sqlite3.connect('data.db')
        cursor = conn.cursor()
        cursor.execute('SELECT data, created_at FROM filtered_data ORDER BY created_at DESC LIMIT 1')
        result = cursor.fetchone()
        conn.close()
        
        if result:
            data = json.loads(result[0])
            return {
                "status": "success",
                "table": "filtered_data",
                "data": data,
                "fetched_at": result[1],
                "row_count": len(data) if isinstance(data, list) else 1
            }
        else:
            return {
                "status": "no_data",
                "message": "No data available yet. Run /refresh to fetch data."
            }
    
    except Exception as e:
        logger.error(f"Error retrieving filtered data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/domain-data")
async def get_domain_data():
    """
    Get Table 2: Domain-focused data without warmup filters.
    Returns aggregated data by domain and campaign with vendor names.
    """
    try:
        conn = sqlite3.connect('data.db')
        cursor = conn.cursor()
        cursor.execute('SELECT data, created_at FROM domain_data ORDER BY created_at DESC LIMIT 1')
        result = cursor.fetchone()
        conn.close()
        
        if result:
            data = json.loads(result[0])
            return {
                "status": "success",
                "table": "domain_data",
                "data": data,
                "fetched_at": result[1],
                "row_count": len(data) if isinstance(data, list) else 1
            }
        else:
            return {
                "status": "no_data",
                "message": "No data available yet. Run /refresh to fetch data."
            }
    
    except Exception as e:
        logger.error(f"Error retrieving domain data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/refresh")
@app.post("/refresh")
async def manual_refresh():
    """
    Manually trigger data fetch and processing for both tables.
    Note: This may take 2-5 minutes due to rate limiting (1 sec pause every 60 requests).
    """
    logger.info("Manual refresh triggered")
    result = await fetch_and_process_data()
    return result

@app.get("/logs")
async def get_logs(limit: int = 10):
    """
    Get fetch logs to monitor the service.
    """
    try:
        conn = sqlite3.connect('data.db')
        cursor = conn.cursor()
        cursor.execute(
            'SELECT status, message, fetched_at FROM fetch_log ORDER BY fetched_at DESC LIMIT ?',
            (limit,)
        )
        logs = cursor.fetchall()
        conn.close()
        
        return {
            "logs": [
                {
                    "status": log[0],
                    "message": log[1],
                    "timestamp": log[2]
                }
                for log in logs
            ]
        }
    
    except Exception as e:
        logger.error(f"Error retrieving logs: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
