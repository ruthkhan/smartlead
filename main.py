from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import httpx
import sqlite3
import json
from datetime import datetime
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize scheduler
scheduler = AsyncIOScheduler()

# Database setup
def init_db():
    """Initialize SQLite database"""
    conn = sqlite3.connect('data.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS filtered_data (
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

# Fetch and filter data function
async def fetch_and_filter_data():
    """
    Fetch data from SmartLead API and perform joins to create final dataset.
    
    Workflow:
    1. [Dataset 1] Call /email-accounts to get all email account data
    2. [Dataset 2] Call /campaigns to get all campaigns
    3. [Dataset 3] For each campaign, call /campaigns/{campaign_id}/email-accounts
    4. [Dataset 4] Left outer join Dataset 1 and Dataset 3 on email account id
    5. [Dataset 5] Left join Dataset 4 and Dataset 2 on campaign id
    """
    logger.info("Starting SmartLead data fetch job...")
    
    try:
        # IMPORTANT: Set your SmartLead API key here or use environment variable
        # Get from: https://app.smartlead.ai/app/settings/profile
        import os
        api_key = os.getenv('SMARTLEAD_API_KEY', 'YOUR_API_KEY_HERE')
        
        if api_key == 'YOUR_API_KEY_HERE':
            raise ValueError("Please set your SmartLead API key in the SMARTLEAD_API_KEY environment variable or update the code")
        
        base_url = "https://server.smartlead.ai/api/v1"
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            # ========================================
            # Dataset 1: Get all email accounts
            # ========================================
            logger.info("Fetching email accounts (Dataset 1)...")
            dataset1 = []
            offset = 0
            limit = 100
            
            # First, get list of all email accounts
            email_account_ids = []
            while True:
                url = f"{base_url}/email-accounts/?api_key={api_key}&offset={offset}&limit={limit}"
                response = await client.get(url)
                response.raise_for_status()
                data = response.json()
                
                if not data or len(data) == 0:
                    break
                
                for account in data:
                    email_account_ids.append({
                        'id': account.get('id'),
                        'from_email': account.get('from_email'),
                        'warmup_reputation': account.get('warmup_details', {}).get('warmup_reputation')
                    })
                
                if len(data) < limit:
                    break
                offset += limit
            
            # Now fetch individual email account details to get warmup_start_date
            logger.info(f"Fetching detailed warmup info for {len(email_account_ids)} email accounts...")
            for account_info in email_account_ids:
                email_id = account_info['id']
                
                # Fetch individual account details
                url = f"{base_url}/email-accounts/{email_id}?api_key={api_key}"
                try:
                    response = await client.get(url)
                    response.raise_for_status()
                    account_detail = response.json()
                    
                    dataset1.append({
                        'email_account_id': email_id,
                        'email_address': account_info['from_email'],
                        'warmup_reputation': account_info['warmup_reputation'],
                        'warmup_start_date': account_detail.get('warmupdetails', {}).get('created_at')
                    })
                except Exception as e:
                    logger.warning(f"Could not fetch details for email account {email_id}: {str(e)}")
                    # Add with null warmup_start_date if individual fetch fails
                    dataset1.append({
                        'email_account_id': email_id,
                        'email_address': account_info['from_email'],
                        'warmup_reputation': account_info['warmup_reputation'],
                        'warmup_start_date': None
                    })
            
            logger.info(f"Fetched {len(dataset1)} email accounts")
            
            # ========================================
            # Dataset 2: Get all campaigns
            # ========================================
            logger.info("Fetching campaigns (Dataset 2)...")
            url = f"{base_url}/campaigns?api_key={api_key}"
            response = await client.get(url)
            response.raise_for_status()
            campaigns = response.json()
            
            dataset2 = []
            for campaign in campaigns:
                dataset2.append({
                    'campaign_id': campaign.get('id'),
                    'campaign_name': campaign.get('name'),
                    'campaign_status': campaign.get('status')
                })
            
            logger.info(f"Fetched {len(dataset2)} campaigns")
            
            # ========================================
            # Dataset 3: Get email accounts for each campaign
            # ========================================
            logger.info("Fetching email accounts per campaign (Dataset 3)...")
            dataset3 = []
            
            for campaign in dataset2:
                campaign_id = campaign['campaign_id']
                url = f"{base_url}/campaigns/{campaign_id}/email-accounts?api_key={api_key}"
                
                try:
                    response = await client.get(url)
                    response.raise_for_status()
                    campaign_accounts = response.json()
                    
                    for account_mapping in campaign_accounts:
                        dataset3.append({
                            'email_account_id': account_mapping.get('id'),
                            'campaign_id': campaign_id,
                            'time_added_to_campaign': account_mapping.get('created_at')
                        })
                except Exception as e:
                    logger.warning(f"Could not fetch accounts for campaign {campaign_id}: {str(e)}")
                    continue
            
            logger.info(f"Fetched {len(dataset3)} campaign-account mappings")
            
            # ========================================
            # Dataset 4: Left outer join Dataset 1 and Dataset 3
            # ========================================
            logger.info("Performing join operations...")
            dataset4 = []
            
            # Create a lookup for dataset3 by email_account_id
            dataset3_lookup = {}
            for row in dataset3:
                email_account_id = row['email_account_id']
                if email_account_id not in dataset3_lookup:
                    dataset3_lookup[email_account_id] = []
                dataset3_lookup[email_account_id].append(row)
            
            # Left outer join: for each email account, get all campaign associations
            for account in dataset1:
                email_account_id = account['email_account_id']
                campaign_mappings = dataset3_lookup.get(email_account_id, [None])
                
                # If no campaigns, still include the email account with null campaign data
                if not campaign_mappings or campaign_mappings == [None]:
                    dataset4.append({
                        **account,
                        'campaign_id': None,
                        'time_added_to_campaign': None
                    })
                else:
                    # For each campaign the email account is associated with
                    for mapping in campaign_mappings:
                        dataset4.append({
                            **account,
                            'campaign_id': mapping['campaign_id'],
                            'time_added_to_campaign': mapping['time_added_to_campaign']
                        })
            
            # ========================================
            # Dataset 5: Left join Dataset 4 and Dataset 2
            # ========================================
            # Create a lookup for dataset2 by campaign_id
            dataset2_lookup = {row['campaign_id']: row for row in dataset2}
            
            final_dataset = []
            for row in dataset4:
                campaign_id = row['campaign_id']
                campaign_info = dataset2_lookup.get(campaign_id, {})
                
                final_row = {
                    'email_account_id': row['email_account_id'],
                    'email_address': row['email_address'],
                    'warmup_reputation': row['warmup_reputation'],
                    'warmup_start_date': row['warmup_start_date'],
                    'campaign_id': campaign_id,
                    'campaign_name': campaign_info.get('campaign_name'),
                    'campaign_status': campaign_info.get('campaign_status'),
                    'time_added_to_campaign': row['time_added_to_campaign']
                }
                final_dataset.append(final_row)
        
        # Store filtered data in database
        conn = sqlite3.connect('data.db')
        cursor = conn.cursor()
        
        # Clear old data (keep only latest)
        cursor.execute('DELETE FROM filtered_data')
        
        # Insert new data
        cursor.execute(
            'INSERT INTO filtered_data (data) VALUES (?)',
            (json.dumps(final_dataset),)
        )
        
        # Log successful fetch
        cursor.execute(
            'INSERT INTO fetch_log (status, message) VALUES (?, ?)',
            ('success', f'Successfully fetched and joined data: {len(dataset1)} email accounts, '
                       f'{len(dataset2)} campaigns, {len(final_dataset)} final rows')
        )
        
        conn.commit()
        conn.close()
        
        logger.info(f"Successfully processed {len(final_dataset)} final rows")
        return {
            "status": "success",
            "email_accounts": len(dataset1),
            "campaigns": len(dataset2),
            "final_rows": len(final_dataset)
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
        # Continue anyway - database will be created on first use
    
    try:
        # Schedule daily job at 2 AM UTC (adjust as needed)
        scheduler.add_job(
            fetch_and_filter_data,
            CronTrigger(hour=2, minute=0),  # 2:00 AM daily
            id='daily_fetch',
            replace_existing=True
        )
        scheduler.start()
        logger.info("Scheduler started - daily job at 2:00 AM UTC")
    except Exception as e:
        logger.error(f"Scheduler initialization failed: {e}")
        # Continue anyway - app will still work without scheduler
    
    yield
    
    # Shutdown
    try:
        scheduler.shutdown()
        logger.info("Scheduler shut down")
    except Exception as e:
        logger.error(f"Scheduler shutdown failed: {e}")

# Initialize FastAPI app
app = FastAPI(
    title="Data Filter API",
    description="Daily data fetching and filtering service",
    version="1.0.0",
    lifespan=lifespan
)

@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "status": "running",
        "service": "Data Filter API",
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/filtered-data")
async def get_filtered_data():
    """
    Get the latest filtered data.
    This is the endpoint your next workflow step will call.
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
        logger.error(f"Error retrieving data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/refresh")
@app.post("/refresh")
async def manual_refresh():
    """
    Manually trigger data fetch and filter.
    Useful for testing or immediate updates.
    Works with both GET and POST requests.
    """
    result = await fetch_and_filter_data()
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

@app.get("/schedule-info")
async def schedule_info():
    """Get information about scheduled jobs"""
    jobs = scheduler.get_jobs()
    return {
        "scheduled_jobs": [
            {
                "id": job.id,
                "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
                "trigger": str(job.trigger)
            }
            for job in jobs
        ]
    }
