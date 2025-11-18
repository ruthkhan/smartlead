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
    Fetch data from API and filter it according to your criteria.
    Modify this function with your actual API endpoint and filtering logic.
    """
    logger.info("Starting data fetch and filter job...")
    
    try:
        # REPLACE THIS URL with your actual API endpoint
        api_url = "https://api.example.com/data"
        
        # Example: Fetch data from API
        async with httpx.AsyncClient(timeout=30.0) as client:
            # Add any headers or authentication here
            # headers = {"Authorization": "Bearer YOUR_TOKEN"}
            response = await client.get(api_url)
            response.raise_for_status()
            raw_data = response.json()
        
        # REPLACE THIS with your actual filtering logic
        # Example filtering: keep only rows where 'status' == 'active'
        filtered_data = []
        
        # Assuming raw_data is a list of dictionaries
        if isinstance(raw_data, list):
            for row in raw_data:
                # Add your filtering criteria here
                # Example: if row.get('status') == 'active':
                filtered_data.append(row)
        else:
            # If API returns a different format, adjust accordingly
            filtered_data = raw_data
        
        # Store filtered data in database
        conn = sqlite3.connect('data.db')
        cursor = conn.cursor()
        
        # Clear old data (keep only latest)
        cursor.execute('DELETE FROM filtered_data')
        
        # Insert new data
        cursor.execute(
            'INSERT INTO filtered_data (data) VALUES (?)',
            (json.dumps(filtered_data),)
        )
        
        # Log successful fetch
        cursor.execute(
            'INSERT INTO fetch_log (status, message) VALUES (?, ?)',
            ('success', f'Fetched and filtered {len(filtered_data)} rows')
        )
        
        conn.commit()
        conn.close()
        
        logger.info(f"Successfully filtered {len(filtered_data)} rows")
        return {"status": "success", "rows_filtered": len(filtered_data)}
        
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
    init_db()
    logger.info("Database initialized")
    
    # Schedule daily job at 2 AM UTC (adjust as needed)
    scheduler.add_job(
        fetch_and_filter_data,
        CronTrigger(hour=2, minute=0),  # 2:00 AM daily
        id='daily_fetch',
        replace_existing=True
    )
    scheduler.start()
    logger.info("Scheduler started - daily job at 2:00 AM UTC")
    
    yield
    
    # Shutdown
    scheduler.shutdown()
    logger.info("Scheduler shut down")

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

@app.post("/refresh")
async def manual_refresh():
    """
    Manually trigger data fetch and filter.
    Useful for testing or immediate updates.
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