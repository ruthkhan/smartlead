# SmartLead Data Integration API - Setup Instructions

This is a FastAPI application that fetches email account and campaign data from SmartLead daily, performs data joins with intelligent filtering, and exposes it via an API endpoint.

## 📋 What This Does

- **Fetches SmartLead data** daily at 2 AM UTC (configurable):
  - Email accounts with warmup reputation and start dates
  - **Filters**: Only accounts with reputation <= 98% (need attention) and warming 2+ weeks
  - All campaigns with names and statuses
  - Campaign-to-email-account associations
- **Performs data joins** to create a unified dataset
- **Stores joined data** in SQLite database
- **Exposes API endpoint** at `/filtered-data` for your next workflow step
- **Runs in the cloud** on Railway's free tier

## 🎯 Built-in Smart Filtering

The application automatically filters to reduce API calls and focus on actionable accounts:
1. **Warmup Reputation Filter**: Keeps only accounts with reputation null or <= 98%
2. **Warmup Duration Filter**: Keeps only accounts warming for 2+ weeks (or null)

## 🚀 Step-by-Step Setup

### Step 1: Prepare Your Files

You should have these 3 files:
- `main.py` - The application code
- `requirements.txt` - Python dependencies
- `Procfile` - Deployment configuration

### Step 2: Create a GitHub Account (if you don't have one)

1. Go to https://github.com
2. Click "Sign up"
3. Follow the registration process
4. Verify your email

### Step 3: Create a GitHub Repository

1. Log into GitHub
2. Click the "+" icon in top right → "New repository"
3. Name it: `data-filter-api`
4. Make it **Public** (required for Railway free tier)
5. Do NOT initialize with README
6. Click "Create repository"

### Step 4: Upload Your Files to GitHub

**Option A: Using GitHub Web Interface (Easiest)**
1. On your new repository page, click "uploading an existing file"
2. Drag and drop all 3 files (`main.py`, `requirements.txt`, `Procfile`)
3. Scroll down and click "Commit changes"

**Option B: Using Git Command Line**
```bash
git init
git add main.py requirements.txt Procfile
git commit -m "Initial commit"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/data-filter-api.git
git push -u origin main
```

### Step 5: Create Railway Account

1. Go to https://railway.app
2. Click "Login"
3. Choose "Login with GitHub"
4. Authorize Railway to access your GitHub account
5. Complete any verification steps

### Step 6: Deploy to Railway

1. Click "New Project" on Railway dashboard
2. Select "Deploy from GitHub repo"
3. Choose your `data-filter-api` repository
4. Railway will automatically detect it's a Python app and start deploying
5. Wait 2-3 minutes for deployment to complete

### Step 7: Configure Your SmartLead API Key

**CRITICAL STEP - Required for the app to work!**

1. Get your SmartLead API key:
   - Log into SmartLead at https://app.smartlead.ai
   - Go to Settings → Your Profile
   - Copy your API key

2. In Railway, click on your deployed service
3. Go to the "Variables" tab
4. Click "New Variable"
5. Add:
   - Variable name: `SMARTLEAD_API_KEY`
   - Value: (paste your SmartLead API key)
6. Click "Add"
7. Your service will automatically redeploy with the new variable

### Step 8: Get Your API URL

1. In Railway, go to "Settings" tab
2. Scroll to "Networking"
3. Click "Generate Domain"
4. Copy your public URL (e.g., `https://data-filter-api-production.up.railway.app`)

### Step 9: Test Your Deployment

Open your browser and test these endpoints:

1. **Health check:**
   ```
   https://YOUR_URL.railway.app/
   ```

2. **Manual data fetch:**
   ```
   https://YOUR_URL.railway.app/refresh
   ```
   (Works with both GET and POST - you can visit this URL in your browser)

3. **Get filtered data:**
   ```
   https://YOUR_URL.railway.app/filtered-data
   ```

4. **View logs:**
   ```
   https://YOUR_URL.railway.app/logs
   ```

5. **API documentation:**
   ```
   https://YOUR_URL.railway.app/docs
   ```

## ⚙️ Customization

### Update Your SmartLead API Key

The application uses the `SMARTLEAD_API_KEY` environment variable. To update it:

1. Go to Railway dashboard → Your service → Variables
2. Update the `SMARTLEAD_API_KEY` value
3. Railway will automatically redeploy

### Add Filtering Logic (Optional)

If you want to filter the final dataset before storing it, edit `main.py` and add filtering logic after the joins are complete. For example, to only include email accounts with high warmup reputation:

```python
# After Dataset 5 is created, add:
filtered_dataset = [
    row for row in final_dataset 
    if row.get('warmup_reputation', 0) >= 80
]
```

### Change Schedule Time

In `main.py`, find this line:
```python
CronTrigger(hour=2, minute=0)  # 2:00 AM UTC
```

Change to your preferred time:
```python
CronTrigger(hour=14, minute=30)  # 2:30 PM UTC
```

### After Making Changes

1. Commit changes to GitHub
2. Railway automatically redeploys when you push to GitHub

## 📊 Using the API in Your Workflow

Your next workflow step should call:
```
GET https://YOUR_URL.railway.app/filtered-data
```

Response format:
```json
{
  "status": "success",
  "data": [
    {
      "email_account_id": 12345,
      "email_address": "[email protected]",
      "warmup_reputation": 85,
      "warmup_start_date": "2024-01-15T10:00:00Z",
      "campaign_id": 6789,
      "campaign_name": "Q1 Outreach Campaign",
      "campaign_status": "ACTIVE",
      "time_added_to_campaign": "2024-01-20T14:30:00Z"
    },
    {
      "email_account_id": 12346,
      "email_address": "[email protected]",
      "warmup_reputation": 92,
      "warmup_start_date": "2024-01-10T08:00:00Z",
      "campaign_id": null,
      "campaign_name": null,
      "campaign_status": null,
      "time_added_to_campaign": null
    }
  ],
  "fetched_at": "2024-01-25T02:00:00",
  "row_count": 2
}
```

**Note:** Email accounts not associated with any campaign will have `null` values for campaign fields.

## 🔍 Monitoring

- **View logs in Railway:** Click your service → "Logs" tab
- **Check fetch history:** `GET /logs`
- **Verify schedule:** `GET /schedule-info`

**Note:** The initial fetch makes individual API calls for each email account to get complete warmup details. This is normal and ensures data accuracy.

## 💾 Data Persistence

Railway provides persistent storage, so your SQLite database (`data.db`) will persist between deployments. Your data is safe.

## 🆓 Free Tier Limits

Railway free tier includes:
- 500 hours/month (always-on for one service)
- $5 credit/month
- Your usage should easily stay within free limits

## 🐛 Troubleshooting

**App not starting?**
- Check Railway logs for errors
- Ensure all files are in repository root
- Verify `Procfile` has correct content

**No data returned?**
- Make sure you set `SMARTLEAD_API_KEY` in Railway variables
- Call `/refresh` to manually trigger fetch
- Check `/logs` for error messages
- Verify your SmartLead API key is correct and active

**API Key error?**
- Get your key from: https://app.smartlead.ai/app/settings/profile
- Ensure it's set as an environment variable in Railway
- The variable name must be exactly `SMARTLEAD_API_KEY`

**Rate limiting?**
- SmartLead API limit: 60 requests per 60 seconds
- If you have many campaigns, the initial fetch might take time
- Check logs to see progress

**Need help?**
- Check Railway logs: Dashboard → Your Service → Logs
- View application logs: `GET /logs`
- Railway docs: https://docs.railway.app

## 📝 Next Steps

1. ✅ Deploy to Railway
2. ✅ Test endpoints
3. ✅ Customize API URL and filtering logic
4. ✅ Set appropriate schedule time
5. ✅ Integrate with your next workflow step

---

**Your API will be available at:** `https://YOUR_URL.railway.app`

**Main endpoint for your workflow:** `GET /filtered-data`
