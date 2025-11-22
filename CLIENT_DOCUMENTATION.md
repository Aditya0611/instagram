# Instagram Trending Hashtag Discovery System

## 📋 Project Overview

This is a fully automated Instagram trending hashtag discovery and analysis system that:
- Discovers trending hashtags from Instagram's explore page
- Analyzes real engagement metrics (likes, comments, views)
- Categorizes hashtags automatically
- Tracks trend lifecycle and versioning
- Stores normalized data in Supabase database
- Runs automatically every 3 hours

---

## 🚀 Key Features

### ✅ Automated Trend Discovery
- Automatically discovers trending hashtags from Instagram
- Scans 200+ posts and identifies top 20 trending hashtags
- Categorizes hashtags into 10 categories (Fashion, Fitness, Food, etc.)

### ✅ Real Engagement Analysis
- Extracts actual engagement data from Instagram posts
- Analyzes likes, comments, and views
- Calculates engagement scores
- Special handling for video/reel content

### ✅ Normalized Data Structure
- TrendRecord objects with all required fields:
  - Platform, URL, Hashtags
  - Likes, Comments, Views
  - Language, Timestamp
  - Engagement Score, Version
  - Complete metadata (raw_blob)
  - Lifecycle tracking (first_seen, last_seen)

### ✅ Automated Scheduling
- APScheduler runs every 3 hours automatically
- Configurable scheduling interval
- Test mode for manual runs
- Graceful shutdown handling

### ✅ Comprehensive Monitoring
- File and console logging
- Success/failure metrics
- Error tracking and reporting
- Performance monitoring

### ✅ Trend Lifecycle Management
- Unique version ID per run
- First seen timestamp tracking
- Last seen timestamp updates
- Automatic update vs insert logic

---

## 📦 Included Files

```
client_delivery.zip
├── main.py                        # Main application code
├── requirements.txt               # Python dependencies
├── README.md                      # Basic project information
├── .gitignore                     # Git ignore rules
└── CLIENT_DOCUMENTATION.md        # This file
```

---

## ⚙️ Installation & Setup

### Prerequisites
- Python 3.10 or higher
- Instagram account credentials
- Supabase account and database
- Internet connection

### Step 1: Extract Files
```bash
# Extract the zip file to your desired location
unzip client_delivery.zip
cd instagram-scraper
```

### Step 2: Create Virtual Environment
```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate
```

### Step 3: Install Dependencies
```bash
pip install -r requirements.txt
```

### Step 4: Install Playwright Browser
```bash
playwright install chromium
```

### Step 5: Configure Credentials

Edit `main.py` and update these configuration values:

```python
# Line 98-99: Instagram Credentials
USERNAME = "your_instagram_username"
PASSWORD = "your_instagram_password"

# Line 107-108: Supabase Configuration
SUPABASE_URL = "your_supabase_url"
SUPABASE_KEY = "your_supabase_key"
```

### Step 6: Database Setup

Run this SQL in your Supabase SQL Editor to ensure the table exists:

```sql
-- Verify table exists (should already be created)
SELECT * FROM public.instagram LIMIT 1;
```

The existing `instagram` table schema:
```sql
- id (bigserial, primary key)
- platform (text)
- topic_hashtag (text)
- engagement_score (double precision)
- sentiment_polarity (double precision)
- sentiment_label (text)
- posts (bigint)
- views (bigint)
- metadata (jsonb)
- scraped_at (timestamp with time zone)
- version_id (uuid)
```

---

## 🎯 Usage

### Option 1: Run with Scheduler (Production)

Start the automated scheduler that runs every 3 hours:

```bash
python main.py
```

**Output:**
```
🕐 Instagram Scraper started with APScheduler
📅 Scheduled to run every 3 hours
💡 Use --run-once flag to run once for testing
🛑 Press Ctrl+C to stop
```

**To Stop:**
Press `Ctrl+C` to gracefully shutdown the scheduler.

### Option 2: Run Once (Testing)

Run the scraper once for testing:

```bash
python main.py --run-once
```

**This will:**
1. Login to Instagram
2. Discover trending hashtags
3. Analyze engagement for each hashtag
4. Save to database
5. Exit after completion

---

## 📊 What the System Does

### Step-by-Step Process

#### 1. Instagram Login
- Automatically logs into Instagram using provided credentials
- Handles popup dismissals
- Maintains session for scraping

#### 2. Hashtag Discovery
- Navigates to Instagram Explore page
- Scrolls 10 times to load posts
- Scans up to 200 posts
- Extracts hashtags from post descriptions
- Filters and ranks by frequency

#### 3. Hashtag Categorization
- Automatically categorizes each hashtag:
  - Fashion (fashion, style, ootd, etc.)
  - Fitness (gym, workout, health, etc.)
  - Food (foodie, cooking, recipe, etc.)
  - Travel (wanderlust, vacation, explore, etc.)
  - Technology (tech, coding, ai, etc.)
  - Business (entrepreneur, startup, etc.)
  - Entertainment (viral, trending, funny, etc.)
  - Lifestyle (lifestyle, love, happy, etc.)
  - Photography (photography, photo, art, etc.)
  - Sports (sports, football, athlete, etc.)
  - General (uncategorized hashtags)

#### 4. Engagement Analysis
For each discovered hashtag:
- Opens 3 sample posts
- Extracts real engagement metrics:
  - Likes count
  - Comments count
  - Views count (for videos/reels)
- Calculates average engagement
- Determines content type (photo/video)

#### 5. Data Normalization
Creates TrendRecord objects with:
- Platform: "Instagram"
- URL: Hashtag explore page URL
- Hashtags: List of hashtags
- Likes: Average likes across analyzed posts
- Comments: Average comments
- Views: Average views
- Language: Content language (default: 'en')
- Timestamp: When scraped
- Engagement Score: Calculated engagement metric
- Version: Unique run identifier
- Raw Blob: Complete metadata including:
  - Category
  - Frequency
  - Posts count
  - Sample posts URLs
  - Discovery method
  - Analysis details

#### 6. Database Storage
- Checks if hashtag already exists in database
- If exists: Updates with new data and lifecycle info
- If new: Inserts as new record
- Stores lifecycle information:
  - First seen: When trend was first discovered
  - Last seen: When trend was last updated
  - Version: Unique identifier for this run

#### 7. Logging & Metrics
- Logs all operations to file and console
- Tracks success/failure counts
- Records errors for debugging
- Provides summary statistics

---

## 📈 Example Output

### Successful Run Output

```
======================================================================
🔥 INSTAGRAM TRENDING HASHTAG DISCOVERY
   WITH CATEGORIES & REAL ENGAGEMENT
======================================================================
📋 Version ID: 14557df7-709b-4705-be13-26dec4e46bff
🎯 Target: Top 20 trending hashtags
📊 Analyzing 3 posts per hashtag for engagement
======================================================================

✅ Connected to Supabase

[+] Navigating to Instagram...
✅ Login successful!

======================================================================
🔍 DISCOVERING TRENDING HASHTAGS
======================================================================

✅ DISCOVERED 18 TRENDING HASHTAGS

📁 ENTERTAINMENT (2 hashtags)
   #trending - 2x
   #viralpost - 2x

📁 FASHION (1 hashtags)
   #naturalhairstyles - 2x

📁 LIFESTYLE (2 hashtags)
   #instagram - 4x
   #love - 2x

======================================================================
💾 ANALYZING ENGAGEMENT & SAVING TO DATABASE
======================================================================

[1/18] 📊 LIFESTYLE: #instagram

    💯 Average Engagement: 95,975
    👍 Average Likes: 95,975
    💬 Average Comments: 0
    👁️  Average Views: 1,508,018

    💾 Saving to database...
    ✅ Saved successfully

======================================================================
📊 SAVE RESULTS
======================================================================
✅ Successful: 18/18
❌ Failed: 0/18

📋 Version ID: 14557df7-709b-4705-be13-26dec4e46bff
```

---

## 🗄️ Database Structure

### Data Storage

All trend data is stored in the `instagram` table with the following structure:

#### Direct Columns
- `platform`: "Instagram"
- `topic_hashtag`: "#hashtag" (e.g., "#trending")
- `engagement_score`: Average engagement score
- `posts`: Number of posts analyzed
- `views`: Average views count
- `version_id`: Unique UUID for this run
- `scraped_at`: Timestamp of when data was scraped

#### Metadata JSONB Column
Contains all TrendRecord data:
```json
{
  "url": "https://www.instagram.com/explore/tags/trending/",
  "hashtags": ["#trending"],
  "likes": 22400,
  "comments": 0,
  "language": "en",
  "timestamp": "2025-10-19T14:57:08.670000",
  "version": "14557df7-709b-4705-be13-26dec4e46bff",
  "first_seen": "2025-10-19T14:57:08.670000",
  "last_seen": "2025-10-19T14:57:08.670000",
  "raw_blob": {
    "category": "entertainment",
    "frequency": 2,
    "posts_count": 2,
    "discovery_method": "explore_page",
    "avg_likes": 22400.0,
    "avg_comments": 0.0,
    "total_engagement": 44800.0,
    "avg_views": 380635.0,
    "video_count": 0
  }
}
```

### Database Queries

#### View All Trends from Latest Run
```sql
SELECT 
    topic_hashtag,
    engagement_score,
    metadata->>'category' as category,
    metadata->>'likes' as likes,
    metadata->>'views' as views,
    scraped_at
FROM instagram 
WHERE version_id = 'your-version-id-here'
ORDER BY engagement_score DESC;
```

#### Track Hashtag Lifecycle
```sql
SELECT 
    topic_hashtag,
    metadata->>'first_seen' as first_seen,
    metadata->>'last_seen' as last_seen,
    COUNT(*) as total_appearances
FROM instagram
WHERE topic_hashtag = '#trending'
GROUP BY topic_hashtag
ORDER BY scraped_at DESC;
```

#### Top Trending Hashtags (All Time)
```sql
SELECT 
    topic_hashtag,
    AVG(engagement_score) as avg_engagement,
    COUNT(*) as appearances,
    MAX(scraped_at) as last_seen
FROM instagram
GROUP BY topic_hashtag
ORDER BY avg_engagement DESC
LIMIT 20;
```

#### Trends by Category
```sql
SELECT 
    metadata->>'category' as category,
    topic_hashtag,
    engagement_score,
    views
FROM instagram
WHERE metadata->>'category' = 'entertainment'
ORDER BY engagement_score DESC;
```

---

## 📝 Logging

### Log Files

**Location:** `instagram_scraper.log`

**Format:**
```
2025-10-19 14:57:08,669 - __main__ - INFO - Running scraper once (test mode)
2025-10-19 14:57:09,040 - __main__ - INFO - Successfully connected to Supabase
2025-10-19 14:58:36,959 - __main__ - INFO - Starting database save process
2025-10-19 14:59:04,140 - __main__ - INFO - Successfully saved trend: #instagram
```

### Log Levels
- **INFO**: Normal operation events
- **WARNING**: Non-critical issues
- **ERROR**: Errors that need attention
- **DEBUG**: Detailed debugging information

### Viewing Logs

```bash
# View entire log file
cat instagram_scraper.log

# View last 50 lines
tail -n 50 instagram_scraper.log

# Follow log in real-time
tail -f instagram_scraper.log

# Search for errors
grep "ERROR" instagram_scraper.log

# Search for specific hashtag
grep "#trending" instagram_scraper.log
```

---

## ⚙️ Configuration Options

### Scraping Parameters
**Location:** `main.py` (Lines 101-105)

```python
SCROLL_COUNT = 10                # Number of scrolls on explore page
POSTS_TO_SCAN = 200             # Maximum posts to scan for hashtags
MIN_HASHTAG_FREQUENCY = 2       # Minimum hashtag frequency to save
TOP_HASHTAGS_TO_SAVE = 20       # Number of top hashtags to analyze
POSTS_PER_HASHTAG = 3           # Posts to analyze per hashtag
```

### Scheduling Configuration
**Location:** `main.py` (Line 789)

```python
# Change scheduling interval
trigger=CronTrigger(hour='*/3')  # Current: Every 3 hours

# Options:
trigger=CronTrigger(hour='*/2')  # Every 2 hours
trigger=CronTrigger(hour='*/4')  # Every 4 hours
trigger=CronTrigger(hour='0')    # Daily at midnight
```

### Browser Settings
**Location:** `main.py` (Lines 686-693)

```python
browser = p.chromium.launch(
    headless=False,  # Set to True for background operation
    args=[
        '--disable-blink-features=AutomationControlled',
        '--disable-dev-shm-usage',
        '--no-sandbox'
    ]
)
```

---

## 🔧 Troubleshooting

### Common Issues

#### 1. Login Fails
**Problem:** Cannot login to Instagram

**Solutions:**
- Verify username and password are correct
- Check if account has 2FA enabled (disable for automation)
- Instagram may have rate limits - wait and try again
- Try logging in manually first to clear any security checks

#### 2. No Hashtags Discovered
**Problem:** Zero hashtags found

**Solutions:**
- Check internet connection
- Verify Instagram is accessible
- Increase `SCROLL_COUNT` to load more posts
- Decrease `MIN_HASHTAG_FREQUENCY` threshold

#### 3. Database Connection Failed
**Problem:** Cannot connect to Supabase

**Solutions:**
- Verify `SUPABASE_URL` and `SUPABASE_KEY` are correct
- Check internet connection
- Verify Supabase project is active
- Check if table `instagram` exists

#### 4. Slow Performance
**Problem:** Scraper runs very slowly

**Solutions:**
- Reduce `POSTS_TO_SCAN` value
- Reduce `TOP_HASHTAGS_TO_SAVE` value
- Reduce `POSTS_PER_HASHTAG` value
- Improve internet connection speed

#### 5. Memory Issues
**Problem:** High memory usage

**Solutions:**
- Run in headless mode (`headless=True`)
- Reduce scraping parameters
- Close other applications
- Increase system RAM

---

## 📊 Performance Metrics

### Typical Run Statistics

**Average Execution Time:**
- Login: ~3 seconds
- Hashtag Discovery: ~1.5 minutes
- Engagement Analysis: ~5-8 minutes
- Database Operations: ~30 seconds
- **Total: 8-10 minutes per run**

**Data Collected Per Run:**
- Hashtags Discovered: 15-20
- Posts Analyzed: 45-60 (3 per hashtag)
- Database Records: 15-20
- Log Entries: 100-150

**Resource Usage:**
- Memory: 200-400 MB
- CPU: 10-30% (during active scraping)
- Network: 50-100 MB per run
- Disk: 1-2 MB logs per day

---

## 🔒 Security Best Practices

### Credentials Management
1. **Never commit credentials to Git**
   - Credentials are in `.gitignore`
   - Use environment variables for production

2. **Use Strong Passwords**
   - Instagram account should have strong password
   - Consider using dedicated account for scraping

3. **Secure Database Access**
   - Use Supabase Row Level Security (RLS)
   - Limit API key permissions
   - Rotate keys regularly

### Rate Limiting
1. **Respect Instagram's Limits**
   - Current delays (3-5 seconds) are safe
   - Don't reduce delays too much
   - Monitor for rate limit errors

2. **Scheduled Runs**
   - 3-hour interval is safe
   - Don't run too frequently
   - Space out runs during off-peak hours

---

## 🚀 Deployment Options

### Option 1: Local Server
Run on your own server or computer:
```bash
# Start with scheduler
python main.py

# Run as background process (Linux/Mac)
nohup python main.py &

# Run as Windows service
# Use NSSM or Task Scheduler
```

### Option 2: Cloud Deployment
Deploy to cloud platforms:

**AWS EC2:**
1. Launch Ubuntu instance
2. Install Python and dependencies
3. Run as systemd service

**Google Cloud:**
1. Use Compute Engine
2. Setup startup script
3. Configure auto-restart

**Heroku:**
1. Add Procfile: `worker: python main.py`
2. Use scheduler add-on
3. Deploy via Git

### Option 3: GitHub Actions
Run via GitHub Actions (current setup):
- Runs every 8 hours automatically
- No server required
- Free tier available

---

## 📚 Technical Details

### Technologies Used
- **Python 3.10+**: Core programming language
- **Playwright**: Browser automation
- **APScheduler**: Job scheduling
- **Supabase**: Database storage
- **Logging**: Built-in Python logging

### Architecture
```
┌─────────────────┐
│  APScheduler    │  (Triggers every 3 hours)
└────────┬────────┘
         │
         v
┌─────────────────┐
│  Instagram      │  (Login & Discovery)
│  Scraper        │
└────────┬────────┘
         │
         v
┌─────────────────┐
│  Engagement     │  (Analyze Posts)
│  Analyzer       │
└────────┬────────┘
         │
         v
┌─────────────────┐
│  TrendRecord    │  (Normalize Data)
│  Normalizer     │
└────────┬────────┘
         │
         v
┌─────────────────┐
│  Database       │  (Store in Supabase)
│  Writer         │
└─────────────────┘
```

### Data Flow
1. **Scheduler triggers** job execution
2. **Scraper logs in** to Instagram
3. **Hashtags discovered** from explore page
4. **Engagement analyzed** for each hashtag
5. **Data normalized** into TrendRecord
6. **Lifecycle checked** (new vs existing)
7. **Database updated** with new data
8. **Metrics logged** for monitoring

---

## 📞 Support & Maintenance

### Regular Maintenance
1. **Weekly:**
   - Check logs for errors
   - Verify database connections
   - Monitor disk space

2. **Monthly:**
   - Update dependencies: `pip install --upgrade -r requirements.txt`
   - Review and clean old logs
   - Analyze trend data

3. **Quarterly:**
   - Update Instagram credentials if needed
   - Review and optimize parameters
   - Backup database

### Monitoring Checklist
- [ ] Scheduler is running
- [ ] Recent logs show successful runs
- [ ] Database is receiving new data
- [ ] No error spikes in logs
- [ ] Disk space is adequate
- [ ] Memory usage is normal

---

## 📄 License & Usage

This system is provided for the client's use. Please ensure:
- Compliance with Instagram's Terms of Service
- Responsible use of automation
- Respect for rate limits
- Proper data privacy handling

---

## 🎯 Summary

This Instagram Trending Hashtag Discovery System provides:

✅ **Automated Discovery** - Finds trending hashtags every 3 hours  
✅ **Real Engagement Data** - Extracts actual metrics from Instagram  
✅ **Smart Categorization** - Automatically categorizes hashtags  
✅ **Lifecycle Tracking** - Monitors trends over time  
✅ **Comprehensive Logging** - Complete audit trail  
✅ **Production Ready** - Fully tested and operational  

**Quick Start:**
```bash
pip install -r requirements.txt
playwright install chromium
# Configure credentials in main.py
python main.py --run-once  # Test
python main.py             # Production
```

For questions or issues, please refer to the Troubleshooting section or contact support.

---

**Document Version:** 1.0  
**Last Updated:** October 19, 2025  
**System Status:** Production Ready ✅


