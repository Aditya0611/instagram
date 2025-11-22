# Troubleshooting Guide: Data Not Appearing in Supabase

If you can't see the latest scraped data in Supabase, follow these steps:

## 1. Check GitHub Actions Workflow Status

1. Go to your repository: https://github.com/Aditya0611/instagram
2. Click on the **Actions** tab
3. Check if the workflow runs are:
   - ✅ **Green (Success)**: Workflow completed successfully
   - ❌ **Red (Failed)**: Workflow failed - check the logs
   - ⏸️ **Yellow (In Progress)**: Still running

## 2. Verify GitHub Secrets Are Set

The workflow requires these secrets to be configured:

1. Go to **Settings** → **Secrets and variables** → **Actions**
2. Verify these secrets exist (names must be EXACT):
   - `INSTAGRAM_USERNAME`
   - `INSTAGRAM_PASSWORD`
   - `SUPABASE_URL`
   - `SUPABASE_KEY`

**Important**: Secret names must:
- Only contain letters, numbers, and underscores
- Start with a letter or underscore
- No spaces, hyphens, or special characters

## 3. Test Supabase Connection Locally

Run the test script to verify your Supabase connection:

```bash
python test_supabase_connection.py
```

This will:
- Test the connection
- Verify the table exists
- Test INSERT operation
- Show any errors

## 4. Check Supabase Table Structure

The scraper saves data to a table called `instagram` with these columns:

**Required Columns:**
- `id` (auto-generated)
- `platform` (TEXT)
- `topic_hashtag` (TEXT) - e.g., "#fashion"
- `engagement_score` (FLOAT)
- `sentiment_polarity` (FLOAT)
- `sentiment_label` (TEXT)
- `posts` (INTEGER)
- `views` (INTEGER)
- `metadata` (JSONB)
- `scraped_at` (TIMESTAMPTZ)
- `version_id` (TEXT)

**Create the table if it doesn't exist:**

```sql
CREATE TABLE instagram (
  id BIGSERIAL PRIMARY KEY,
  platform TEXT NOT NULL,
  topic_hashtag TEXT NOT NULL,
  engagement_score FLOAT,
  sentiment_polarity FLOAT DEFAULT 0.0,
  sentiment_label TEXT DEFAULT 'neutral',
  posts INTEGER DEFAULT 0,
  views INTEGER DEFAULT 0,
  metadata JSONB DEFAULT '{}'::jsonb,
  scraped_at TIMESTAMPTZ DEFAULT NOW(),
  version_id TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create index for faster lookups
CREATE INDEX idx_instagram_topic_hashtag ON instagram(topic_hashtag);
CREATE INDEX idx_instagram_scraped_at ON instagram(scraped_at DESC);
```

## 5. Check Workflow Logs

1. Go to **Actions** → Click on the latest workflow run
2. Expand the **Run Instagram Scraper** step
3. Look for:
   - ✅ "Successfully saved trend" messages
   - ❌ Error messages
   - Database connection errors

## 6. Verify Data in Supabase

1. Go to your Supabase dashboard
2. Navigate to **Table Editor**
3. Select the `instagram` table
4. Check:
   - Are there any rows?
   - What's the latest `scraped_at` timestamp?
   - Are the `topic_hashtag` values correct?

## 7. Common Issues and Solutions

### Issue: "Table 'instagram' does not exist"
**Solution**: Create the table using the SQL above

### Issue: "Permission denied" or "Unauthorized"
**Solution**: 
- Check your `SUPABASE_KEY` is correct
- Ensure the key has INSERT/UPDATE permissions
- Use the `anon` key (not the `service_role` key unless needed)

### Issue: "Column does not exist"
**Solution**: 
- Check your table structure matches the required columns
- The code expects exact column names (case-sensitive in some databases)

### Issue: Workflow runs but no data appears
**Solution**:
- Check the workflow logs for "Failed to save trend" messages
- Verify the scraper is actually finding hashtags
- Check if login is successful

### Issue: Data appears but is old
**Solution**:
- The scraper updates existing records instead of creating duplicates
- Check `scraped_at` and `version_id` fields to see latest updates
- Query: `SELECT * FROM instagram ORDER BY scraped_at DESC LIMIT 10;`

## 8. Manual Test

Run the scraper locally to test:

```bash
# Set environment variables
export INSTAGRAM_USERNAME="your_username"
export INSTAGRAM_PASSWORD="your_password"
export SUPABASE_URL="your_supabase_url"
export SUPABASE_KEY="your_supabase_key"

# Run the scraper
python main.py --run-once
```

## 9. Query Latest Data

Use this SQL query in Supabase to see the latest data:

```sql
-- Get latest scraped data
SELECT 
  topic_hashtag,
  engagement_score,
  posts,
  views,
  scraped_at,
  version_id
FROM instagram
ORDER BY scraped_at DESC
LIMIT 20;

-- Count records by date
SELECT 
  DATE(scraped_at) as date,
  COUNT(*) as count
FROM instagram
GROUP BY DATE(scraped_at)
ORDER BY date DESC;
```

## 10. Enable Row Level Security (RLS)

If RLS is enabled on your table, you may need to create policies:

```sql
-- Allow anonymous inserts (for the scraper)
CREATE POLICY "Allow anonymous inserts" ON instagram
  FOR INSERT
  TO anon
  WITH CHECK (true);

-- Allow anonymous selects
CREATE POLICY "Allow anonymous selects" ON instagram
  FOR SELECT
  TO anon
  USING (true);

-- Allow anonymous updates
CREATE POLICY "Allow anonymous updates" ON instagram
  FOR UPDATE
  TO anon
  USING (true);
```

## Still Having Issues?

1. Check the `instagram_scraper.log` file (if running locally)
2. Review GitHub Actions workflow logs
3. Run `test_supabase_connection.py` to diagnose connection issues
4. Verify all secrets are set correctly in GitHub

