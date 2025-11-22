# GitHub Actions Workflow Status

## Current Situation

Based on the data check:
- **Last successful scrape**: November 15, 2025 (7 days ago)
- **Total scraper runs**: 14 unique version IDs
- **Workflow schedule**: Every 4 hours (should have run ~42 times since last scrape)

## Why the Workflow Might Not Be Running

### 1. **GitHub Actions Scheduled Workflows Limitations**
   - Scheduled workflows only run if the repository has been active (at least one push in the last 60 days)
   - If the repository is inactive, scheduled workflows are paused
   - **Solution**: Make a commit/push to reactivate scheduled workflows

### 2. **Workflow File Location**
   - The workflow file must be in `.github/workflows/` on the **default branch** (main)
   - ✅ Your workflow is in the correct location

### 3. **GitHub Actions Must Be Enabled**
   - Go to: Settings → Actions → General
   - Ensure "Allow all actions and reusable workflows" is enabled

### 4. **Secrets Must Be Configured**
   - All 4 secrets must be set:
     - `INSTAGRAM_USERNAME`
     - `INSTAGRAM_PASSWORD`
     - `SUPABASE_URL`
     - `SUPABASE_KEY`

## How to Fix

### Step 1: Manually Trigger the Workflow (Test)
1. Go to: https://github.com/Aditya0611/instagram/actions
2. Click on "Instagram Trend Analyzer" workflow
3. Click "Run workflow" button (top right)
4. Select branch: `main`
5. Click "Run workflow"
6. Watch the run to see if it succeeds

### Step 2: Reactivate Scheduled Workflows
Make a small commit to reactivate scheduled workflows:

```bash
# Make a small change (like updating this file)
git add .
git commit -m "Reactivate scheduled workflows"
git push origin main
```

### Step 3: Verify Workflow Runs
After pushing:
1. Wait a few minutes
2. Go to Actions tab
3. Check if new workflow runs appear
4. The next scheduled run should be within 4 hours

### Step 4: Check Workflow Logs
If the workflow runs but fails:
1. Click on the failed workflow run
2. Expand each step to see errors
3. Common issues:
   - Login failures (Instagram credentials)
   - Timeout errors (increase timeout)
   - Missing dependencies

## Expected Behavior

Once working correctly:
- Workflow runs every 4 hours automatically
- Each run creates a new `version_id` (UUID)
- Updates existing records or creates new ones
- You should see new `scraped_at` timestamps every 4 hours

## Monitoring

Run this command periodically to check for new data:
```bash
python check_latest_data.py
```

Look for:
- Recent `scraped_at` timestamps (within last 4-8 hours)
- New `version_id` values
- Updated engagement scores

## Troubleshooting

### If workflow doesn't run at all:
1. Check repository activity (make a commit)
2. Verify GitHub Actions is enabled
3. Check if you're on a free plan (scheduled workflows work on free plan)

### If workflow runs but fails:
1. Check the logs in GitHub Actions
2. Verify all secrets are set correctly
3. Test locally: `python main.py --run-once`

### If workflow runs but no data appears:
1. Check Supabase connection: `python test_supabase_connection.py`
2. Verify table structure matches expected schema
3. Check for error messages in workflow logs

