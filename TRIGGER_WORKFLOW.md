# How to Manually Trigger the Instagram Scraper Workflow

## Quick Steps

1. **Go to GitHub Actions**
   - Visit: https://github.com/Aditya0611/instagram/actions

2. **Find the Workflow**
   - Look for "Instagram Trend Analyzer" in the left sidebar
   - Click on it

3. **Trigger Manually**
   - Click the "Run workflow" dropdown button (top right)
   - Select branch: `main`
   - Click the green "Run workflow" button

4. **Monitor the Run**
   - The workflow will appear in the list
   - Click on it to see progress
   - It should take 5-15 minutes to complete

5. **Check Results**
   - After completion, run: `python check_latest_data.py`
   - You should see a new `scraped_at` timestamp
   - A new `version_id` will appear

## What to Look For

### ✅ Success Indicators:
- All steps complete with green checkmarks
- "Run Instagram Scraper" step shows success
- No error messages in logs

### ❌ Failure Indicators:
- Red X marks on any step
- Error messages in the logs
- Timeout errors

## Common Issues

### Issue: "Secrets not found"
**Solution**: Go to Settings → Secrets and variables → Actions
- Verify all 4 secrets exist:
  - `INSTAGRAM_USERNAME`
  - `INSTAGRAM_PASSWORD`
  - `SUPABASE_URL`
  - `SUPABASE_KEY`

### Issue: "Login failed"
**Solution**: Check Instagram credentials
- Verify username/password are correct
- Instagram might require 2FA or have blocked the account

### Issue: "Workflow timeout"
**Solution**: Increase timeout in workflow file
- Current timeout: 45 minutes
- Can be increased if needed

## After Manual Trigger

Once you manually trigger the workflow:
1. **It should reactivate scheduled runs** - GitHub will resume the 4-hour schedule
2. **Check in 4-8 hours** - Run `python check_latest_data.py` again
3. **You should see new data** - Latest `scraped_at` should be recent

## Verify It's Working

Run this command after triggering:
```bash
python check_latest_data.py
```

Look for:
- ✅ New `scraped_at` timestamp (within last hour)
- ✅ New `version_id` in the list
- ✅ Updated engagement scores

