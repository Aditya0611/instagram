# Fix SUPABASE_KEY Secret in GitHub

## The Problem

The workflow is failing because the `SUPABASE_KEY` secret is empty or not set in GitHub Actions.

Error message:
```
Supabase credentials are not configured
```

## Solution: Set the Secret in GitHub

### Step 1: Get Your Supabase Key

Your Supabase key is:
```
eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJucm5iYnhubXRhamp4c2Nhd3JjIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTY4MzI4OTYsImV4cCI6MjA3MjQwODg5Nn0.WMigmhXcYKYzZxjQFmn6p_Y9y8oNVjuo5YJ0-xzY4h4
```

### Step 2: Add/Update the Secret in GitHub

1. **Go to your repository**: https://github.com/Aditya0611/instagram

2. **Navigate to Secrets**:
   - Click **Settings** (top menu)
   - Click **Secrets and variables** → **Actions** (left sidebar)

3. **Add or Update SUPABASE_KEY**:
   - If `SUPABASE_KEY` exists: Click on it → **Update**
   - If it doesn't exist: Click **New repository secret**

4. **Enter the details**:
   - **Name**: `SUPABASE_KEY` (must be EXACT, no spaces, all caps)
   - **Secret**: Paste the key above
   - Click **Add secret** or **Update secret**

### Step 3: Verify All Secrets Are Set

Make sure these 4 secrets exist:
- ✅ `INSTAGRAM_USERNAME`
- ✅ `INSTAGRAM_PASSWORD`
- ✅ `SUPABASE_URL`
- ✅ `SUPABASE_KEY` ← **This one is missing/empty**

### Step 4: Re-run the Workflow

After setting the secret:
1. Go to **Actions** tab
2. Click **Instagram Trend Analyzer**
3. Click **Run workflow** → **Run workflow**
4. The workflow should now succeed!

## Important Notes

### Secret Naming Rules:
- ✅ `SUPABASE_KEY` - Correct
- ❌ `SUPABASE-KEY` - Wrong (hyphen not allowed)
- ❌ `SUPABASE KEY` - Wrong (space not allowed)
- ❌ `supabase_key` - Wrong (should be uppercase, though lowercase might work)

### Security:
- Never commit secrets to code
- Secrets are masked in logs (shown as `***`)
- Only repository owners/collaborators can see/edit secrets

## Verify It's Working

After setting the secret and re-running the workflow:
1. Check the workflow logs - should see "✅ Connected to Supabase"
2. Run: `python check_latest_data.py`
3. You should see new data with recent timestamps

