"""
Check the latest scraped data in Supabase.
Shows when data was last updated and helps identify if new data is being saved.
"""
import os
from supabase import create_client, Client
from datetime import datetime

# Get credentials from environment or use defaults
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://rnrnbbxnmtajjxscawrc.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJucm5iYnhubXRhamp4c2Nhd3JjIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTY4MzI4OTYsImV4cCI6MjA3MjQwODg5Nn0.WMigmhXcYKYzZxjQFmn6p_Y9y8oNVjuo5YJ0-xzY4h4")

print("=" * 70)
print("📊 CHECKING LATEST SCRAPED DATA IN SUPABASE")
print("=" * 70)

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # Get total count
    all_records = supabase.table('instagram').select('id', count='exact').execute()
    total_count = all_records.count if hasattr(all_records, 'count') else len(all_records.data)
    print(f"\n📈 Total records in database: {total_count}")
    
    # Get latest 10 records ordered by scraped_at
    print("\n🔍 Latest 10 records (ordered by scraped_at):")
    print("-" * 70)
    
    latest = supabase.table('instagram')\
        .select('topic_hashtag, engagement_score, posts, views, scraped_at, version_id')\
        .order('scraped_at', desc=True)\
        .limit(10)\
        .execute()
    
    if latest.data:
        for i, record in enumerate(latest.data, 1):
            hashtag = record.get('topic_hashtag', 'N/A')
            engagement = record.get('engagement_score') or 0
            posts = record.get('posts') or 0
            views = record.get('views') or 0
            scraped_at = record.get('scraped_at', 'N/A')
            version_id_raw = record.get('version_id')
            version_id = str(version_id_raw)[:8] + '...' if version_id_raw else 'N/A'
            
            # Parse and format the date
            try:
                if scraped_at and scraped_at != 'N/A':
                    dt = datetime.fromisoformat(scraped_at.replace('Z', '+00:00'))
                    time_ago = datetime.now(dt.tzinfo) - dt
                    hours_ago = time_ago.total_seconds() / 3600
                    if hours_ago < 24:
                        time_str = f"{hours_ago:.1f} hours ago"
                    else:
                        days_ago = hours_ago / 24
                        time_str = f"{days_ago:.1f} days ago"
                else:
                    time_str = "N/A"
            except:
                time_str = scraped_at if scraped_at else "N/A"
            
            print(f"\n[{i}] {hashtag}")
            print(f"    Engagement: {engagement:,.0f} | Posts: {posts} | Views: {views:,}")
            print(f"    Scraped: {scraped_at} ({time_str})")
            print(f"    Version: {version_id}")
    else:
        print("❌ No records found")
    
    # Get records grouped by date
    print("\n" + "=" * 70)
    print("📅 Records by date (last 7 days):")
    print("-" * 70)
    
    # Get all records to group by date
    all_data = supabase.table('instagram')\
        .select('scraped_at')\
        .order('scraped_at', desc=True)\
        .execute()
    
    if all_data.data:
        from collections import Counter
        dates = []
        for record in all_data.data:
            scraped_at = record.get('scraped_at')
            if scraped_at:
                try:
                    dt = datetime.fromisoformat(scraped_at.replace('Z', '+00:00'))
                    dates.append(dt.date().isoformat())
                except:
                    pass
        
        date_counts = Counter(dates)
        for date, count in sorted(date_counts.items(), reverse=True)[:7]:
            print(f"  {date}: {count} record(s)")
    else:
        print("  No records found")
    
    # Check for unique version_ids (each scraper run should have a unique version_id)
    print("\n" + "=" * 70)
    print("🆔 Unique version IDs (each represents a scraper run):")
    print("-" * 70)
    
    versions = supabase.table('instagram')\
        .select('version_id')\
        .execute()
    
    if versions.data:
        unique_versions = set()
        for record in versions.data:
            vid = record.get('version_id')
            if vid:
                unique_versions.add(vid)
        
        print(f"  Found {len(unique_versions)} unique scraper runs")
        print(f"  Latest version IDs:")
        for vid in list(unique_versions)[:5]:
            print(f"    - {vid}")
    else:
        print("  No version IDs found")
    
    print("\n" + "=" * 70)
    print("💡 TIP: If you don't see recent data, check:")
    print("   1. GitHub Actions workflow status")
    print("   2. Workflow logs for errors")
    print("   3. Ensure secrets are configured correctly")
    print("=" * 70)
    
except Exception as e:
    print(f"\n❌ Error: {str(e)}")
    import traceback
    traceback.print_exc()

