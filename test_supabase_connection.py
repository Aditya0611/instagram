"""
Test Supabase connection and verify data can be saved.
Run this to diagnose why data isn't appearing in Supabase.
"""
import os
import uuid
from supabase import create_client, Client

# Get credentials from environment or use defaults
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://rnrnbbxnmtajjxscawrc.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJucm5iYnhubXRhamp4c2Nhd3JjIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTY4MzI4OTYsImV4cCI6MjA3MjQwODg5Nn0.WMigmhXcYKYzZxjQFmn6p_Y9y8oNVjuo5YJ0-xzY4h4")

print("=" * 70)
print("🔍 SUPABASE CONNECTION TEST")
print("=" * 70)
print(f"\n📋 Supabase URL: {SUPABASE_URL}")
print(f"🔑 Key (first 20 chars): {SUPABASE_KEY[:20]}...")

try:
    # Create client
    print("\n[1] Creating Supabase client...")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("✅ Client created successfully")
    
    # Test connection by querying the instagram table
    print("\n[2] Testing connection by querying 'instagram' table...")
    try:
        result = supabase.table('instagram').select('*').limit(1).execute()
        print(f"✅ Connection successful! Found {len(result.data)} existing records")
        
        if result.data:
            print("\n📊 Sample record structure:")
            import json
            print(json.dumps(result.data[0], indent=2, default=str))
    except Exception as e:
        print(f"❌ Error querying table: {str(e)}")
        print("\n⚠️  Possible issues:")
        print("   - Table 'instagram' doesn't exist")
        print("   - Table has different column names")
        print("   - Permission issues with the API key")
        
    # Test inserting a sample record
    print("\n[3] Testing INSERT operation...")
    test_uuid = str(uuid.uuid4())
    test_payload = {
        "platform": "Instagram",
        "topic_hashtag": "#test_hashtag",
        "engagement_score": 1000.0,
        "sentiment_polarity": 0.5,
        "sentiment_label": "positive",
        "posts": 1,
        "views": 5000,
        "metadata": {
            "test": True,
            "created_by": "test_script"
        },
        "scraped_at": "2025-01-27T00:00:00",
        "version_id": test_uuid
    }
    print(f"   Using test UUID: {test_uuid}")
    
    try:
        insert_result = supabase.table('instagram').insert(test_payload).execute()
        if insert_result.data:
            print("✅ INSERT successful!")
            print(f"   Inserted record ID: {insert_result.data[0].get('id', 'N/A')}")
            
            # Try to delete the test record
            print("\n[4] Cleaning up test record...")
            try:
                delete_result = supabase.table('instagram').delete().eq('topic_hashtag', '#test_hashtag').execute()
                print("✅ Test record deleted")
            except Exception as e:
                print(f"⚠️  Could not delete test record: {str(e)}")
        else:
            print("❌ INSERT returned no data")
    except Exception as e:
        print(f"❌ INSERT failed: {str(e)}")
        print("\n⚠️  Possible issues:")
        print("   - Missing required columns in table")
        print("   - Data type mismatches")
        print("   - Constraint violations (e.g., unique constraint)")
        print("   - Permission issues")
        
    # Check table structure
    print("\n[5] Checking table structure...")
    try:
        # Try to get column info by selecting with limit 0 or checking error messages
        structure_check = supabase.table('instagram').select('*').limit(0).execute()
        print("✅ Table exists and is accessible")
    except Exception as e:
        print(f"⚠️  Could not verify table structure: {str(e)}")
        
except Exception as e:
    print(f"\n❌ Failed to create Supabase client: {str(e)}")
    print("\n⚠️  Possible issues:")
    print("   - Invalid Supabase URL")
    print("   - Invalid API key")
    print("   - Network connectivity issues")

print("\n" + "=" * 70)
print("✅ Test completed")
print("=" * 70)

