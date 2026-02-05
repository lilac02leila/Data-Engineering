"""
Debug script to inspect the actual structure of your data
Run this to see what fields your data actually has
"""

import json
import os

def check_file_exists(filepath):
    """Check if a file exists and its size"""
    if os.path.exists(filepath):
        size = os.path.getsize(filepath)
        print(f"  ✓ File exists ({size:,} bytes)")
        if size == 0:
            print(f"  ⚠️  WARNING: File is EMPTY!")
        return True, size
    else:
        print(f"  ✗ File not found!")
        return False, 0

print("DATA STRUCTURE INSPECTOR")

# Check if files exist
print("\n--- CHECKING FILES ---")
print("Apps metadata file:")
apps_exists, apps_size = check_file_exists('data/raw/apps_metadata.json')
print("\nReviews file:")
reviews_exists, reviews_size = check_file_exists('data/raw/apps_reviews.jsonl')

if not apps_exists or not reviews_exists:
    print("\n  Some data files are missing!")
    print("Make sure you've run Step 1 (data ingestion) first.")
    print("\nRun: python src/1_data_ingestion.py")
    exit(1)

if reviews_size == 0:
    print(" PROBLEM FOUND: Reviews file is EMPTY!")
    print("\nThis means Step 1 (data ingestion) didn't collect any reviews.")
    print("\nPossible reasons:")
    print("  1. The apps you found have no reviews yet")
    print("  2. Google Play Scraper couldn't access the reviews")
    print("  3. There was an API rate limiting issue")
    print("\nSolutions:")
    print("  1. Re-run Step 1: python src/1_data_ingestion.py")
    print("  2. Try again after waiting 5-10 minutes")
    print("  3. Search for more popular apps (they usually have reviews)")
    print("  4. Use the provided test data files (see lab instructions)")

# Inspect apps data
print("--- APPS METADATA STRUCTURE ---")
try:
    with open('data/raw/apps_metadata.json', 'r', encoding='utf-8') as f:
        apps = json.load(f)
    
    print(f"\n✓ Total apps loaded: {len(apps)}")
    
    if apps:
        print("\n--- First App Full Structure ---")
        first_app = apps[0]
        print(f"App Title: {first_app.get('title', 'Unknown')}")
        print(f"App ID: {first_app.get('appId', 'Unknown')}")
        print(f"Total fields: {len(first_app)}")
        print(f"\nAll field names:")
        print(f"  {list(first_app.keys())}")
        
        print("\n--- Target Fields Check ---")
        target_fields = ['appId', 'title', 'developer', 'score', 'ratings', 'installs', 'genre', 'price']
        for field in target_fields:
            if field in first_app:
                value = first_app[field]
                if isinstance(value, str) and len(value) > 50:
                    value = value[:50] + "..."
                print(f"  ✓ {field:15s}: {value}")
            else:
                print(f"  ✗ {field:15s}: MISSING")
        
        # Check how many apps have reviews
        print("\n--- Apps Review Count ---")
        apps_with_reviews = [a for a in apps if a.get('reviews', 0) > 0]
        print(f"Apps with reviews: {len(apps_with_reviews)} / {len(apps)}")
        
        if apps_with_reviews:
            print("\nApps that have reviews:")
            for a in apps_with_reviews[:10]:  # Show first 10
                print(f"  - {a.get('title', 'Unknown'):40s} ({a.get('reviews', 0)} reviews)")
        
except Exception as e:
    print(f"✗ Error loading apps: {e}")
    import traceback
    traceback.print_exc()

# Inspect reviews data
print("--- REVIEWS DATA STRUCTURE ---")

if reviews_size == 0:
    print(" Skipping review inspection - file is empty")
    print("\nNext step: Re-run data ingestion to collect reviews")
else:
    try:
        # Read first review
        with open('data/raw/apps_reviews.jsonl', 'r', encoding='utf-8') as f:
            first_line = f.readline()
            if first_line.strip():
                review = json.loads(first_line)
            else:
                print("✗ First line is empty!")
                exit(1)
        
        print("\n--- First Review Full Structure ---")
        print(f"Total fields: {len(review)}")
        print(f"Field names: {list(review.keys())}")
        
        print("\n--- Field Values (first review) ---")
        for key, value in review.items():
            value_str = str(value)
            if len(value_str) > 100:
                value_str = value_str[:100] + "..."
            print(f"  {key:20s}: {value_str:50s} (type: {type(value).__name__})")
        
        print("\n--- Target Fields Check ---")
        target_fields = ['app_id', 'reviewId', 'userName', 'score', 'content', 'thumbsUpCount', 'at']
        print("Looking for these fields:")
        for field in target_fields:
            if field in review:
                value = review[field]
                if len(str(value)) > 50:
                    value = str(value)[:50] + "..."
                print(f"  ✓ {field:20s}: {value}")
            else:
                print(f"  ✗ {field:20s}: MISSING")
        
        # Check for alternative field names
        print("\n--- Checking Alternative Field Names ---")
        alternatives = {
            'score': ['rating', 'stars', 'userRating'],
            'thumbsUpCount': ['thumbsUp', 'helpful', 'helpfulCount', 'thumbsup'],
            'userName': ['username', 'user', 'author'],
            'reviewId': ['id', 'review_id'],
        }
        
        for target, alts in alternatives.items():
            if target not in review:
                print(f"\n'{target}' not found, checking alternatives:")
                for alt in alts:
                    if alt in review:
                        print(f"  ✓ Found '{alt}' instead: {review[alt]}")
                        break
                else:
                    print(f"  ✗ None of the alternatives found: {alts}")
        
        # Count total reviews
        print("\n--- Counting All Reviews ---")
        with open('data/raw/apps_reviews.jsonl', 'r', encoding='utf-8') as f:
            review_count = sum(1 for line in f if line.strip())
        print(f"Total reviews in file: {review_count:,}")
        
        # Sample a few more reviews to check consistency
        if review_count > 1:
            print("\n--- Checking Field Consistency (next 5 reviews) ---")
            with open('data/raw/apps_reviews.jsonl', 'r', encoding='utf-8') as f:
                for i in range(min(5, review_count)):
                    line = f.readline()
                    if line.strip():
                        rev = json.loads(line)
                        fields = list(rev.keys())
                        print(f"  Review {i+1}: {len(fields)} fields")
        
    except Exception as e:
        print(f"✗ Error loading reviews: {e}")
        import traceback
        traceback.print_exc()

print("INSPECTION COMPLETE")

# Provide actionable next steps
if reviews_size == 0:
    print("\n ACTION REQUIRED:")
    print("  Your reviews file is empty. You need to re-run data ingestion.")
    print("\n  Run this command:")
    print("  python src/1_data_ingestion.py")
else:
    print("\n DATA LOOKS GOOD")
    print("  You can proceed with transformation:")
    print("  python src/2_data_transformation.py")

