"""
Step 1: Data Acquisition and Ingestion
This script extracts AI note-taking app data from Google Play Store
"""

import json
from google_play_scraper import app, search, reviews
import os
import time
import sys

# Force UTF-8 encoding for Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

def create_directories():
    """Create necessary directories if they don't exist"""
    os.makedirs('data/raw', exist_ok=True)
    os.makedirs('data/processed', exist_ok=True)
    print("✓ Directories created")

def search_ai_note_apps():
    """
    Search for AI note-taking apps on Google Play Store
    Returns: List of app IDs
    """
    print("\n--- Searching for AI note-taking apps ---")
    
    # Search terms for AI note-taking apps
    search_terms = [
        "AI note taking",
        "AI notes",
        "note taking AI",
        "smart notes",
        "AI notebook"
    ]
    
    app_ids = set()  # Use set to avoid duplicates
    
    for term in search_terms:
        try:
            print(f"Searching for: '{term}'")
            results = search(
                term,
                lang="en",
                country="us",
                n_hits=20  # Get top 20 results per search term
            )
            
            for result in results:
                app_ids.add(result['appId'])
                title = result['title']
                # Handle non-ASCII characters safely
                try:
                    print(f"  Found: {title} ({result['appId']})")
                except UnicodeEncodeError:
                    print(f"  Found: [App with special characters] ({result['appId']})")
                
        except Exception as e:
            print(f"  ✗ Error searching '{term}': {e}")
    
    print(f"\n✓ Total unique apps found: {len(app_ids)}")
    return list(app_ids)

def extract_app_metadata(app_ids):
    """
    Extract detailed metadata for each app
    Args:
        app_ids: List of app IDs to extract data for
    Returns:
        List of app metadata dictionaries
    """
    print("\n--- Extracting app metadata ---")
    apps_data = []
    
    for i, app_id in enumerate(app_ids, 1):
        try:
            print(f"[{i}/{len(app_ids)}] Extracting metadata for: {app_id}")
            
            # Get detailed app information
            app_details = app(
                app_id,
                lang='en',
                country='us'
            )
            
            apps_data.append(app_details)
            
            # Safely print title and score
            title = app_details.get('title', 'Unknown')
            score = app_details.get('score', 'N/A')
            try:
                print(f"  ✓ Success: {title} (Score: {score})")
            except UnicodeEncodeError:
                print(f"  ✓ Success: [App] (Score: {score})")
            
            # Small delay to be nice to the API
            time.sleep(0.5)
            
        except Exception as e:
            print(f"  ✗ Error extracting {app_id}: {e}")
    
    print(f"\n✓ Successfully extracted metadata for {len(apps_data)} apps")
    return apps_data

def extract_app_reviews(app_ids, max_reviews_per_app=100):
    """
    Extract reviews for each app
    Args:
        app_ids: List of app IDs to extract reviews for
        max_reviews_per_app: Maximum number of reviews to extract per app
    Returns:
        List of review dictionaries
    """
    print("\n--- Extracting app reviews ---")
    print(f"Attempting to extract up to {max_reviews_per_app} reviews per app...")
    all_reviews = []
    apps_with_reviews = 0
    apps_without_reviews = 0
    
    for i, app_id in enumerate(app_ids, 1):
        try:
            print(f"\n[{i}/{len(app_ids)}] Extracting reviews for: {app_id}")
            
            # Get reviews for the app using reviews() instead of reviews_all()
            # The reviews() function returns a tuple of (reviews_list, token)
            result, continuation_token = reviews(
                app_id,
                lang='en',
                country='us',
                count=max_reviews_per_app
            )
            
            if result and len(result) > 0:
                # Add app_id to each review for later reference
                for review in result:
                    review['app_id'] = app_id
                
                all_reviews.extend(result)
                apps_with_reviews += 1
                print(f"  ✓ Extracted {len(result)} reviews")
            else:
                apps_without_reviews += 1
                print(f"   No reviews found for this app")
            
            # Small delay between apps to avoid rate limiting
            time.sleep(1)
            
        except Exception as e:
            apps_without_reviews += 1
            error_msg = str(e)
            # Truncate long error messages
            if len(error_msg) > 100:
                error_msg = error_msg[:100] + "..."
            print(f"  ✗ Error extracting reviews: {error_msg}")
    
    print(f"\n--- Review Extraction Summary ---")
    print(f"✓ Total reviews extracted: {len(all_reviews)}")
    print(f"  Apps with reviews: {apps_with_reviews}")
    print(f"  Apps without reviews: {apps_without_reviews}")
    
    return all_reviews

def clean_for_json(obj):
    """
    Clean data for JSON serialization, handling special characters
    """
    if isinstance(obj, dict):
        return {k: clean_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_for_json(item) for item in obj]
    elif isinstance(obj, str):
        # Replace or remove problematic characters
        try:
            # Try to encode as UTF-8
            obj.encode('utf-8')
            return obj
        except UnicodeEncodeError:
            # If it fails, use ASCII with replacement
            return obj.encode('ascii', errors='replace').decode('ascii')
    else:
        return obj

def save_raw_data(apps_data, reviews_data):
    """
    Save raw data to JSON files (no transformations applied)
    Args:
        apps_data: List of app metadata
        reviews_data: List of reviews
    """
    print("\n--- Saving raw data ---")
    
    # Clean data before saving
    apps_data_clean = clean_for_json(apps_data)
    reviews_data_clean = clean_for_json(reviews_data)
    
    # Save apps metadata
    apps_file = 'data/raw/apps_metadata.json'
    try:
        with open(apps_file, 'w', encoding='utf-8') as f:
            json.dump(apps_data_clean, f, indent=2, ensure_ascii=False, default=str)
        print(f"✓ Saved: {apps_file} ({len(apps_data)} apps)")
        
        # Verify file was written
        if os.path.exists(apps_file):
            size = os.path.getsize(apps_file)
            print(f"  File size: {size:,} bytes")
    except Exception as e:
        print(f"✗ Error saving apps file: {e}")
        # Try with ASCII encoding as fallback
        with open(apps_file, 'w', encoding='utf-8') as f:
            json.dump(apps_data_clean, f, indent=2, ensure_ascii=True, default=str)
        print(f"✓ Saved with ASCII encoding: {apps_file}")
    
    # Save reviews as JSONL (one JSON object per line)
    reviews_file = 'data/raw/apps_reviews.jsonl'
    try:
        with open(reviews_file, 'w', encoding='utf-8') as f:
            for review in reviews_data_clean:
                f.write(json.dumps(review, ensure_ascii=False, default=str) + '\n')
        print(f"✓ Saved: {reviews_file} ({len(reviews_data)} reviews)")
        
        # Verify file was written
        if os.path.exists(reviews_file):
            size = os.path.getsize(reviews_file)
            print(f"  File size: {size:,} bytes")
            if size == 0:
                print(f"  WARNING: Reviews file is empty! ")
    except Exception as e:
        print(f"✗ Error saving reviews file: {e}")
        # Try with ASCII encoding as fallback
        with open(reviews_file, 'w', encoding='utf-8') as f:
            for review in reviews_data_clean:
                f.write(json.dumps(review, ensure_ascii=True, default=str) + '\n')
        print(f"✓ Saved with ASCII encoding: {reviews_file}")

def main():
    """Main execution function"""
    print("DATA INGESTION PIPELINE - STEP 1")
    
    try:
        # Create directory structure
        create_directories()
        
        # Search for AI note-taking apps
        app_ids = search_ai_note_apps()
        
        if not app_ids:
            print("\n✗ No apps found. Try adjusting search terms.")
            return
        
        # Limit to first 15 apps for faster testing (you can increase this)
        if len(app_ids) > 15:
            print(f"\n  Found {len(app_ids)} apps. Using first 15 for this run.")
            print("   (You can increase this limit in the code)")
            app_ids = app_ids[:15]
        
        # Extract app metadata
        apps_data = extract_app_metadata(app_ids)
        
        if not apps_data:
            print("\n✗ No app metadata extracted. Cannot proceed.")
            return
        
        # Extract reviews for each app
        print("NOTE: Some apps may have very few or no reviews.")
        print("This is normal for new or niche apps.")
        
        reviews_data = extract_app_reviews(app_ids, max_reviews_per_app=100)
        
        # Check if we got any reviews
        if len(reviews_data) == 0:
            print("\n  WARNING: No reviews were extracted!")
            print("This could mean:")
            print("  - The apps found have no reviews yet")
            print("  - The Google Play Scraper API had issues")
            print("  - There was rate limiting")
            print("\nThe pipeline can still continue with empty reviews data.")
            print("You'll be able to complete the lab using just the app metadata.")
        
        # Save everything as raw data
        save_raw_data(apps_data, reviews_data)
        
        print("DATA INGESTION COMPLETE!")
        print(f"Apps collected: {len(apps_data)}")
        print(f"Reviews collected: {len(reviews_data)}")
        
        if len(reviews_data) == 0:
            print("\n No reviews were collected!")
            print("You can still proceed with the lab using app metadata only.")
            print("The transformation and later steps will work but have limited data.")
        else:
            print("\n✓ Data collection successful!")
            print("You can now run Step 2: Data Transformation")
        
        print("="*60)
        
    except Exception as e:
        print(f"\n✗ Error in data ingestion: {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()