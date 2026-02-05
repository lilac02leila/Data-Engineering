"""
Step 2: Data Transformation
This script cleans and transforms raw JSON data into structured CSV files
"""

import json
import pandas as pd
from datetime import datetime
import re

def load_raw_data():
    """Load raw JSON/JSONL data"""
    print("\n--- Loading raw data ---")
    
    # Load apps metadata (JSON)
    with open('data/raw/apps_metadata.json', 'r', encoding='utf-8') as f:
        apps_data = json.load(f)
    print(f"✓ Loaded {len(apps_data)} apps")
    
    # Load reviews (JSONL - one JSON per line)
    reviews_data = []
    with open('data/raw/apps_reviews.jsonl', 'r', encoding='utf-8') as f:
        for line in f:
            reviews_data.append(json.loads(line))
    print(f"✓ Loaded {len(reviews_data)} reviews")
    
    return apps_data, reviews_data

def inspect_raw_data(apps_data, reviews_data):
    """
    Inspect raw data and identify issues
    This is what you'd do manually before writing transformation code
    """
    print("DATA INSPECTION - IDENTIFYING ISSUES")
    
    print("\n--- Sample App Record ---")
    if apps_data:
        sample_app = apps_data[0]
        print(f"App has {len(sample_app)} fields:")
        for key, value in list(sample_app.items())[:10]:
            print(f"  {key}: {value} (type: {type(value).__name__})")
    
    print("\n--- Sample Review Record ---")
    if reviews_data:
        sample_review = reviews_data[0]
        print(f"Review has {len(sample_review)} fields:")
        for key, value in sample_review.items():
            print(f"  {key}: {value} (type: {type(value).__name__})")
    
    print("\n--- COMMON ISSUES IDENTIFIED ---")
    issues = [
        "1. NESTED STRUCTURES: Some fields contain nested dictionaries/lists",
        "2. INCONSISTENT TYPES: 'installs' is string like '1,000,000+' not numeric",
        "3. MISSING VALUES: Not all apps have all fields (price, score, etc.)",
        "4. TIMESTAMP FORMAT: Review timestamps are datetime objects, need string format",
        "5. EXTRA FIELDS: Raw data has many fields we don't need for analytics",
        "6. DATA TYPES: Need to convert strings to proper numeric/date types",
        "7. SPECIAL CHARACTERS: Text fields may contain problematic characters"
    ]
    for issue in issues:
        print(f"  {issue}")
    

def transform_apps_data(apps_data):
    """
    Transform apps metadata into structured format
    Target schema: appId, title, developer, score, ratings, installs, genre, price
    """
    print("\n--- Transforming apps metadata ---")
    
    transformed_apps = []
    error_count = 0
    
    for app in apps_data:
        try:
            # Extract and clean installs field
            # Converts "1,000,000+" to 1000000
            installs_str = app.get('installs', '0')
            installs = int(re.sub(r'[,+]', '', str(installs_str)))
            
            # Extract price (remove currency symbol if present)
            price_str = app.get('price', '0')
            if price_str == 0 or price_str == '0':
                price = 0.0
            else:
                # Remove currency symbols and convert to float
                price = float(re.sub(r'[^0-9.]', '', str(price_str)))
            
            # Get genre (might be in 'genre' or 'genreId')
            genre = app.get('genre', app.get('genreId', 'Unknown'))
            
            # Handle missing scores (apps without ratings)
            score = app.get('score', None)
            if score is None or score == 0:
                score = None  # Will handle as NaN in pandas
            
            transformed_app = {
                'appId': app.get('appId', ''),
                'title': app.get('title', 'Unknown'),
                'developer': app.get('developer', 'Unknown'),
                'score': score,
                'ratings': app.get('ratings', 0),
                'installs': installs,
                'genre': genre,
                'price': price
            }
            
            transformed_apps.append(transformed_app)
            
        except Exception as e:
            error_count += 1
            if error_count <= 3:
                print(f"  ✗ Error transforming app {app.get('appId', 'unknown')}: {e}")
    
    if error_count > 3:
        print(f"  ✗ ... and {error_count - 3} more app transformation errors")
    
    # Convert to DataFrame for easier manipulation
    df_apps = pd.DataFrame(transformed_apps)
    
    # Remove duplicates based on appId
    df_apps = df_apps.drop_duplicates(subset=['appId'], keep='first')
    
    print(f"✓ Transformed {len(df_apps)} apps")
    print(f"  Removed {len(transformed_apps) - len(df_apps)} duplicates")
    
    return df_apps

def get_app_name_mapping(apps_data):
    """Create a mapping of app_id to app_name from apps data"""
    mapping = {}
    for app in apps_data:
        app_id = app.get('appId', '')
        app_name = app.get('title', 'Unknown')
        if app_id:
            mapping[app_id] = app_name
    return mapping

def transform_reviews_data(reviews_data, valid_app_ids, app_name_mapping):
    """
    Transform reviews into structured format
    Target schema: app_id, app_name, reviewId, userName, score, content, thumbsUpCount, at
    """
    print("\n--- Transforming reviews data ---")
    
    transformed_reviews = []
    skipped_count = 0
    error_count = 0
    
    for idx, review in enumerate(reviews_data):
        try:
            # Skip reviews for apps not in our apps catalog
            app_id = review.get('app_id', '')
            if app_id not in valid_app_ids:
                skipped_count += 1
                continue
            
            # Get app name from mapping
            app_name = app_name_mapping.get(app_id, 'Unknown')
            
            # Convert timestamp to string format (ISO format)
            # Handle different possible timestamp formats
            at_timestamp = review.get('at')
            if isinstance(at_timestamp, datetime):
                at_str = at_timestamp.strftime('%Y-%m-%d %H:%M:%S')
            elif at_timestamp:
                at_str = str(at_timestamp)
            else:
                at_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Clean review content (remove excessive whitespace)
            content = review.get('content', '')
            if isinstance(content, str):
                content = ' '.join(content.split())
            
            # Handle score field - it might be called 'score' or 'rating'
            score = review.get('score', review.get('rating', 0))
            
            # Handle thumbsUpCount - might be called differently
            thumbs_up = review.get('thumbsUpCount', 
                                  review.get('thumbsUp', 
                                  review.get('helpfulCount', 0)))
            
            transformed_review = {
                'app_id': app_id,
                'app_name': app_name,
                'reviewId': review.get('reviewId', ''),
                'userName': review.get('userName', 'Anonymous'),
                'score': score,
                'content': content,
                'thumbsUpCount': thumbs_up,
                'at': at_str
            }
            
            transformed_reviews.append(transformed_review)
            
        except KeyError as e:
            error_count += 1
            if error_count <= 3:
                print(f"  ✗ Missing field in review {idx}: {e}")
                print(f"     Available fields: {list(review.keys())}")
        except Exception as e:
            error_count += 1
            if error_count <= 3:
                print(f"  ✗ Error transforming review {idx}: {e}")
    
    if error_count > 3:
        print(f"  ✗ ... and {error_count - 3} more review transformation errors")
    
    # Check if we have any reviews
    if len(transformed_reviews) == 0:
        print("\n WARNING: No reviews were successfully transformed!")
        print("  This might mean:")
        print("    - Field names in reviews don't match expectations")
        print("    - All reviews were filtered out")
        print("    - There's a data structure issue")
        
        if reviews_data:
            print("\n  First review structure for debugging:")
            first_review = reviews_data[0]
            print(f"  Available fields: {list(first_review.keys())}")
            print("\n  Field values:")
            for key, value in first_review.items():
                print(f"    {key}: {str(value)[:50]}...")
        
        # Return empty dataframe with correct columns
        return pd.DataFrame(columns=['app_id', 'app_name', 'reviewId', 'userName', 
                                    'score', 'content', 'thumbsUpCount', 'at'])
    
    # Convert to DataFrame
    df_reviews = pd.DataFrame(transformed_reviews)
    
    # Remove duplicate reviews (same reviewId)
    before_dedup = len(df_reviews)
    df_reviews = df_reviews.drop_duplicates(subset=['reviewId'], keep='first')
    
    # Convert score to numeric (handle any string values)
    df_reviews['score'] = pd.to_numeric(df_reviews['score'], errors='coerce')
    df_reviews['thumbsUpCount'] = pd.to_numeric(df_reviews['thumbsUpCount'], errors='coerce').fillna(0).astype(int)
    
    # Convert timestamp to datetime
    df_reviews['at'] = pd.to_datetime(df_reviews['at'], errors='coerce')
    
    # Remove reviews with invalid timestamps or scores
    before_clean = len(df_reviews)
    df_reviews = df_reviews.dropna(subset=['at', 'score'])
    
    print(f"✓ Transformed {len(df_reviews)} reviews")
    print(f"  Skipped {skipped_count} reviews for apps not in catalog")
    print(f"  Removed {before_dedup - len(df_reviews)} duplicate reviews")
    print(f"  Removed {before_clean - len(df_reviews)} reviews with invalid data")
    if error_count > 0:
        print(f"  Total errors encountered: {error_count}")
    
    return df_reviews

def save_processed_data(df_apps, df_reviews):
    """Save transformed data to CSV files"""
    print("\n--- Saving processed data ---")
    
    # Save apps catalog
    df_apps.to_csv('data/processed/apps_catalog.csv', index=False)
    print(f"✓ Saved: data/processed/apps_catalog.csv ({len(df_apps)} rows)")
    
    # Save reviews
    df_reviews.to_csv('data/processed/apps_reviews.csv', index=False)
    print(f"✓ Saved: data/processed/apps_reviews.csv ({len(df_reviews)} rows)")
    
    # Display summary statistics
    print("\n--- Data Quality Summary ---")
    print(f"Apps with scores: {df_apps['score'].notna().sum()} / {len(df_apps)}")
    print(f"Apps with ratings: {(df_apps['ratings'] > 0).sum()} / {len(df_apps)}")
    
    if df_apps['score'].notna().sum() > 0:
        print(f"Average app score: {df_apps['score'].mean():.2f}")
    
    if len(df_reviews) > 0 and df_reviews['score'].notna().sum() > 0:
        print(f"Average review score: {df_reviews['score'].mean():.2f}")
        print(f"Date range: {df_reviews['at'].min()} to {df_reviews['at'].max()}")
    else:
        print(" No valid reviews to analyze")

def main():
    """Main execution function"""
    print("DATA TRANSFORMATION PIPELINE - STEP 2")
    
    try:
        # Load raw data
        apps_data, reviews_data = load_raw_data()
        
        # Inspect data and identify issues
        inspect_raw_data(apps_data, reviews_data)
        
        # Transform apps metadata
        df_apps = transform_apps_data(apps_data)
        
        # Get valid app IDs for filtering reviews
        valid_app_ids = set(df_apps['appId'].values)
        
        # Create app name mapping
        app_name_mapping = get_app_name_mapping(apps_data)
        
        # Transform reviews
        df_reviews = transform_reviews_data(reviews_data, valid_app_ids, app_name_mapping)
        
        # Only save if we have data
        if len(df_apps) > 0:
            save_processed_data(df_apps, df_reviews)
            print("DATA TRANSFORMATION COMPLETE!")
            
            if len(df_reviews) == 0:
                print("\n  WARNING: No reviews were processed!")
                print("Pipeline can continue but later steps may have limited data.")
        else:
            print("DATA TRANSFORMATION FAILED!")
            print("Not enough app data to proceed.")
            raise Exception("No apps transformed successfully")
            
    except Exception as e:
        print(f"\n✗ Error in Step 2: Data Transformation:")
        print(f"   {str(e)}")
        raise

if __name__ == "__main__":
    main()