"""
SCENARIO 1: New Reviews Batch
Handle a new batch of reviews arriving from upstream system

This tests:
- How the pipeline handles new data
- Duplicate review detection
- Reviews for unknown apps
- Full rebuild vs incremental processing
"""

import pandas as pd
import os
import json

def load_existing_data():
    """Load existing processed data"""
    print("\n--- Loading Existing Processed Data ---")
    
    try:
        apps = pd.read_csv('data/processed/apps_catalog.csv')
        reviews = pd.read_csv('data/processed/apps_reviews.csv')
        print(f"✓ Loaded {len(apps)} apps")
        print(f"✓ Loaded {len(reviews)} existing reviews")
        return apps, reviews
    except FileNotFoundError as e:
        print(f"✗ Error: {e}")
        print("Make sure you've run Steps 1-3 first!")
        return None, None

def load_new_batch():
    """Load the new batch of reviews"""
    print("\n--- Loading New Reviews Batch ---")
    
    batch_file = 'data/raw/note_taking_ai_reviews_batch2.csv'
    
    if not os.path.exists(batch_file):
        print(f"✗ Error: {batch_file} not found!")
        print("Make sure the file is in data/raw/ folder")
        return None
    
    new_reviews = pd.read_csv(batch_file)
    print(f"✓ Loaded {len(new_reviews)} reviews from new batch")
    
    # Show sample
    print("\nSample of new batch data:")
    print(new_reviews.head(3))
    
    return new_reviews

def analyze_batch(new_batch, existing_reviews, existing_apps):
    """Analyze the new batch for potential issues"""
    print("\n" + "="*60)
    print("BATCH ANALYSIS")
    print("="*60)
    
    # Check for duplicate reviewIds
    if len(existing_reviews) > 0:
        existing_ids = set(existing_reviews['reviewId'].values)
        new_ids = set(new_batch['reviewId'].values)
        
        duplicates = existing_ids & new_ids
        print(f"\n1. Duplicate Review Detection:")
        print(f"   Existing reviews: {len(existing_ids)}")
        print(f"   New batch reviews: {len(new_ids)}")
        print(f"   Duplicates found: {len(duplicates)}")
        if len(duplicates) > 0:
            print(f"   ⚠️  {len(duplicates)} reviews already exist!")
    
    # Check for unknown apps
    known_apps = set(existing_apps['appId'].values)
    batch_apps = set(new_batch['app_id'].unique())
    
    unknown_apps = batch_apps - known_apps
    print(f"\n2. Unknown App Detection:")
    print(f"   Known apps: {len(known_apps)}")
    print(f"   Apps in batch: {len(batch_apps)}")
    print(f"   Unknown apps: {len(unknown_apps)}")
    if len(unknown_apps) > 0:
        print(f"   ⚠️  Reviews reference {len(unknown_apps)} apps not in catalog!")
        print(f"   Unknown app IDs: {list(unknown_apps)[:5]}")
    
    # Check date range
    if 'at' in new_batch.columns:
        new_batch['at'] = pd.to_datetime(new_batch['at'], errors='coerce')
        print(f"\n3. Date Range:")
        print(f"   Earliest review: {new_batch['at'].min()}")
        print(f"   Latest review: {new_batch['at'].max()}")
    
    print("="*60)

def process_new_batch_full_rebuild(new_batch, existing_reviews, existing_apps):
    """
    Strategy 1: FULL REBUILD
    Combine old and new data, remove duplicates, rebuild everything
    """
    print("\n" + "="*60)
    print("PROCESSING STRATEGY: FULL REBUILD")
    print("="*60)
    
    print("\nThis approach:")
    print("  - Combines all old and new reviews")
    print("  - Removes duplicates")
    print("  - Filters out reviews for unknown apps")
    print("  - Rebuilds all outputs from scratch")
    
    # Combine old and new
    all_reviews = pd.concat([existing_reviews, new_batch], ignore_index=True)
    print(f"\n✓ Combined reviews: {len(all_reviews)}")
    
    # Remove duplicates (keep first occurrence)
    before_dedup = len(all_reviews)
    all_reviews = all_reviews.drop_duplicates(subset=['reviewId'], keep='first')
    print(f"✓ Removed {before_dedup - len(all_reviews)} duplicate reviews")
    
    # Filter out reviews for unknown apps
    known_apps = set(existing_apps['appId'].values)
    before_filter = len(all_reviews)
    all_reviews = all_reviews[all_reviews['app_id'].isin(known_apps)]
    print(f"✓ Filtered out {before_filter - len(all_reviews)} reviews for unknown apps")
    
    # Ensure proper data types
    all_reviews['at'] = pd.to_datetime(all_reviews['at'], errors='coerce')
    all_reviews['score'] = pd.to_numeric(all_reviews['score'], errors='coerce')
    all_reviews = all_reviews.dropna(subset=['at', 'score'])
    
    print(f"\n✓ Final review count: {len(all_reviews)}")
    
    return all_reviews

def rebuild_serving_layer(apps, reviews):
    """Rebuild serving layer outputs with new data"""
    print("\n--- Rebuilding Serving Layer ---")
    
    # App-level KPIs
    app_kpis = reviews.groupby('app_id').agg({
        'reviewId': 'count',
        'score': ['mean', lambda x: (x <= 2).mean() * 100],
        'at': ['min', 'max']
    }).reset_index()
    
    app_kpis.columns = [
        'app_id',
        'num_reviews',
        'avg_rating',
        'pct_low_rating',
        'first_review_date',
        'most_recent_review_date'
    ]
    
    app_kpis['avg_rating'] = app_kpis['avg_rating'].round(2)
    app_kpis['pct_low_rating'] = app_kpis['pct_low_rating'].round(2)
    app_kpis = app_kpis.sort_values('num_reviews', ascending=False)
    
    # Daily metrics
    reviews['date'] = reviews['at'].dt.date
    daily_metrics = reviews.groupby('date').agg({
        'reviewId': 'count',
        'score': 'mean'
    }).reset_index()
    
    daily_metrics.columns = ['date', 'daily_num_reviews', 'daily_avg_rating']
    daily_metrics['daily_avg_rating'] = daily_metrics['daily_avg_rating'].round(2)
    daily_metrics = daily_metrics.sort_values('date')
    
    print(f"✓ Created KPIs for {len(app_kpis)} apps")
    print(f"✓ Created metrics for {len(daily_metrics)} days")
    
    return app_kpis, daily_metrics

def save_updated_data(reviews, app_kpis, daily_metrics):
    """Save updated processed data"""
    print("\n--- Saving Updated Data ---")
    
    # Save with version suffix to preserve original
    reviews.to_csv('data/processed/apps_reviews_v2.csv', index=False)
    print(f"✓ Saved: apps_reviews_v2.csv ({len(reviews)} reviews)")
    
    app_kpis.to_csv('data/processed/app_level_kpis_v2.csv', index=False)
    print(f"✓ Saved: app_level_kpis_v2.csv")
    
    daily_metrics.to_csv('data/processed/daily_metrics_v2.csv', index=False)
    print(f"✓ Saved: daily_metrics_v2.csv")
    
    print("\n💡 Note: Original files preserved. New files have '_v2' suffix.")

def compare_before_after(old_reviews, new_reviews):
    """Compare metrics before and after"""
    print("\n" + "="*60)
    print("BEFORE vs AFTER COMPARISON")
    print("="*60)
    
    print(f"\nTotal Reviews:")
    print(f"  Before: {len(old_reviews):,}")
    print(f"  After:  {len(new_reviews):,}")
    print(f"  Change: +{len(new_reviews) - len(old_reviews):,} ({((len(new_reviews)/len(old_reviews) - 1) * 100):.1f}%)")
    
    if len(old_reviews) > 0:
        print(f"\nAverage Rating:")
        print(f"  Before: {old_reviews['score'].mean():.2f}")
        print(f"  After:  {new_reviews['score'].mean():.2f}")
        print(f"  Change: {new_reviews['score'].mean() - old_reviews['score'].mean():+.2f}")
    
    print("="*60)

def main():
    """Main execution"""
    print("="*60)
    print("SCENARIO 1: NEW REVIEWS BATCH")
    print("="*60)
    
    # Load existing data
    existing_apps, existing_reviews = load_existing_data()
    if existing_apps is None:
        return
    
    # Load new batch
    new_batch = load_new_batch()
    if new_batch is None:
        return
    
    # Analyze the batch
    analyze_batch(new_batch, existing_reviews, existing_apps)
    
    # Process with full rebuild strategy
    updated_reviews = process_new_batch_full_rebuild(
        new_batch, existing_reviews, existing_apps
    )
    
    # Rebuild serving layer
    app_kpis, daily_metrics = rebuild_serving_layer(existing_apps, updated_reviews)
    
    # Save updated data
    save_updated_data(updated_reviews, app_kpis, daily_metrics)
    
    # Compare before/after
    compare_before_after(existing_reviews, updated_reviews)
    


if __name__ == "__main__":
    main()
