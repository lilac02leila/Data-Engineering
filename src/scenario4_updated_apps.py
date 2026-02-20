"""
SCENARIO 4: Updated Applications Metadata
Handle updated apps with duplicates and inconsistencies

This tests:
- Duplicate key handling
- Referential integrity
- Join behavior with bad data
- Downstream impact of metadata issues
"""

import pandas as pd
import os
import sys

def load_updated_apps():
    """Load updated apps metadata with robust CSV parsing"""
    print("\n--- Loading Updated Apps Metadata ---")
    
    apps_file = 'data/raw/note_taking_ai_apps_updated.csv'
    
    if not os.path.exists(apps_file):
        print(f"✗ Error: {apps_file} not found!")
        print("Make sure the file is in data/raw/ folder")
        return None
    
    # Try different parsing strategies
    try:
        # Strategy 1: Standard parsing with error handling
        apps = pd.read_csv(
            apps_file,
            encoding='utf-8',
            on_bad_lines='skip',  # Skip problematic lines
            engine='python'  # Use Python engine for more flexibility
        )
        print(f"✓ Loaded {len(apps)} app records")
        
    except Exception as e:
        print(f"✗ Error with standard parsing: {e}")
        
        try:
            # Strategy 2: More flexible parsing
            apps = pd.read_csv(
                apps_file,
                encoding='utf-8',
                quoting=1,  # QUOTE_ALL
                escapechar='\\',
                on_bad_lines='skip',
                engine='python'
            )
            print(f"✓ Loaded {len(apps)} app records (with flexible parsing)")
            
        except Exception as e2:
            print(f"✗ Error with flexible parsing: {e2}")
            
            # Strategy 3: Manual line-by-line parsing
            print("\nAttempting manual parsing...")
            apps = parse_csv_manually(apps_file)
            if apps is not None:
                print(f"✓ Loaded {len(apps)} app records (manual parsing)")
            else:
                return None
    
    print("\nColumns found:")
    for col in apps.columns:
        print(f"  - {col}")
    
    return apps

def parse_csv_manually(filepath):
    """Manually parse CSV file line by line to handle errors"""
    import csv
    
    try:
        rows = []
        with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.reader(f)
            header = next(reader)
            
            for line_num, row in enumerate(reader, start=2):
                if len(row) == len(header):
                    rows.append(row)
                else:
                    print(f"  Skipping line {line_num}: expected {len(header)} fields, got {len(row)}")
        
        if rows:
            df = pd.DataFrame(rows, columns=header)
            return df
        else:
            print("No valid rows found")
            return None
            
    except Exception as e:
        print(f"Manual parsing failed: {e}")
        return None

def diagnose_apps_issues(apps):
    """Diagnose issues in apps metadata"""
    print("\n" + "="*60)
    print("APPS METADATA DIAGNOSIS")
    print("="*60)
    
    # Check for duplicates
    print("\n1. DUPLICATE APP IDS:")
    duplicate_ids = apps['appId'].duplicated()
    num_duplicates = duplicate_ids.sum()
    print(f"   Total records: {len(apps)}")
    print(f"   Unique appIds: {apps['appId'].nunique()}")
    print(f"   Duplicate records: {num_duplicates}")
    
    if num_duplicates > 0:
        print("\n   Duplicate appIds:")
        dup_apps = apps[apps['appId'].duplicated(keep=False)].sort_values('appId')
        for app_id in dup_apps['appId'].unique()[:5]:  # Show first 5
            count = (dup_apps['appId'] == app_id).sum()
            print(f"     - {app_id}: appears {count} times")
            
            # Show the different values
            dup_records = dup_apps[dup_apps['appId'] == app_id]
            if 'title' in dup_records.columns:
                print(f"       Titles: {dup_records['title'].tolist()}")
            if 'score' in dup_records.columns:
                print(f"       Scores: {dup_records['score'].tolist()}")
    
    # Check for missing values
    print("\n2. MISSING VALUES:")
    missing = apps.isnull().sum()
    for col in missing[missing > 0].index:
        pct = (missing[col] / len(apps)) * 100
        print(f"   {col:20s}: {missing[col]:4d} ({pct:.1f}%)")
    
    # Check numeric fields
    print("\n3. NUMERIC FIELD ISSUES:")
    
    # Score
    if 'score' in apps.columns:
        apps['score'] = pd.to_numeric(apps['score'], errors='coerce')
        invalid_scores = ((apps['score'] < 0) | (apps['score'] > 5)).sum()
        null_scores = apps['score'].isnull().sum()
        print(f"   Invalid scores (not 0-5): {invalid_scores}")
        print(f"   Null scores: {null_scores}")
    
    # Ratings
    if 'ratings' in apps.columns:
        apps['ratings'] = pd.to_numeric(apps['ratings'], errors='coerce')
        negative_ratings = (apps['ratings'] < 0).sum()
        print(f"   Negative ratings: {negative_ratings}")
    
    # Installs
    if 'installs' in apps.columns:
        apps['installs'] = pd.to_numeric(apps['installs'], errors='coerce')
        negative_installs = (apps['installs'] < 0).sum()
        print(f"   Negative installs: {negative_installs}")
    
    print("="*60)
    
    return apps

def handle_duplicates_naive(apps):
    """Show what happens with naive duplicate handling"""
    print("\n" + "="*60)
    print("NAIVE DUPLICATE HANDLING")
    print("="*60)
    
    print("\nStrategy: Keep first occurrence, drop rest")
    print("(This is what drop_duplicates() does by default)")
    
    before = len(apps)
    apps_dedup = apps.drop_duplicates(subset=['appId'], keep='first')
    removed = before - len(apps_dedup)
    
    print(f"\nRemoved {removed} duplicate records")
    print(f"Remaining: {len(apps_dedup)} apps")
    
    # Show what was lost
    if removed > 0:
        print("\n⚠️  PROBLEM: What if the first record had bad data?")
        print("   We blindly kept it and threw away potentially good data!")
        
        # Show example
        dup_apps = apps[apps['appId'].duplicated(keep=False)].sort_values('appId')
        if len(dup_apps) > 0:
            first_dup_id = dup_apps['appId'].iloc[0]
            dup_records = apps[apps['appId'] == first_dup_id]
            
            print(f"\n   Example: {first_dup_id}")
            print("   Different versions of same app:")
            for idx, row in dup_records.iterrows():
                score = row.get('score', 'N/A')
                ratings = row.get('ratings', 'N/A')
                title = row.get('title', 'N/A')
                if isinstance(title, str) and len(title) > 30:
                    title = title[:30] + "..."
                print(f"     Score: {score}, Ratings: {ratings}, Title: {title}")
            
            print("\n   Which one is correct? We don't know!")
            print("   We just kept the first one arbitrarily.")
    
    return apps_dedup

def handle_duplicates_smart(apps):
    """Handle duplicates with business logic"""
    print("\n" + "="*60)
    print("SMART DUPLICATE HANDLING")
    print("="*60)
    
    print("\nStrategy: Keep record with most information")
    
    # Group by appId and create aggregation logic
    def pick_best_record(group):
        # Prefer record with:
        # 1. Non-null score
        # 2. Highest number of ratings
        # 3. Most complete data
        
        if len(group) == 1:
            return group.iloc[0]
        
        # Score completeness
        group = group.copy()
        group['has_score'] = group['score'].notna()
        
        # Convert ratings to numeric for comparison
        group['ratings_num'] = pd.to_numeric(group['ratings'], errors='coerce')
        group['ratings_num'] = group['ratings_num'].fillna(0)
        
        # Count complete fields
        group['complete_fields'] = group.notna().sum(axis=1)
        
        # Sort by: has score, ratings count, completeness
        group = group.sort_values(
            ['has_score', 'ratings_num', 'complete_fields'],
            ascending=[False, False, False]
        )
        
        return group.iloc[0]
    
    apps_smart = apps.groupby('appId', as_index=False).apply(pick_best_record)
    apps_smart = apps_smart.reset_index(drop=True)
    
    print(f"✓ Resolved to {len(apps_smart)} unique apps")
    print("  Used business logic to pick best record for each appId")
    
    return apps_smart

def test_join_impact(apps_with_dups):
    """Show how duplicate appIds affect joins"""
    print("\n" + "="*60)
    print("IMPACT ON JOINS WITH REVIEWS")
    print("="*60)
    
    # Load reviews
    reviews_file = 'data/processed/apps_reviews.csv'
    if not os.path.exists(reviews_file):
        print("⚠️  No reviews file found, skipping join test")
        return
    
    reviews = pd.read_csv(reviews_file)
    print(f"Loaded {len(reviews)} reviews")
    
    # Check if there are actually duplicates
    dup_count = apps_with_dups['appId'].duplicated().sum()
    
    if dup_count == 0:
        print("\n✓ No duplicates in apps data")
        print("  Join will work correctly")
        return
    
    # Join with duplicate apps
    print("\n1. Join with DUPLICATE apps:")
    joined_dups = reviews.merge(apps_with_dups, left_on='app_id', right_on='appId', how='left')
    print(f"   Result: {len(joined_dups)} rows")
    print(f"   Expected: {len(reviews)} rows")
    
    if len(joined_dups) > len(reviews):
        print(f"   ⚠️  Extra rows created: {len(joined_dups) - len(reviews)}")
        print(f"   This is a CARTESIAN PRODUCT problem!")
        
        # Show example
        dup_app_id = apps_with_dups[apps_with_dups['appId'].duplicated()]['appId'].iloc[0]
        affected_reviews = joined_dups[joined_dups['app_id'] == dup_app_id]
        original_count = len(reviews[reviews['app_id'] == dup_app_id])
        
        print(f"\n   Example: Reviews for {dup_app_id}")
        print(f"   Original reviews: {original_count}")
        print(f"   After join: {len(affected_reviews)} rows (duplicated!)")
    
    # Join with deduplicated apps
    print("\n2. Join with DEDUPLICATED apps:")
    apps_clean = apps_with_dups.drop_duplicates(subset=['appId'])
    joined_clean = reviews.merge(apps_clean, left_on='app_id', right_on='appId', how='left')
    print(f"   Result: {len(joined_clean)} rows")
    print(f"   ✓ Correct! Same as number of reviews")
    
    # Check for missing joins
    missing_apps = joined_clean['appId'].isnull().sum()
    if missing_apps > 0:
        print(f"\n   ⚠️  {missing_apps} reviews couldn't be joined (unknown apps)")

def create_referential_integrity_report(apps):
    """Check referential integrity"""
    print("\n" + "="*60)
    print("REFERENTIAL INTEGRITY CHECK")
    print("="*60)
    
    # Load reviews
    reviews_file = 'data/processed/apps_reviews.csv'
    if not os.path.exists(reviews_file):
        print("No reviews file found")
        return
    
    reviews = pd.read_csv(reviews_file)
    
    app_ids_in_catalog = set(apps['appId'].unique())
    app_ids_in_reviews = set(reviews['app_id'].unique())
    
    orphan_reviews = app_ids_in_reviews - app_ids_in_catalog
    unused_apps = app_ids_in_catalog - app_ids_in_reviews
    
    print(f"\nApps in catalog:    {len(app_ids_in_catalog)}")
    print(f"Apps in reviews:    {len(app_ids_in_reviews)}")
    print(f"Orphan reviews:     {len(orphan_reviews)} apps")
    print(f"Unused apps:        {len(unused_apps)} apps")
    
    if orphan_reviews:
        print(f"\n⚠️  {len(orphan_reviews)} apps have reviews but no metadata!")
        count = sum(reviews['app_id'].isin(orphan_reviews))
        print(f"   This affects {count} reviews")
        print(f"   Example orphan app IDs: {list(orphan_reviews)[:5]}")

def save_cleaned_apps(apps):
    """Save cleaned apps data"""
    print("\n--- Saving Cleaned Apps ---")
    
    output_file = 'data/processed/apps_catalog_updated_clean.csv'
    apps.to_csv(output_file, index=False)
    print(f"✓ Saved: {output_file}")

def main():
    """Main execution"""
    print("="*60)
    print("SCENARIO 4: UPDATED APPS METADATA")
    print("="*60)
    
    # Load updated apps
    apps = load_updated_apps()
    if apps is None:
        print("\n✗ Could not load apps data. Exiting.")
        return
    
    # Diagnose issues
    apps = diagnose_apps_issues(apps)
    
    # Show naive duplicate handling
    apps_naive = handle_duplicates_naive(apps.copy())
    
    # Show smart duplicate handling
    apps_smart = handle_duplicates_smart(apps.copy())
    
    # Test join impact
    test_join_impact(apps)
    
    # Referential integrity check
    create_referential_integrity_report(apps_smart)
    
    # Save cleaned data
    save_cleaned_apps(apps_smart)
    
    print("\n" + "="*60)
    print("OBSERVATIONS & REFLECTIONS")
    print("="*60)
    print("""
1. DUPLICATE HANDLING:
   ✗ Naive: drop_duplicates(keep='first') - arbitrary choice
   ✓ Smart: Business logic to pick best record
   ✓ Better: Prevent duplicates at source
   
2. JOIN BEHAVIOR:
   - Duplicate keys cause CARTESIAN PRODUCTS
   - One review becomes multiple rows in output
   - Aggregations produce wrong counts
   - Very hard to debug!
   
3. REFERENTIAL INTEGRITY:
   - Reviews reference apps that don't exist
   - Apps have no reviews (orphaned metadata)
   - No foreign key constraints to enforce
   - No cascade delete/update
   
4. DOWNSTREAM IMPACT:
   - Aggregates are wrong (inflated counts)
   - Averages calculated on duplicated data
   - Dashboards show incorrect metrics
   - Business decisions based on bad joins
   
5. WHAT'S MISSING:
   - Primary key constraints (prevent duplicate appIds)
   - Foreign key constraints (reviews → apps)
   - Data validation at insert time
   - Audit trail of changes
   - Merge strategy documentation
   
LESSON: Metadata quality is as important as data quality.
Duplicates and broken references silently corrupt analytics.
Databases provide constraints - file-based pipelines don't!
    """)
    print("="*60)

if __name__ == "__main__":
    main()