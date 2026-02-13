"""
Load Raw Data from Lab 1 into DuckDB
This script creates raw tables in DuckDB from your Lab 1 processed CSV files
"""

import duckdb
import pandas as pd
import os

def create_duckdb_connection(db_path='playstore_analytics/playstore_analytics.duckdb'):
    """Create connection to DuckDB"""
    print(f"\n--- Connecting to DuckDB ---")
    print(f"Database: {db_path}")
    
    conn = duckdb.connect(db_path)
    print("✓ Connected successfully")
    return conn

def create_raw_schema(conn):
    """Create raw schema for source data"""
    print("\n--- Creating Raw Schema ---")
    
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    print("✓ Created schema: raw")

def load_apps_data(conn):
    """Load apps data from CSV to DuckDB"""
    print("\n--- Loading Apps Data ---")
    
    apps_file = 'data/processed/apps_catalog.csv'
    
    if not os.path.exists(apps_file):
        print(f"✗ Error: {apps_file} not found!")
        print("Make sure you've run Lab 1 first!")
        return False
    
    # Read CSV
    df_apps = pd.read_csv(apps_file)
    print(f"✓ Read {len(df_apps)} apps from CSV")
    
    # Create table in DuckDB
    conn.execute("DROP TABLE IF EXISTS raw.apps")
    conn.execute("""
        CREATE TABLE raw.apps AS 
        SELECT * FROM df_apps
    """)
    
    # Verify
    count = conn.execute("SELECT COUNT(*) FROM raw.apps").fetchone()[0]
    print(f"✓ Loaded {count} apps into raw.apps table")
    
    # Show sample
    print("\nSample data:")
    sample = conn.execute("SELECT * FROM raw.apps LIMIT 3").df()
    print(sample)
    
    return True

def load_reviews_data(conn):
    """Load reviews data from CSV to DuckDB"""
    print("\n--- Loading Reviews Data ---")
    
    reviews_file = 'data/processed/apps_reviews.csv'
    
    if not os.path.exists(reviews_file):
        print(f"✗ Error: {reviews_file} not found!")
        print("Make sure you've run Lab 1 first!")
        return False
    
    # Read CSV
    df_reviews = pd.read_csv(reviews_file)
    
    # Convert timestamp to proper datetime
    df_reviews['at'] = pd.to_datetime(df_reviews['at'])
    
    print(f"✓ Read {len(df_reviews)} reviews from CSV")
    
    # Create table in DuckDB
    conn.execute("DROP TABLE IF EXISTS raw.reviews")
    conn.execute("""
        CREATE TABLE raw.reviews AS 
        SELECT * FROM df_reviews
    """)
    
    # Verify
    count = conn.execute("SELECT COUNT(*) FROM raw.reviews").fetchone()[0]
    print(f"✓ Loaded {count} reviews into raw.reviews table")
    
    # Show sample
    print("\nSample data:")
    sample = conn.execute("SELECT * FROM raw.reviews LIMIT 3").df()
    print(sample)
    
    return True

def create_indexes(conn):
    """Create indexes for better query performance"""
    print("\n--- Creating Indexes ---")
    
    # Apps indexes
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_apps_appid ON raw.apps(appId)")
        print("✓ Created index on apps.appId")
    except:
        print("  Index already exists or failed")
    
    # Reviews indexes
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_reviews_appid ON raw.reviews(app_id)")
        print("✓ Created index on reviews.app_id")
        
        conn.execute("CREATE INDEX IF NOT EXISTS idx_reviews_date ON raw.reviews(at)")
        print("✓ Created index on reviews.at")
    except:
        print("  Indexes already exist or failed")

def show_summary(conn):
    """Show summary of loaded data"""
    print("\n" + "="*60)
    print("DATA LOAD SUMMARY")
    print("="*60)
    
    # Apps summary
    apps_stats = conn.execute("""
        SELECT 
            COUNT(*) as total_apps,
            COUNT(DISTINCT developer) as unique_developers,
            AVG(score) as avg_app_score,
            SUM(ratings) as total_ratings
        FROM raw.apps
        WHERE score IS NOT NULL
    """).df()
    
    print("\nApps Table:")
    print(f"  Total apps: {apps_stats['total_apps'].iloc[0]}")
    print(f"  Unique developers: {apps_stats['unique_developers'].iloc[0]}")
    print(f"  Average app score: {apps_stats['avg_app_score'].iloc[0]:.2f}")
    print(f"  Total ratings: {apps_stats['total_ratings'].iloc[0]:,.0f}")
    
    # Reviews summary
    reviews_stats = conn.execute("""
        SELECT 
            COUNT(*) as total_reviews,
            COUNT(DISTINCT app_id) as apps_with_reviews,
            AVG(score) as avg_review_score,
            MIN(at) as earliest_review,
            MAX(at) as latest_review
        FROM raw.reviews
    """).df()
    
    print("\nReviews Table:")
    print(f"  Total reviews: {reviews_stats['total_reviews'].iloc[0]}")
    print(f"  Apps with reviews: {reviews_stats['apps_with_reviews'].iloc[0]}")
    print(f"  Average review score: {reviews_stats['avg_review_score'].iloc[0]:.2f}")
    print(f"  Date range: {reviews_stats['earliest_review'].iloc[0]} to {reviews_stats['latest_review'].iloc[0]}")
    
    print("\n" + "="*60)

def main():
    """Main execution"""
    print("="*60)
    print("LOAD RAW DATA INTO DUCKDB")
    print("="*60)
    
    # Create connection
    conn = create_duckdb_connection()
    
    # Create raw schema
    create_raw_schema(conn)
    
    # Load data
    apps_loaded = load_apps_data(conn)
    reviews_loaded = load_reviews_data(conn)
    
    if not (apps_loaded and reviews_loaded):
        print("\n✗ Data loading failed!")
        print("Make sure you have:")
        print("  - data/processed/apps_catalog.csv")
        print("  - data/processed/apps_reviews.csv")
        print("Run Lab 1 Steps 1-3 first to create these files.")
        return
    
    # Create indexes
    create_indexes(conn)
    
    # Show summary
    show_summary(conn)
    
    # Close connection
    conn.close()
    
    print("\n✓ Data successfully loaded into DuckDB!")
    print("\nNext steps:")
    print("  1. Navigate to playstore_analytics folder")
    print("  2. Run: dbt run")
    print("  3. Run: dbt test")
    print("="*60)

if __name__ == "__main__":
    main()
