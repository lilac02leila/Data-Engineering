"""
SCENARIO 2: Schema Drift
Handle reviews with different column names and structure

This tests:
- Hard-coded column name assumptions
- Schema validation
- Graceful failure vs silent errors
- Code modularity
"""

import pandas as pd
import os

def load_schema_drift_data():
    """Load reviews with schema drift"""
    print("\n--- Loading Schema Drift Data ---")
    
    drift_file = 'data/raw/note_taking_ai_reviews_schema_drift.csv'
    
    if not os.path.exists(drift_file):
        print(f"✗ Error: {drift_file} not found!")
        print("Make sure the file is in data/raw/ folder")
        return None
    
    reviews = pd.read_csv(drift_file)
    print(f"✓ Loaded {len(reviews)} reviews")
    
    # Show actual schema
    print(f"\nActual columns in file ({len(reviews.columns)}):")
    for col in reviews.columns:
        print(f"  - {col}")
    
    print("\nSample data:")
    print(reviews.head(2))
    
    return reviews

def detect_schema_differences(drift_data):
    """Detect schema differences from expected"""
    print("SCHEMA COMPARISON")
    
    expected_schema = [
        'app_id', 'app_name', 'reviewId', 'userName', 
        'score', 'content', 'thumbsUpCount', 'at'
    ]
    
    actual_schema = list(drift_data.columns)
    
    print("\nExpected columns:")
    for col in expected_schema:
        print(f"  - {col}")
    
    print("\nActual columns:")
    for col in actual_schema:
        print(f"  - {col}")
    
    # Find differences
    missing = set(expected_schema) - set(actual_schema)
    extra = set(actual_schema) - set(expected_schema)
    
    print("\n Differences:")
    print(f"  Missing columns: {missing if missing else 'None'}")
    print(f"  Extra columns: {extra if extra else 'None'}")
    
    # Try to find mappings
    print("\n Possible Column Mappings:")
    mappings = {
        'app_id' : ['appId', 'AppID'],
        'app_name' : ['appName', 'appTitle'],
        'content' : ['review_text', 'description'],
        'score': ['rating', 'user_rating', 'stars'],
        'thumbsUpCount': ['helpful_count', 'likes', 'thumbs_up'],
        'userName': ['user_name', 'author', 'reviewer', 'username'],
        'reviewId': ['review_id', 'id'],
        'at': ['date', 'timestamp', 'review_date', 'review_time']
    }
    
    found_mappings = {}
    for expected, alternatives in mappings.items():
        if expected not in actual_schema:
            for alt in alternatives:
                if alt in actual_schema:
                    found_mappings[expected] = alt
                    print(f"  '{expected}' → '{alt}' ✓")
                    break
            else:
                print(f"  '{expected}' → NOT FOUND ✗")
        
    return found_mappings

def run_old_pipeline(drift_data):
    """Try to run the original transformation logic"""
    print("RUNNING ORIGINAL PIPELINE")
    
    print("\nAttempting to access expected columns...")
    
    try:
        # This is what the original code does
        app_ids = drift_data['app_id']
        print("  ✓ app_id column found")
    except KeyError as e:
        print(f"  ✗ KeyError: {e}")
    
    try:
        scores = drift_data['score']
        print("  ✓ score column found")
    except KeyError as e:
        print(f"  ✗ KeyError: {e}")
    
    try:
        thumbs = drift_data['thumbsUpCount']
        print("  ✓ thumbsUpCount column found")
    except KeyError as e:
        print(f"  ✗ KeyError: {e}")
    
    print("\n Observation: Pipeline FAILS EXPLICITLY with KeyError")
    print("   This is GOOD, we know immediately something is wrong")
    print("   BAD alternative: silently producing wrong results")

def create_schema_mapper(mappings):
    """Create a flexible schema mapper"""
    print("\n--- Creating Schema Mapper ---")
    
    # Default schema
    default_schema = {
        'app_id': 'app_id',
        'app_name': 'app_name',
        'reviewId': 'reviewId',
        'userName': 'userName',
        'score': 'score',
        'content': 'content',
        'thumbsUpCount': 'thumbsUpCount',
        'at': 'at'
    }
    
    # Update with found mappings
    schema_map = {**default_schema, **mappings}
    
    print("Schema mapping:")
    for standard, actual in schema_map.items():
        if standard != actual:
            print(f"  {standard:20s} ← {actual}")
        else:
            print(f"  {standard:20s} (no change)")
    
    return schema_map

def transform_with_mapper(drift_data, schema_map):
    """Transform data using schema mapper"""
    print("\n--- Transforming with Schema Mapper ---")
    
    transformed = pd.DataFrame()
    
    for standard_name, actual_name in schema_map.items():
        if actual_name in drift_data.columns:
            transformed[standard_name] = drift_data[actual_name]
            print(f"  ✓ Mapped {actual_name} → {standard_name}")
        else:
            print(f"  Column {actual_name} not found, using NULL")
            transformed[standard_name] = None
    
    # Apply transformations
    transformed['at'] = pd.to_datetime(transformed['at'], errors='coerce')
    transformed['score'] = pd.to_numeric(transformed['score'], errors='coerce')
    transformed['thumbsUpCount'] = pd.to_numeric(transformed['thumbsUpCount'], errors='coerce').fillna(0)
    
    # Remove invalid rows
    before = len(transformed)
    transformed = transformed.dropna(subset=['at', 'score'])
    print(f"\n✓ Transformed {len(transformed)} reviews")
    print(f"  Removed {before - len(transformed)} invalid rows")
    
    return transformed

def save_transformed_data(transformed):
    """Save transformed data"""
    print("\n--- Saving Transformed Data ---")
    
    output_file = 'data/processed/apps_reviews_schema_drift_fixed.csv'
    transformed.to_csv(output_file, index=False)
    print(f"✓ Saved: {output_file}")

def analyze_code_changes():
    """Analyze what code changes were needed"""
    print("CODE CHANGE ANALYSIS")
    
    print("""
TOTAL: 4 files changed for a simple column rename!

PROBLEMS:
✗ Column names scattered throughout codebase
✗ No central schema definition
✗ No validation until runtime
✗ Changes ripple through entire pipeline
    """)

def main():
    """Main execution"""
    print("SCENARIO 2: SCHEMA DRIFT")
    
    # Load data with schema drift
    drift_data = load_schema_drift_data()
    if drift_data is None:
        return
    
    # Detect schema differences
    mappings = detect_schema_differences(drift_data)
    
    # Show how original pipeline fails
    run_old_pipeline(drift_data)
    
    # Create schema mapper
    schema_map = create_schema_mapper(mappings)
    
    # Transform with mapper
    transformed = transform_with_mapper(drift_data, schema_map)
    
    # Save results
    save_transformed_data(transformed)
    
    # Analyze code changes
    analyze_code_changes()
    
    print("OBSERVATIONS & REFLECTIONS")
    print("""
1. FAILURE MODE:
   ✓ GOOD: Pipeline fails explicitly with KeyError
   ✗ BAD: Failure happens at runtime, not earlier
   
2. HARD-CODED ASSUMPTIONS:
   - Column names are hard-coded everywhere
   - No schema validation at pipeline start
   - Schema changes require code changes in multiple files
   
3. LOCALIZATION OF CHANGES:
   - Changes needed in ALL pipeline stages
   - No single source of truth for schema
   - Testing requires running entire pipeline
   
4. BETTER APPROACHES:
   - Schema registry (defines expected structure)
   - Schema validation at ingestion
   - Column mapping configuration file
   - Type checking and contracts
   
LESSON: Hard-coded schemas make pipelines brittle.
Schema evolution is common - pipelines must handle it gracefully.
    """)

if __name__ == "__main__":
    main()
