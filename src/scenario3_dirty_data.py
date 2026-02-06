"""
SCENARIO 3: Dirty and Inconsistent Data
Handle invalid, missing, and malformed values

This tests:
- Data quality validation
- Error detection timing
- Impact on aggregates
- Silent failures vs explicit failures
"""

import pandas as pd
import numpy as np
import os

def load_dirty_data():
    """Load reviews with data quality issues"""
    print("\n--- Loading Dirty Data ---")
    
    dirty_file = 'data/raw/note_taking_ai_reviews_dirty.csv'
    
    if not os.path.exists(dirty_file):
        print(f"✗ Error: {dirty_file} not found!")
        print("Make sure the file is in data/raw/ folder")
        return None
    
    # Load without any type conversion first
    reviews = pd.read_csv(dirty_file, dtype=str)
    print(f"✓ Loaded {len(reviews)} reviews (as strings)")
    
    return reviews

def diagnose_data_quality(dirty_data):
    """Diagnose all data quality issues"""
    print("\n" + "="*60)
    print("DATA QUALITY DIAGNOSIS")
    print("="*60)
    
    print("\n1. MISSING VALUES:")
    missing = dirty_data.isnull().sum()
    for col in missing[missing > 0].index:
        pct = (missing[col] / len(dirty_data)) * 100
        print(f"   {col:20s}: {missing[col]:4d} ({pct:.1f}%)")
    
    print("\n2. SCORE FIELD ISSUES:")
    scores = dirty_data['score']
    print(f"   Unique values: {scores.nunique()}")
    print(f"   Sample values: {scores.unique()[:10]}")
    
    # Try to convert to numeric
    scores_numeric = pd.to_numeric(scores, errors='coerce')
    invalid_scores = scores_numeric.isnull().sum()
    print(f"   Invalid scores: {invalid_scores}")
    
    # Check range
    valid_scores = scores_numeric.dropna()
    if len(valid_scores) > 0:
        out_of_range = ((valid_scores < 1) | (valid_scores > 5)).sum()
        print(f"   Out of range (not 1-5): {out_of_range}")
        print(f"   Min: {valid_scores.min()}, Max: {valid_scores.max()}")
    
    print("\n3. TIMESTAMP FIELD ISSUES:")
    timestamps = pd.to_datetime(dirty_data['at'], errors='coerce')
    invalid_timestamps = timestamps.isnull().sum()
    print(f"   Invalid timestamps: {invalid_timestamps}")
    
    if invalid_timestamps > 0:
        print("   Examples of invalid timestamps:")
        invalid_examples = dirty_data[timestamps.isnull()]['at'].head(3)
        for ex in invalid_examples:
            print(f"     - '{ex}'")
    
    print("\n4. THUMBS UP COUNT ISSUES:")
    thumbs = pd.to_numeric(dirty_data['thumbsUpCount'], errors='coerce')
    invalid_thumbs = thumbs.isnull().sum()
    print(f"   Invalid thumbsUpCount: {invalid_thumbs}")
    
    valid_thumbs = thumbs.dropna()
    if len(valid_thumbs) > 0:
        negative = (valid_thumbs < 0).sum()
        print(f"   Negative values: {negative}")
    
    print("\n5. TEXT FIELD ISSUES:")
    empty_content = (dirty_data['content'].str.strip() == '').sum()
    null_content = dirty_data['content'].isnull().sum()
    print(f"   Empty content: {empty_content}")
    print(f"   Null content: {null_content}")
    
    print("="*60)

def process_without_validation(dirty_data):
    """Process data without validation - show silent failures"""
    print("\n" + "="*60)
    print("PROCESSING WITHOUT VALIDATION")
    print("="*60)
    
    print("\nAttempting to convert and aggregate...")
    
    # Convert to numeric (errors='coerce' hides the problem!)
    dirty_data['score_num'] = pd.to_numeric(dirty_data['score'], errors='coerce')
    dirty_data['thumbsUpCount_num'] = pd.to_numeric(dirty_data['thumbsUpCount'], errors='coerce')
    dirty_data['at_dt'] = pd.to_datetime(dirty_data['at'], errors='coerce')
    
    # Calculate metrics WITHOUT removing bad data
    avg_score_bad = dirty_data['score_num'].mean()
    count_with_nulls = len(dirty_data)
    count_valid_scores = dirty_data['score_num'].notna().sum()
    
    print(f"\nResults (WITH bad data included):")
    print(f"  Total records processed: {count_with_nulls}")
    print(f"  Records with valid scores: {count_valid_scores}")
    print(f"  Average score: {avg_score_bad:.2f}")
    print(f"   This average is WRONG - it ignored {count_with_nulls - count_valid_scores} records!")
    
    # Show how it affects aggregates
    print("\n PROBLEM: Bad data was silently ignored")
    print("   - NaN values dropped from mean calculation")
    print("   - No warning or error raised")
    print("   - Analytics show incorrect results")
    print("   - Business decisions based on bad data!")

def process_with_validation(dirty_data):
    """Process with proper validation and cleaning"""
    print("\n" + "="*60)
    print("PROCESSING WITH VALIDATION")
    print("="*60)
    
    cleaned = dirty_data.copy()
    total_removed = 0
    
    # Step 1: Convert types
    print("\n1. Converting data types...")
    cleaned['score'] = pd.to_numeric(cleaned['score'], errors='coerce')
    cleaned['thumbsUpCount'] = pd.to_numeric(cleaned['thumbsUpCount'], errors='coerce')
    cleaned['at'] = pd.to_datetime(cleaned['at'], errors='coerce')
    
    # Step 2: Validate scores
    print("\n2. Validating scores...")
    before = len(cleaned)
    
    # Remove null scores
    invalid_scores = cleaned['score'].isnull()
    print(f"   Removing {invalid_scores.sum()} records with invalid scores")
    cleaned = cleaned[~invalid_scores]
    
    # Remove out-of-range scores
    out_of_range = (cleaned['score'] < 1) | (cleaned['score'] > 5)
    print(f"   Removing {out_of_range.sum()} records with out-of-range scores")
    cleaned = cleaned[~out_of_range]
    
    total_removed += before - len(cleaned)
    
    # Step 3: Validate timestamps
    print("\n3. Validating timestamps...")
    before = len(cleaned)
    invalid_timestamps = cleaned['at'].isnull()
    print(f"   Removing {invalid_timestamps.sum()} records with invalid timestamps")
    cleaned = cleaned[~invalid_timestamps]
    total_removed += before - len(cleaned)
    
    # Step 4: Fix thumbs up count
    print("\n4. Fixing thumbsUpCount...")
    cleaned['thumbsUpCount'] = cleaned['thumbsUpCount'].fillna(0)
    cleaned.loc[cleaned['thumbsUpCount'] < 0, 'thumbsUpCount'] = 0
    print(f"   Set negative/null values to 0")
    
    # Step 5: Clean text
    print("\n5. Cleaning text content...")
    cleaned['content'] = cleaned['content'].fillna('')
    cleaned['userName'] = cleaned['userName'].fillna('Anonymous')
    
    print(f"\n✓ Cleaned dataset: {len(cleaned)} records")
    print(f"  Removed: {total_removed} bad records ({(total_removed/len(dirty_data)*100):.1f}%)")
    
    # Calculate CORRECT metrics
    avg_score_good = cleaned['score'].mean()
    print(f"  Average score (CORRECT): {avg_score_good:.2f}")
    
    return cleaned

def compare_results(dirty_data, clean_data):
    """Compare results with and without validation"""
    print("\n" + "="*60)
    print("IMPACT ON ANALYTICS")
    print("="*60)
    
    # Bad processing (silent failures)
    dirty_data['score_num'] = pd.to_numeric(dirty_data['score'], errors='coerce')
    avg_bad = dirty_data['score_num'].mean()
    
    # Good processing (validated)
    avg_good = clean_data['score'].mean()
    
    print("\nAverage Score Comparison:")
    print(f"  Without validation: {avg_bad:.2f} (WRONG)")
    print(f"  With validation:    {avg_good:.2f} (CORRECT)")
    print(f"  Difference:         {abs(avg_good - avg_bad):.2f}")
    
    if abs(avg_good - avg_bad) > 0.1:
        print("\n  ⚠️  SIGNIFICANT DIFFERENCE!")
        print("     Bad data silently corrupted analytics")
    
    # App-level impact
    print("\nRecord Count:")
    print(f"  Without validation: {len(dirty_data)} (includes bad data)")
    print(f"  With validation:    {len(clean_data)} (only valid data)")
    print(f"  Data loss:          {len(dirty_data) - len(clean_data)} records")

def save_cleaned_data(clean_data):
    """Save cleaned data"""
    print("\n--- Saving Cleaned Data ---")
    
    output_file = 'data/processed/apps_reviews_dirty_cleaned.csv'
    clean_data.to_csv(output_file, index=False)
    print(f"✓ Saved: {output_file}")

def create_data_quality_report(dirty_data, clean_data):
    """Create a data quality report"""
    print("\n" + "="*60)
    print("DATA QUALITY REPORT")
    print("="*60)
    
    total_records = len(dirty_data)
    clean_records = len(clean_data)
    rejected_records = total_records - clean_records
    
    print(f"""
Total Records:       {total_records:,}
Clean Records:       {clean_records:,}
Rejected Records:    {rejected_records:,} ({(rejected_records/total_records*100):.1f}%)

Issues Found:
- Invalid scores:    {pd.to_numeric(dirty_data['score'], errors='coerce').isnull().sum()}
- Invalid dates:     {pd.to_datetime(dirty_data['at'], errors='coerce').isnull().sum()}
- Missing content:   {dirty_data['content'].isnull().sum()}
- Negative thumbs:   {(pd.to_numeric(dirty_data['thumbsUpCount'], errors='coerce') < 0).sum()}

Data Quality Score:  {(clean_records/total_records*100):.1f}%
    """)

def main():
    """Main execution"""
    print("="*60)
    print("SCENARIO 3: DIRTY DATA")
    print("="*60)
    
    # Load dirty data
    dirty_data = load_dirty_data()
    if dirty_data is None:
        return
    
    # Diagnose issues
    diagnose_data_quality(dirty_data)
    
    # Show silent failures
    process_without_validation(dirty_data)
    
    # Process with validation
    clean_data = process_with_validation(dirty_data)
    
    # Compare results
    compare_results(dirty_data, clean_data)
    
    # Save cleaned data
    save_cleaned_data(clean_data)
    
    # Create quality report
    create_data_quality_report(dirty_data, clean_data)
 

if __name__ == "__main__":
    main()
