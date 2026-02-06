"""
SCENARIO 5: New Business Logic - Sentiment Analysis
Add sentiment detection to identify rating-text mismatches

This tests:
- Where new logic belongs in pipeline
- How many stages need modification
- Code reusability and maintainability
- Separation of concerns
"""

import pandas as pd
import re

def load_processed_reviews():
    """Load existing processed reviews"""
    print("\n--- Loading Processed Reviews ---")
    
    reviews_file = 'data/processed/apps_reviews.csv'
    
    try:
        reviews = pd.read_csv(reviews_file)
        print(f"✓ Loaded {len(reviews)} reviews")
        return reviews
    except FileNotFoundError:
        print(f"✗ Error: {reviews_file} not found!")
        print("Make sure you've run Steps 1-3 first!")
        return None

def simple_sentiment_analysis(text):
    """
    Simple keyword-based sentiment analysis
    Returns: 'positive', 'negative', or 'neutral'
    """
    if pd.isna(text) or text == '':
        return 'neutral'
    
    text = text.lower()
    
    # Positive keywords
    positive_words = [
        'great', 'excellent', 'amazing', 'love', 'perfect', 'best',
        'wonderful', 'awesome', 'fantastic', 'good', 'nice', 'helpful',
        'easy', 'useful', 'recommend', 'impressed', 'satisfied'
    ]
    
    # Negative keywords
    negative_words = [
        'bad', 'terrible', 'worst', 'hate', 'awful', 'poor', 'useless',
        'horrible', 'disappointing', 'frustrating', 'broken', 'crash',
        'bug', 'slow', 'waste', 'refund', 'uninstall', 'annoying'
    ]
    
    # Count occurrences
    pos_count = sum(1 for word in positive_words if word in text)
    neg_count = sum(1 for word in negative_words if word in text)
    
    # Determine sentiment
    if pos_count > neg_count:
        return 'positive'
    elif neg_count > pos_count:
        return 'negative'
    else:
        return 'neutral'

def detect_mismatches(reviews):
    """Detect rating-sentiment mismatches"""
    print("\n--- Detecting Rating-Sentiment Mismatches ---")
    
    # Add sentiment column
    print("Analyzing sentiment of review text...")
    reviews['sentiment'] = reviews['content'].apply(simple_sentiment_analysis)
    
    print(f"✓ Sentiment analysis complete")
    print(f"\nSentiment distribution:")
    print(reviews['sentiment'].value_counts())
    
    # Define mismatches
    # High rating (4-5) with negative sentiment
    high_rating_neg_sentiment = (
        (reviews['score'] >= 4) & 
        (reviews['sentiment'] == 'negative')
    )
    
    # Low rating (1-2) with positive sentiment
    low_rating_pos_sentiment = (
        (reviews['score'] <= 2) & 
        (reviews['sentiment'] == 'positive')
    )
    
    # Identify all mismatches
    reviews['is_mismatch'] = high_rating_neg_sentiment | low_rating_pos_sentiment
    reviews['mismatch_type'] = 'none'
    reviews.loc[high_rating_neg_sentiment, 'mismatch_type'] = 'high_rating_negative_text'
    reviews.loc[low_rating_pos_sentiment, 'mismatch_type'] = 'low_rating_positive_text'
    
    num_mismatches = reviews['is_mismatch'].sum()
    pct_mismatches = (num_mismatches / len(reviews)) * 100
    
    print(f"\n📊 Results:")
    print(f"Total reviews: {len(reviews)}")
    print(f"Mismatches found: {num_mismatches} ({pct_mismatches:.1f}%)")
    print(f"  High rating + negative text: {high_rating_neg_sentiment.sum()}")
    print(f"  Low rating + positive text: {low_rating_pos_sentiment.sum()}")
    
    return reviews

def show_mismatch_examples(reviews):
    """Show examples of mismatches"""
    print("\n" + "="*60)
    print("MISMATCH EXAMPLES")
    print("="*60)
    
    mismatches = reviews[reviews['is_mismatch']]
    
    if len(mismatches) == 0:
        print("No mismatches found!")
        return
    
    # High rating + negative sentiment
    high_neg = mismatches[mismatches['mismatch_type'] == 'high_rating_negative_text']
    if len(high_neg) > 0:
        print("\n1. HIGH RATING (4-5★) but NEGATIVE text:")
        for idx, row in high_neg.head(3).iterrows():
            print(f"\n   App: {row['app_name']}")
            print(f"   Rating: {row['score']}★")
            print(f"   Text: \"{row['content'][:150]}...\"")
            print(f"   Sentiment: {row['sentiment']}")
    
    # Low rating + positive sentiment
    low_pos = mismatches[mismatches['mismatch_type'] == 'low_rating_positive_text']
    if len(low_pos) > 0:
        print("\n2. LOW RATING (1-2★) but POSITIVE text:")
        for idx, row in low_pos.head(3).iterrows():
            print(f"\n   App: {row['app_name']}")
            print(f"   Rating: {row['score']}★")
            print(f"   Text: \"{row['content'][:150]}...\"")
            print(f"   Sentiment: {row['sentiment']}")

def create_mismatch_metrics(reviews):
    """Create app-level mismatch metrics"""
    print("\n--- Creating App-Level Mismatch Metrics ---")
    
    app_metrics = reviews.groupby('app_id').agg({
        'reviewId': 'count',
        'is_mismatch': 'sum',
        'score': 'mean'
    }).reset_index()
    
    app_metrics.columns = ['app_id', 'total_reviews', 'mismatch_count', 'avg_rating']
    app_metrics['mismatch_rate'] = (
        app_metrics['mismatch_count'] / app_metrics['total_reviews'] * 100
    ).round(2)
    
    # Sort by mismatch rate
    app_metrics = app_metrics.sort_values('mismatch_rate', ascending=False)
    
    print(f"✓ Created metrics for {len(app_metrics)} apps")
    print("\nTop 5 apps by mismatch rate:")
    print(app_metrics.head(5)[['app_id', 'total_reviews', 'mismatch_count', 'mismatch_rate']])
    
    return app_metrics

def save_enriched_data(reviews, app_metrics):
    """Save data with new sentiment features"""
    print("\n--- Saving Enriched Data ---")
    
    # Save reviews with sentiment
    reviews_file = 'data/processed/apps_reviews_with_sentiment.csv'
    reviews.to_csv(reviews_file, index=False)
    print(f"✓ Saved: {reviews_file}")
    
    # Save app-level mismatch metrics
    metrics_file = 'data/processed/app_mismatch_metrics.csv'
    app_metrics.to_csv(metrics_file, index=False)
    print(f"✓ Saved: {metrics_file}")

def analyze_pipeline_changes():
    """Analyze what changes were needed"""
    
    print("""

CURRENT PIPELINE ISSUES:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✗ No clear place for analytical logic
✗ Features mixed with transformation
✗ Hard to reuse logic
✗ No feature documentation
✗ No feature versioning
✗ Testing requires running full pipeline

    """)

def main():
    """Main execution"""
    print("="*60)
    print("SCENARIO 5: NEW BUSINESS LOGIC")
    print("="*60)
    
    # Load reviews
    reviews = load_processed_reviews()
    if reviews is None:
        return
    
    # Detect mismatches
    reviews = detect_mismatches(reviews)
    
    # Show examples
    show_mismatch_examples(reviews)
    
    # Create metrics
    app_metrics = create_mismatch_metrics(reviews)
    
    # Save enriched data
    save_enriched_data(reviews, app_metrics)
    
    # Analyze changes
    analyze_pipeline_changes()
    
    # Discuss better approaches
    #discuss_better_approaches()
    

if __name__ == "__main__":
    main()
