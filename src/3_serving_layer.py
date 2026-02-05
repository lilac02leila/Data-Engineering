"""
Step 3: Serving Layer
This script creates analytics-ready outputs for dashboards and reporting
"""

import pandas as pd
from datetime import datetime

def load_processed_data():
    """Load the cleaned, transformed data"""
    print("\n--- Loading processed data ---")
    
    df_apps = pd.read_csv('data/processed/apps_catalog.csv')
    df_reviews = pd.read_csv('data/processed/apps_reviews.csv')
    
    # Convert timestamp column to datetime
    df_reviews['at'] = pd.to_datetime(df_reviews['at'])
    
    print(f"✓ Loaded {len(df_apps)} apps")
    print(f"✓ Loaded {len(df_reviews)} reviews")
    
    return df_apps, df_reviews

def create_app_level_kpis(df_reviews):
    """
    Create app-level KPIs
    Output: One row per application with aggregated metrics
    """
    print("\n--- Creating app-level KPIs ---")
    
    # Calculate metrics for each app
    app_kpis = df_reviews.groupby('app_id').agg({
        'reviewId': 'count',  # Number of reviews
        'score': ['mean', lambda x: (x <= 2).mean() * 100],  # Avg rating and % low ratings
        'at': ['min', 'max']  # First and last review dates
    }).reset_index()
    
    # Flatten column names
    app_kpis.columns = [
        'app_id',
        'num_reviews',
        'avg_rating',
        'pct_low_rating',
        'first_review_date',
        'most_recent_review_date'
    ]
    
    # Round numeric values for readability
    app_kpis['avg_rating'] = app_kpis['avg_rating'].round(2)
    app_kpis['pct_low_rating'] = app_kpis['pct_low_rating'].round(2)
    
    # Sort by number of reviews (most reviewed apps first)
    app_kpis = app_kpis.sort_values('num_reviews', ascending=False)
    
    print(f"✓ Created KPIs for {len(app_kpis)} apps")
    
    # Display sample
    print("\nSample App-Level KPIs:")
    print(app_kpis.head().to_string(index=False))
    
    return app_kpis

def create_daily_metrics(df_reviews):
    """
    Create daily time series metrics
    Output: One row per date with aggregated daily metrics
    """
    print("\n--- Creating daily metrics ---")
    
    # Extract date from timestamp
    df_reviews['date'] = df_reviews['at'].dt.date
    
    # Calculate daily metrics
    daily_metrics = df_reviews.groupby('date').agg({
        'reviewId': 'count',  # Daily number of reviews
        'score': 'mean'  # Daily average rating
    }).reset_index()
    
    # Rename columns
    daily_metrics.columns = ['date', 'daily_num_reviews', 'daily_avg_rating']
    
    # Round average rating
    daily_metrics['daily_avg_rating'] = daily_metrics['daily_avg_rating'].round(2)
    
    # Sort by date
    daily_metrics = daily_metrics.sort_values('date')
    
    print(f"✓ Created metrics for {len(daily_metrics)} days")
    print(f"  Date range: {daily_metrics['date'].min()} to {daily_metrics['date'].max()}")
    
    # Display sample
    print("\nSample Daily Metrics:")
    print(daily_metrics.head(10).to_string(index=False))
    
    return daily_metrics

def save_serving_outputs(app_kpis, daily_metrics):
    """Save serving layer outputs to CSV"""
    print("\n--- Saving serving layer outputs ---")
    
    # Save app-level KPIs
    app_kpis.to_csv('data/processed/app_level_kpis.csv', index=False)
    print("✓ Saved: data/processed/app_level_kpis.csv")
    
    # Save daily metrics
    daily_metrics.to_csv('data/processed/daily_metrics.csv', index=False)
    print("✓ Saved: data/processed/daily_metrics.csv")

def display_insights(app_kpis, daily_metrics):
    """Display key insights from the data"""
    print("KEY INSIGHTS")
    
    # App insights
    print("\n--- App Performance ---")
    print(f"Total apps analyzed: {len(app_kpis)}")
    print(f"Most reviewed app: {app_kpis.iloc[0]['app_id']} ({app_kpis.iloc[0]['num_reviews']} reviews)")
    print(f"Least reviewed app: {app_kpis.iloc[-1]['app_id']} ({app_kpis.iloc[-1]['num_reviews']} reviews)")
    
    # Rating insights
    print("\n--- Rating Analysis ---")
    print(f"Average rating across all apps: {app_kpis['avg_rating'].mean():.2f}")
    print(f"Best rated app: {app_kpis.loc[app_kpis['avg_rating'].idxmax(), 'app_id']} " +
          f"({app_kpis['avg_rating'].max():.2f} stars)")
    print(f"Worst rated app: {app_kpis.loc[app_kpis['avg_rating'].idxmin(), 'app_id']} " +
          f"({app_kpis['avg_rating'].min():.2f} stars)")
    
    # Apps with high % of low ratings
    high_low_ratings = app_kpis[app_kpis['pct_low_rating'] > 20]
    print(f"\nApps with >20% low ratings (≤2 stars): {len(high_low_ratings)}")
    
    # Time series insights
    print("\n--- Time Series Analysis ---")
    print(f"Total days with reviews: {len(daily_metrics)}")
    print(f"Average reviews per day: {daily_metrics['daily_num_reviews'].mean():.1f}")
    print(f"Peak review day: {daily_metrics.loc[daily_metrics['daily_num_reviews'].idxmax(), 'date']} " +
          f"({daily_metrics['daily_num_reviews'].max()} reviews)")
    

def main():
    """Main execution function"""
    print("SERVING LAYER PIPELINE - STEP 3")
    
    # Load processed data
    df_apps, df_reviews = load_processed_data()
    
    # Create app-level KPIs
    app_kpis = create_app_level_kpis(df_reviews)
    
    # Create daily metrics
    daily_metrics = create_daily_metrics(df_reviews)
    
    # Save outputs
    save_serving_outputs(app_kpis, daily_metrics)
    
    # Display insights
    display_insights(app_kpis, daily_metrics)
    
    print("SERVING LAYER COMPLETE!")

if __name__ == "__main__":
    main()