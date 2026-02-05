"""
Step 4: Lightweight Dashboard
This script creates visualizations from the serving layer outputs
"""

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px

def load_serving_data():
    """Load the serving layer outputs"""
    print("Loading data for dashboard...")
    
    app_kpis = pd.read_csv('data/processed/app_level_kpis.csv')
    daily_metrics = pd.read_csv('data/processed/daily_metrics.csv')
    
    # Convert date column
    daily_metrics['date'] = pd.to_datetime(daily_metrics['date'])
    
    print(f"✓ Loaded {len(app_kpis)} apps")
    print(f"✓ Loaded {len(daily_metrics)} daily records")
    
    return app_kpis, daily_metrics

def create_app_performance_chart(app_kpis):
    """
    Create a chart comparing app performance
    Shows: Which apps perform best/worst based on ratings and review volume
    """
    # Take top 15 apps by review volume for readability
    top_apps = app_kpis.head(15).copy()
    
    # Create figure with secondary y-axis
    fig = make_subplots(
        rows=1, cols=1,
        specs=[[{"secondary_y": True}]]
    )
    
    # Add bar chart for number of reviews
    fig.add_trace(
        go.Bar(
            x=top_apps['app_id'],
            y=top_apps['num_reviews'],
            name='Number of Reviews',
            marker_color='lightblue',
            yaxis='y'
        ),
        secondary_y=False
    )
    
    # Add line chart for average rating
    fig.add_trace(
        go.Scatter(
            x=top_apps['app_id'],
            y=top_apps['avg_rating'],
            name='Average Rating',
            mode='lines+markers',
            line=dict(color='red', width=3),
            marker=dict(size=8),
            yaxis='y2'
        ),
        secondary_y=True
    )
    
    # Update layout
    fig.update_layout(
        title='App Performance: Review Volume vs. Average Rating',
        xaxis_title='App ID',
        height=500,
        hovermode='x unified',
        legend=dict(x=0.01, y=0.99)
    )
    
    fig.update_yaxes(title_text="Number of Reviews", secondary_y=False)
    fig.update_yaxes(title_text="Average Rating (1-5)", secondary_y=True, range=[0, 5])
    
    return fig

def create_rating_distribution_chart(app_kpis):
    """
    Create a histogram showing distribution of average ratings
    Shows: How ratings are distributed across apps
    """
    fig = go.Figure()
    
    fig.add_trace(go.Histogram(
        x=app_kpis['avg_rating'],
        nbinsx=20,
        marker_color='steelblue',
        name='Apps'
    ))
    
    fig.update_layout(
        title='Distribution of Average App Ratings',
        xaxis_title='Average Rating (1-5 stars)',
        yaxis_title='Number of Apps',
        height=400,
        showlegend=False
    )
    
    return fig

def create_daily_trends_chart(daily_metrics):
    """
    Create time series showing review trends over time
    Shows: Are ratings improving or declining? Review volume trends?
    """
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=('Daily Review Volume', 'Daily Average Rating'),
        vertical_spacing=0.12
    )
    
    # Daily review volume
    fig.add_trace(
        go.Scatter(
            x=daily_metrics['date'],
            y=daily_metrics['daily_num_reviews'],
            name='Review Volume',
            fill='tozeroy',
            line=dict(color='blue')
        ),
        row=1, col=1
    )
    
    # Daily average rating with trend line
    fig.add_trace(
        go.Scatter(
            x=daily_metrics['date'],
            y=daily_metrics['daily_avg_rating'],
            name='Avg Rating',
            mode='lines',
            line=dict(color='green', width=2)
        ),
        row=2, col=1
    )
    
    # Add moving average trend
    window = min(7, len(daily_metrics) // 3)  # 7-day or adaptive window
    if window > 1:
        daily_metrics['rating_ma'] = daily_metrics['daily_avg_rating'].rolling(window=window).mean()
        fig.add_trace(
            go.Scatter(
                x=daily_metrics['date'],
                y=daily_metrics['rating_ma'],
                name=f'{window}-day Trend',
                mode='lines',
                line=dict(color='red', width=2, dash='dash')
            ),
            row=2, col=1
        )
    
    fig.update_xaxes(title_text="Date", row=2, col=1)
    fig.update_yaxes(title_text="Number of Reviews", row=1, col=1)
    fig.update_yaxes(title_text="Rating (1-5)", row=2, col=1, range=[0, 5])
    
    fig.update_layout(
        title_text='Review Trends Over Time',
        height=700,
        showlegend=True
    )
    
    return fig

def create_low_rating_analysis(app_kpis):
    """
    Create chart showing apps with high percentage of low ratings
    Shows: Which apps have user satisfaction issues?
    """
    # Filter apps with significant reviews (at least 10)
    significant_apps = app_kpis[app_kpis['num_reviews'] >= 10].copy()
    
    # Sort by percentage of low ratings
    significant_apps = significant_apps.sort_values('pct_low_rating', ascending=False).head(15)
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=significant_apps['app_id'],
        y=significant_apps['pct_low_rating'],
        marker_color='crimson',
        text=significant_apps['pct_low_rating'].round(1),
        textposition='outside'
    ))
    
    fig.update_layout(
        title='Apps with Highest % of Low Ratings (≤2 stars)',
        xaxis_title='App ID',
        yaxis_title='Percentage of Low Ratings (%)',
        height=450,
        showlegend=False
    )
    
    return fig

def create_dashboard(app_kpis, daily_metrics):
    """Create and display all dashboard visualizations"""
    print("CREATING DASHBOARD")
    
    print("\n1. Creating app performance comparison...")
    fig1 = create_app_performance_chart(app_kpis)
    fig1.write_html('data/processed/dashboard_app_performance.html')
    print("   ✓ Saved: dashboard_app_performance.html")
    
    print("\n2. Creating rating distribution...")
    fig2 = create_rating_distribution_chart(app_kpis)
    fig2.write_html('data/processed/dashboard_rating_distribution.html')
    print("   ✓ Saved: dashboard_rating_distribution.html")
    
    print("\n3. Creating daily trends analysis...")
    fig3 = create_daily_trends_chart(daily_metrics)
    fig3.write_html('data/processed/dashboard_daily_trends.html')
    print("   ✓ Saved: dashboard_daily_trends.html")
    
    print("\n4. Creating low rating analysis...")
    fig4 = create_low_rating_analysis(app_kpis)
    fig4.write_html('data/processed/dashboard_low_ratings.html')
    print("   ✓ Saved: dashboard_low_ratings.html")
    
    # Show figures in browser
    print("\n" + "="*60)
    print("Opening dashboards in browser...")
    fig1.show()
    fig2.show()
    fig3.show()
    fig4.show()

def print_dashboard_summary(app_kpis, daily_metrics):
    """Print text summary of what the dashboard shows"""
    print("DASHBOARD SUMMARY")
    
    print("""
This dashboard answers three key business questions:

1. WHICH APPS PERFORM BEST/WORST?
   - The first chart shows review volume vs. ratings
   - High review volume + high rating = strong performer
   - Low rating regardless of volume = potential issues
   
2. ARE RATINGS IMPROVING OR DECLINING?
   - The trends chart shows rating trajectory over time
   - The trend line reveals whether sentiment is positive/negative
   - Helps identify if issues are recent or long-standing
   
3. REVIEW VOLUME DIFFERENCES?
   - Bar charts clearly show which apps have more user engagement
   - Some apps have 10x more reviews than others
   - This indicates market share or popularity differences

KEY FINDINGS:
    """)
    
    # Calculate some insights
    best_app = app_kpis.loc[app_kpis['avg_rating'].idxmax()]
    worst_app = app_kpis.loc[app_kpis['avg_rating'].idxmin()]
    most_reviewed = app_kpis.iloc[0]
    
    print(f"• Best rated app: {best_app['app_id']} ({best_app['avg_rating']:.2f} stars)")
    print(f"• Worst rated app: {worst_app['app_id']} ({worst_app['avg_rating']:.2f} stars)")
    print(f"• Most reviewed app: {most_reviewed['app_id']} ({most_reviewed['num_reviews']} reviews)")
    
    # Check if ratings are improving
    if len(daily_metrics) >= 14:
        recent_avg = daily_metrics.tail(7)['daily_avg_rating'].mean()
        older_avg = daily_metrics.head(7)['daily_avg_rating'].mean()
        trend = "improving" if recent_avg > older_avg else "declining"
        print(f"• Overall rating trend: {trend}")
        print(f"  (Recent avg: {recent_avg:.2f} vs Earlier avg: {older_avg:.2f})")
    

def main():
    """Main execution function"""
    print("DASHBOARD CREATION - STEP 4")
    
    # Load serving layer data
    app_kpis, daily_metrics = load_serving_data()
    
    # Create visualizations
    create_dashboard(app_kpis, daily_metrics)
    
    # Print summary
    print_dashboard_summary(app_kpis, daily_metrics)
    
    print("DASHBOARD COMPLETE!")
    print("Check the data/processed folder for HTML dashboard files")

if __name__ == "__main__":
    # Install plotly if needed: pip install plotly
    main()