/*
    Aggregate: Daily Metrics
    
    Purpose: Time-series metrics for trend analysis
    Grain: One row per date
    
    Business Use: Track review activity and sentiment over time
    - Identify trends
    - Spot anomalies
    - Seasonal patterns
    - Day-of-week effects
*/

{{ config(
    materialized='table'
) }}

WITH fact AS (
    SELECT * FROM {{ ref('fact_review') }}
),

date AS (
    SELECT * FROM {{ ref('dim_date') }}
),

daily_aggregates AS (
    SELECT
        f.date_key,
        
        -- Volume metrics
        COUNT(*) AS daily_review_count,
        COUNT(DISTINCT f.app_key) AS apps_reviewed,
        COUNT(DISTINCT f.user_key) AS unique_reviewers,
        
        -- Rating metrics
        AVG(f.review_score) AS avg_daily_rating,
        MIN(f.review_score) AS min_daily_rating,
        MAX(f.review_score) AS max_daily_rating,
        
        -- Sentiment metrics
        SUM(CASE WHEN f.is_low_rating THEN 1 ELSE 0 END) AS low_rating_count,
        SUM(CASE WHEN f.is_low_rating THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS pct_low_rating,
        
        -- Engagement metrics
        SUM(f.thumbs_up_count) AS total_thumbs_up,
        AVG(f.thumbs_up_count) AS avg_thumbs_up,
        
        -- Content metrics
        SUM(CASE WHEN f.has_content THEN 1 ELSE 0 END) AS reviews_with_content,
        AVG(f.review_length) AS avg_review_length
        
    FROM fact f
    GROUP BY f.date_key
)

SELECT
    d.date_key,
    d.date,
    d.year,
    d.quarter,
    d.quarter_name,
    d.month,
    d.month_name,
    d.week_of_year,
    d.day_of_month,
    d.day_of_week,
    d.day_name,
    d.is_weekend,
    
    -- Daily metrics
    a.daily_review_count,
    a.apps_reviewed,
    a.unique_reviewers,
    ROUND(a.avg_daily_rating, 2) AS avg_daily_rating,
    a.min_daily_rating,
    a.max_daily_rating,
    a.low_rating_count,
    ROUND(a.pct_low_rating, 2) AS pct_low_rating,
    a.total_thumbs_up,
    ROUND(a.avg_thumbs_up, 2) AS avg_thumbs_up,
    a.reviews_with_content,
    ROUND(a.avg_review_length, 0) AS avg_review_length,
    
    -- 7-day moving averages for trend analysis
    ROUND(AVG(a.daily_review_count) OVER (
        ORDER BY d.date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 1) AS review_count_7day_ma,
    
    ROUND(AVG(a.avg_daily_rating) OVER (
        ORDER BY d.date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) AS rating_7day_ma

FROM date d
INNER JOIN daily_aggregates a ON d.date_key = a.date_key
ORDER BY d.date
