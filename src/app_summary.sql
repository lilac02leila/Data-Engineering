/*
    Aggregate: App Summary
    
    Purpose: Pre-aggregated app-level metrics for dashboards
    Grain: One row per app
    
    Business Use: Quick app performance overview
    - Compare apps by review metrics
    - Identify top/bottom performers
    - Track user satisfaction
*/

{{ config(
    materialized='table'
) }}

WITH fact AS (
    SELECT * FROM {{ ref('fact_review') }}
),

app AS (
    SELECT * FROM {{ ref('dim_app') }}
),

date AS (
    SELECT * FROM {{ ref('dim_date') }}
),

app_metrics AS (
    SELECT
        f.app_key,
        
        -- Review volume metrics
        COUNT(*) AS total_reviews,
        COUNT(DISTINCT f.date_key) AS days_with_reviews,
        
        -- Rating metrics
        AVG(f.review_score) AS avg_review_rating,
        MIN(f.review_score) AS min_review_rating,
        MAX(f.review_score) AS max_review_rating,
        
        -- Rating distribution
        SUM(CASE WHEN f.review_score = 5 THEN 1 ELSE 0 END) AS five_star_count,
        SUM(CASE WHEN f.review_score = 4 THEN 1 ELSE 0 END) AS four_star_count,
        SUM(CASE WHEN f.review_score = 3 THEN 1 ELSE 0 END) AS three_star_count,
        SUM(CASE WHEN f.review_score = 2 THEN 1 ELSE 0 END) AS two_star_count,
        SUM(CASE WHEN f.review_score = 1 THEN 1 ELSE 0 END) AS one_star_count,
        
        -- Sentiment metrics
        SUM(CASE WHEN f.is_low_rating THEN 1 ELSE 0 END) AS low_rating_count,
        SUM(CASE WHEN f.is_low_rating THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS pct_low_rating,
        
        -- Engagement metrics
        SUM(f.thumbs_up_count) AS total_thumbs_up,
        AVG(f.thumbs_up_count) AS avg_thumbs_up,
        
        -- Content metrics
        SUM(CASE WHEN f.has_content THEN 1 ELSE 0 END) AS reviews_with_content,
        AVG(f.review_length) AS avg_review_length,
        
        -- Temporal metrics (join with date dimension)
        MIN(d.date) AS first_review_date,
        MAX(d.date) AS most_recent_review_date
        
    FROM fact f
    INNER JOIN date d ON f.date_key = d.date_key
    GROUP BY f.app_key
)

SELECT
    a.app_key,
    a.app_id,
    a.app_title,
    a.developer_name,
    a.app_genre,
    a.price,
    a.price_tier,
    a.install_count,
    a.popularity_tier,
    a.app_rating AS catalog_rating,
    a.rating_count AS catalog_rating_count,
    a.quality_tier,
    
    -- Aggregated review metrics
    m.total_reviews,
    m.days_with_reviews,
    m.avg_review_rating,
    m.min_review_rating,
    m.max_review_rating,
    m.five_star_count,
    m.four_star_count,
    m.three_star_count,
    m.two_star_count,
    m.one_star_count,
    m.low_rating_count,
    ROUND(m.pct_low_rating, 2) AS pct_low_rating,
    m.total_thumbs_up,
    ROUND(m.avg_thumbs_up, 2) AS avg_thumbs_up,
    m.reviews_with_content,
    ROUND(m.avg_review_length, 0) AS avg_review_length,
    m.first_review_date,
    m.most_recent_review_date,
    
    -- Rating variance (catalog vs reviews)
    ROUND(m.avg_review_rating - a.app_rating, 2) AS rating_variance

FROM app a
LEFT JOIN app_metrics m ON a.app_key = m.app_key
ORDER BY m.total_reviews DESC NULLS LAST
