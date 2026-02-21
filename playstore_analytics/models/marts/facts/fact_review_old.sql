/*
    Fact Table: Reviews
    
    Purpose: Capture review events for analysis
    Grain: One row per review
    
    Business Use: Core fact table for analyzing user feedback
    - Calculate average ratings
    - Track review trends over time
    - Identify problematic apps
    - Measure user engagement
*/

{{ config(
    materialized='table',
    unique_key='review_id'
) }}

WITH reviews AS (
    SELECT * FROM {{ ref('stg_reviews') }}
),

apps AS (
    SELECT app_key, app_id FROM {{ ref('dim_app') }}
),

dates AS (
    SELECT date_key, date FROM {{ ref('dim_date') }}
),

users AS (
    SELECT user_key, user_name FROM {{ ref('dim_user') }}
),

final AS (
    SELECT
        -- Primary key (degenerate dimension)
        r.review_id,
        
        -- Foreign keys to dimensions
        a.app_key,
        d.date_key,
        u.user_key,
        
        -- Timestamp (for precise time analysis)
        r.review_timestamp,
        
        -- Measures (additive and semi-additive)
        r.review_score,
        r.thumbs_up_count,
        r.review_length,
        
        -- Degenerate dimension (review text)
        r.review_content,
        
        -- Boolean flags (for counting)
        r.is_low_rating,
        r.has_content
        
    FROM reviews r
    
    -- Join to dimensions to get surrogate keys
    INNER JOIN apps a 
        ON r.app_id = a.app_id
    
    INNER JOIN dates d 
        ON r.review_date = d.date
    
    INNER JOIN users u 
        ON r.user_name = u.user_name
)

SELECT * FROM final
